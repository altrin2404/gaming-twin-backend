from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional
from datetime import datetime, time, timedelta, timezone
from contextlib import contextmanager
import os
import psycopg2
from psycopg2 import pool
import json
import requests
import logging

# ========== SETUP ==========

app = FastAPI(title="Gaming Twin Backend")

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# IST Timezone (UTC+5:30) - for display purposes only
IST = timezone(timedelta(hours=5, minutes=30))

# ML URL from environment
ML_URL = os.getenv("ML_API_URL", "https://digital-twin-gaming-ml.onrender.com/analyze-ml")

# Global connection pool
db_pool: Optional[pool.ThreadedConnectionPool] = None


# ========== DATABASE ==========

def init_db_pool():
    """Initialize connection pool"""
    global db_pool
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL not set")
    try:
        db_pool = pool.ThreadedConnectionPool(1, 20, db_url)
        logger.info("✅ Database pool initialized")
    except Exception as e:
        logger.error(f"❌ Failed to initialize DB pool: {e}")
        raise


def get_db_connection():
    """Get connection from pool"""
    global db_pool
    if db_pool is None:
        init_db_pool()
    return db_pool.getconn()


def release_connection(conn):
    """Return connection to pool"""
    global db_pool
    if db_pool and conn:
        db_pool.putconn(conn)


@contextmanager
def get_db():
    """Context manager for database connections"""
    conn = None
    try:
        conn = get_db_connection()
        yield conn
        conn.commit()
    except Exception:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            release_connection(conn)


def check_and_add_status_column(conn):
    """Add status column if it doesn't exist (migration helper)"""
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = 'events' AND column_name = 'status'
        """)
        if not cur.fetchone():
            logger.info("📦 Adding 'status' column to events table...")
            cur.execute("ALTER TABLE events ADD COLUMN status VARCHAR(20)")
            conn.commit()
            logger.info("✅ 'status' column added successfully")
    except Exception as e:
        logger.warning(f"Could not add status column: {e}")
        conn.rollback()
    finally:
        cur.close()


# ========== STARTUP / SHUTDOWN ==========

@app.on_event("startup")
def startup():
    """Initialize resources on startup"""
    try:
        init_db_pool()
        # Run migration check
        with get_db() as conn:
            check_and_add_status_column(conn)
    except Exception as e:
        logger.error(f"Startup failed: {e}")


@app.on_event("shutdown")
def shutdown():
    """Cleanup on shutdown"""
    global db_pool
    if db_pool:
        db_pool.closeall()
        logger.info("✅ Database pool closed")


# ========== MIDDLEWARE ==========

@app.middleware("http")
async def api_key_auth(request: Request, call_next):
    """API Key authentication middleware"""
    # Allow these endpoints without API key
    public_paths = ["/", "/health", "/parent/login", "/debug/time", "/docs", "/openapi.json"]
    
    if request.url.path in public_paths:
        return await call_next(request)
    
    api_key_header = request.headers.get("X-API-KEY")
    expected = os.getenv("API_KEY")
    
    if not expected:
        logger.warning("API_KEY environment variable not set - allowing request")
        return await call_next(request)
    
    if api_key_header != expected:
        logger.warning(f"Unauthorized access attempt to {request.url.path}")
        return JSONResponse(status_code=401, content={"status": "error", "detail": "Unauthorized"})
    
    return await call_next(request)


# ========== MODELS ==========

class GamingEvent(BaseModel):
    user_id: str
    childdeviceid: str
    package_name: str
    game_name: str
    start_time: int
    end_time: int
    duration: int
    timestamp: int
    status: Optional[str] = "STOP"  # Made optional with default for backward compatibility


class ThresholdUpdate(BaseModel):
    daily: Optional[int] = None
    night: Optional[int] = None


class ParentLoginRequest(BaseModel):
    username: str
    password: str


# ========== HELPER FUNCTIONS ==========

def is_night(now: datetime) -> bool:
    """Check if time is between 10 PM and 6 AM"""
    start = time(22, 0)
    end = time(6, 0)
    t = now.time()
    return t >= start or t < end


def cleanup_stale_sessions(conn):
    """Set offline if no heartbeat > 5 min"""
    cur = conn.cursor()
    try:
        five_min_ago = datetime.utcnow() - timedelta(minutes=5)
        
        cur.execute("""
            UPDATE digital_twins 
            SET is_online = FALSE, current_game = NULL 
            WHERE is_online = TRUE 
            AND (last_heartbeat_at IS NULL OR last_heartbeat_at < %s)
        """, (five_min_ago,))
        
        affected = cur.rowcount
        if affected > 0:
            logger.info(f"🔄 Cleaned up {affected} stale sessions")
        
        conn.commit()
    except Exception as e:
        logger.error(f"Error cleaning stale sessions: {e}")
        conn.rollback()
    finally:
        cur.close()


def check_and_trigger_alert(conn, user_id: str) -> bool:
    """Instant alert if daily limit exceeded"""
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT aggregates, thresholds 
            FROM digital_twins WHERE user_id = %s
        """, (user_id,))
        row = cur.fetchone()
        
        if row:
            aggregates = row[0] or {}
            thresholds = row[1] or {"daily": 120}
            today_minutes = aggregates.get("today_minutes", 0)
            daily_limit = thresholds.get("daily", 120)
            
            if today_minutes > daily_limit:
                logger.warning(f"🚨 ALERT: {user_id} exceeded daily limit {today_minutes}/{daily_limit} min")
                # TODO: Send push notification to parent
                return True
        return False
    finally:
        cur.close()


def ensure_twin_exists(conn, user_id: str):
    """Create digital twin if it doesn't exist"""
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO digital_twins (
                user_id, thresholds, aggregates, state, risk_level, alert_message, 
                is_online, current_game, last_heartbeat_at, created_at, last_updated
            )
            VALUES (
                %s, 
                '{"daily":120,"night":60}', 
                '{"today_minutes":0,"weekly_minutes":0,"night_minutes":0,"sessions_per_day":0}',
                'Healthy', 'Unknown', '', FALSE, NULL, NULL, NOW(), NOW()
            )
            ON CONFLICT (user_id) DO NOTHING
        """, (user_id,))
        conn.commit()
    finally:
        cur.close()


def calculate_fresh_aggregates(conn, user_id: str) -> dict:
    """
    Calculate aggregates based on UTC date (backward compatible)
    Includes old events that don't have status column
    """
    cur = conn.cursor()
    try:
        # Use UTC for date calculations (backward compatible with existing data)
        today_utc = datetime.utcnow().date()
        week_start = today_utc - timedelta(days=6)
        
        # Today's minutes - include STOP events and old events (NULL status)
        cur.execute("""
            SELECT COALESCE(SUM(duration), 0) FROM events 
            WHERE user_id = %s 
            AND DATE(event_time) = %s
            AND (status = 'STOP' OR status IS NULL)
        """, (user_id, today_utc))
        today_minutes = cur.fetchone()[0]
        
        # Weekly minutes
        cur.execute("""
            SELECT COALESCE(SUM(duration), 0) FROM events 
            WHERE user_id = %s 
            AND DATE(event_time) BETWEEN %s AND %s
            AND (status = 'STOP' OR status IS NULL)
        """, (user_id, week_start, today_utc))
        weekly_minutes = cur.fetchone()[0]
        
        # Night minutes (this week)
        cur.execute("""
            SELECT COALESCE(SUM(duration), 0) FROM events 
            WHERE user_id = %s 
            AND DATE(event_time) BETWEEN %s AND %s
            AND isnight = TRUE
            AND (status = 'STOP' OR status IS NULL)
        """, (user_id, week_start, today_utc))
        night_minutes = cur.fetchone()[0]
        
        # Sessions today - count START events or count all events for old data
        cur.execute("""
            SELECT COUNT(*) FROM events 
            WHERE user_id = %s 
            AND DATE(event_time) = %s
            AND (status = 'START' OR status IS NULL)
        """, (user_id, today_utc))
        sessions_per_day = cur.fetchone()[0]
        
        return {
            "today_minutes": int(today_minutes),
            "weekly_minutes": int(weekly_minutes),
            "night_minutes": int(night_minutes),
            "sessions_per_day": int(sessions_per_day),
        }
    finally:
        cur.close()


def update_live_status(conn, user_id: str, status: str, game_name: str):
    """Handle START/HEARTBEAT/STOP live status updates"""
    cur = conn.cursor()
    try:
        if status == "START":
            cur.execute("""
                UPDATE digital_twins 
                SET is_online = TRUE, current_game = %s, last_heartbeat_at = NOW()
                WHERE user_id = %s
            """, (game_name, user_id))
            logger.info(f"🎮 {user_id} started playing {game_name}")
            
        elif status == "HEARTBEAT":
            cur.execute("""
                UPDATE digital_twins 
                SET last_heartbeat_at = NOW()
                WHERE user_id = %s
            """, (user_id,))
            
        elif status == "STOP":
            cur.execute("""
                UPDATE digital_twins 
                SET is_online = FALSE, current_game = NULL
                WHERE user_id = %s
            """, (user_id,))
            logger.info(f"🛑 {user_id} stopped playing")
        
        conn.commit()
    except Exception as e:
        logger.error(f"Error updating live status: {e}")
        conn.rollback()
    finally:
        cur.close()


def call_ml_api(twin_json: dict) -> dict:
    """Call ML API with timeout and error handling"""
    try:
        response = requests.post(ML_URL, json={"digital_twin": twin_json}, timeout=3)
        response.raise_for_status()
        return response.json()
    except requests.Timeout:
        logger.warning("ML API timeout")
        return {}
    except requests.RequestException as e:
        logger.warning(f"ML API error: {e}")
        return {}
    except json.JSONDecodeError:
        logger.warning("ML API returned invalid JSON")
        return {}


def update_aggregates(conn, user_id: str, duration: int, event_time: datetime, status: str, game_name: str):
    """
    Update aggregates - backward compatible version
    This matches the original function signature and behavior
    """
    cur = conn.cursor()
    try:
        # Ensure twin exists
        ensure_twin_exists(conn, user_id)
        
        # Calculate fresh aggregates
        aggregates = calculate_fresh_aggregates(conn, user_id)
        
        # Get thresholds
        cur.execute("SELECT thresholds FROM digital_twins WHERE user_id = %s", (user_id,))
        row = cur.fetchone()
        thresholds = row[0] if row and row[0] else {"daily": 120, "night": 60}
        
        # Determine state based on usage
        today_minutes = aggregates["today_minutes"]
        daily_th = thresholds.get("daily", 120)
        
        if today_minutes <= daily_th * 0.5:
            state = "Healthy"
        elif today_minutes <= daily_th:
            state = "Moderate"
        else:
            state = "Excessive"
        
        risk_level = "Unknown"
        alert_message = ""
        
        # Call ML API for advanced analysis
        twin_json = {
            "user_id": user_id,
            "thresholds": thresholds,
            "aggregates": aggregates,
            "state": state
        }
        
        ml_response = call_ml_api(twin_json)
        if ml_response:
            state = ml_response.get("state", state)
            risk_level = ml_response.get("severity", risk_level)
            alert_message = ml_response.get("alertmessage", alert_message)
            
            if isinstance(alert_message, list):
                alert_message = "; ".join(alert_message)
        
        # Handle live status updates
        if status == "START":
            cur.execute("""
                UPDATE digital_twins 
                SET is_online = TRUE, current_game = %s, last_heartbeat_at = NOW()
                WHERE user_id = %s
            """, (game_name, user_id))
        elif status == "HEARTBEAT":
            cur.execute("""
                UPDATE digital_twins 
                SET last_heartbeat_at = NOW()
                WHERE user_id = %s
            """, (user_id,))
            check_and_trigger_alert(conn, user_id)
        elif status == "STOP":
            cur.execute("""
                UPDATE digital_twins 
                SET is_online = FALSE, current_game = NULL
                WHERE user_id = %s
            """, (user_id,))
        
        # Save aggregates + ML results
        cur.execute("""
            UPDATE digital_twins
            SET aggregates = %s, thresholds = %s, state = %s, risk_level = %s, 
                alert_message = %s, last_updated = NOW()
            WHERE user_id = %s
        """, (json.dumps(aggregates), json.dumps(thresholds), state, risk_level, alert_message, user_id))
        
        conn.commit()
        
        logger.info(f"📊 Updated twin for {user_id}: {aggregates['today_minutes']} min today, state={state}")
        
    except Exception as e:
        logger.error(f"Error updating aggregates: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()


def insert_event_safe(conn, event: GamingEvent, event_dt: datetime) -> Optional[int]:
    """
    Insert event into database with backward compatibility
    Handles cases where status column might not exist
    """
    cur = conn.cursor()
    try:
        # Check if status column exists
        cur.execute("""
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = 'events' AND column_name = 'status'
        """)
        has_status_column = cur.fetchone() is not None
        
        if has_status_column:
            # New schema with status column
            cur.execute("""
                INSERT INTO events (
                    user_id, childdeviceid, package_name, game_name, duration, 
                    event_time, start_time_bigint, end_time_bigint, event_timestamp_bigint,
                    status, isnight
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                event.user_id,
                event.childdeviceid,
                event.package_name,
                event.game_name,
                event.duration,
                event_dt,
                event.start_time,
                event.end_time,
                event.timestamp,
                event.status,
                is_night(event_dt)
            ))
        else:
            # Old schema without status column
            cur.execute("""
                INSERT INTO events (
                    user_id, childdeviceid, package_name, game_name, duration, 
                    event_time, start_time_bigint, end_time_bigint, event_timestamp_bigint, isnight
                )
                VALUES (%s, %s, %s, %s, %s, TO_TIMESTAMP(%s / 1000.0), %s, %s, %s, %s)
                RETURNING id
            """, (
                event.user_id,
                event.childdeviceid,
                event.package_name,
                event.game_name,
                event.duration,
                event.timestamp,
                event.start_time,
                event.end_time,
                event.timestamp,
                is_night(event_dt)
            ))
        
        event_id = cur.fetchone()[0]
        conn.commit()
        return event_id
        
    except Exception as e:
        logger.error(f"Error inserting event: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()


# ========== ENDPOINTS ==========

@app.get("/")
def root():
    """Root endpoint"""
    return {
        "message": "Gaming Twin Backend API",
        "status": "running",
        "version": "1.1.0"
    }


@app.get("/health")
def health():
    """Health check endpoint - backward compatible"""
    try:
        with get_db() as conn:
            cleanup_stale_sessions(conn)
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
            cur.close()
        
        return {"status": "ok"}
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {"status": "error", "message": str(e)}


@app.get("/debug/time")
def debug_time():
    """Debug endpoint to verify timezone"""
    return {
        "server_utc": datetime.utcnow().isoformat(),
        "server_ist": datetime.now(IST).isoformat(),
        "today_utc": str(datetime.utcnow().date()),
        "today_ist": str(datetime.now(IST).date()),
    }


@app.post("/events")
def ingest_event(event: GamingEvent):
    """
    Ingest gaming events from child device
    Backward compatible with original response format
    """
    try:
        # Use UTC for database storage (backward compatible)
        event_dt = datetime.utcfromtimestamp(event.timestamp / 1000.0)
        
        with get_db() as conn:
            # Insert event
            cur = conn.cursor()
            
            # Check if status column exists
            cur.execute("""
                SELECT column_name FROM information_schema.columns 
                WHERE table_name = 'events' AND column_name = 'status'
            """)
            has_status_column = cur.fetchone() is not None
            
            if has_status_column:
                cur.execute("""
                    INSERT INTO events (
                        user_id, childdeviceid, package_name, game_name, duration, 
                        event_time, start_time_bigint, end_time_bigint, event_timestamp_bigint,
                        status, isnight
                    )
                    VALUES (%s, %s, %s, %s, %s, TO_TIMESTAMP(%s / 1000.0), %s, %s, %s, %s, %s)
                """, (
                    event.user_id,
                    event.childdeviceid,
                    event.package_name,
                    event.game_name,
                    event.duration,
                    event.timestamp,
                    event.start_time,
                    event.end_time,
                    event.timestamp,
                    event.status,
                    is_night(event_dt)
                ))
            else:
                cur.execute("""
                    INSERT INTO events (
                        user_id, childdeviceid, package_name, game_name, duration, 
                        event_time, start_time_bigint, end_time_bigint, event_timestamp_bigint
                    )
                    VALUES (%s, %s, %s, %s, %s, TO_TIMESTAMP(%s / 1000.0), %s, %s, %s)
                """, (
                    event.user_id,
                    event.childdeviceid,
                    event.package_name,
                    event.game_name,
                    event.duration,
                    event.timestamp,
                    event.start_time,
                    event.end_time,
                    event.timestamp
                ))
                cur.execute("UPDATE events SET isnight = %s WHERE id = currval('events_id_seq')", 
                           (is_night(event_dt),))
            
            cur.close()
            
            # Update twin with status logic
            update_aggregates(conn, event.user_id, event.duration, event_dt, event.status, event.game_name)
        
        # Backward compatible response format
        return {"status": "processed", "user_id": event.user_id}
        
    except Exception as e:
        logger.error(f"Error processing event: {e}", exc_info=True)
        return {"status": "error", "message": str(e)}


@app.get("/digital-twin/{user_id}")
def get_twin(user_id: str):
    """
    Get digital twin data for a user
    Recalculates fresh aggregates on every request
    """
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT user_id, thresholds, aggregates, state, risk_level, alert_message, 
                       is_online, current_game, last_heartbeat_at
                FROM digital_twins WHERE user_id = %s
            """, (user_id,))
            row = cur.fetchone()
            cur.close()
            
            if not row:
                raise HTTPException(status_code=404, detail="User not found")
            
            # Calculate fresh aggregates
            fresh_aggregates = calculate_fresh_aggregates(conn, user_id)
            
            # Update cached aggregates
            cur = conn.cursor()
            cur.execute("""
                UPDATE digital_twins 
                SET aggregates = %s, last_updated = NOW()
                WHERE user_id = %s
            """, (json.dumps(fresh_aggregates), user_id))
            cur.close()
            
            return {
                "user_id": row[0],
                "thresholds": row[1],
                "aggregates": fresh_aggregates,  # Return fresh aggregates
                "state": row[3],
                "risk_level": row[4],
                "alert_message": row[5],
                "is_online": row[6],
                "current_game": row[7],
                "last_heartbeat_at": row[8]
            }
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting twin for {user_id}: {e}", exc_info=True)
        return {"status": "error", "message": str(e)}


@app.get("/reports/{user_id}")
def get_reports(user_id: str):
    """Get usage reports for a user - backward compatible"""
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT aggregates FROM digital_twins WHERE user_id = %s", (user_id,))
            row = cur.fetchone()
            cur.close()
            
            if not row:
                raise HTTPException(status_code=404, detail="User not found")
            
            # Calculate fresh aggregates
            aggregates = calculate_fresh_aggregates(conn, user_id)
            
            return {
                "user_id": user_id,
                "today_minutes": aggregates.get("today_minutes", 0),
                "weekly_minutes": aggregates.get("weekly_minutes", 0),
                "night_minutes": aggregates.get("night_minutes", 0),
                "sessions_per_day": aggregates.get("sessions_per_day", 0),
            }
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting reports for {user_id}: {e}", exc_info=True)
        return {"status": "error", "message": str(e)}


@app.post("/digital-twin/{user_id}/threshold")
def update_threshold(user_id: str, body: ThresholdUpdate):
    """Update thresholds for a user - backward compatible"""
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT thresholds FROM digital_twins WHERE user_id = %s", (user_id,))
            row = cur.fetchone()
            thresholds = row[0] if row and row[0] else {"daily": 120, "night": 60}
            
            if body.daily is not None:
                thresholds["daily"] = body.daily
            if body.night is not None:
                thresholds["night"] = body.night
            
            cur.execute(
                "UPDATE digital_twins SET thresholds = %s, last_updated = NOW() WHERE user_id = %s",
                (json.dumps(thresholds), user_id)
            )
            cur.close()
            
            logger.info(f"⚙️ Updated thresholds for {user_id}: {thresholds}")
        
        return {"user_id": user_id, "thresholds": thresholds}
        
    except Exception as e:
        logger.error(f"Error updating threshold for {user_id}: {e}", exc_info=True)
        return {"status": "error", "message": str(e)}


@app.post("/parent/login")
def parent_login(body: ParentLoginRequest):
    """
    Parent login endpoint - backward compatible (mock)
    Returns success for any credentials (like original)
    """
    return {
        "success": True,
        "parent_id": "parent_001",
        "token": "jwt_token_here",
        "children": [{"child_id": "child_101", "name": "John"}]
    }


# ========== NEW ENDPOINTS (Additions that don't break existing apps) ==========

@app.post("/digital-twin/{user_id}/reset-today")
def reset_today(user_id: str):
    """Manually reset today's usage to 0"""
    try:
        with get_db() as conn:
            cur = conn.cursor()
            
            cur.execute("SELECT user_id FROM digital_twins WHERE user_id = %s", (user_id,))
            if not cur.fetchone():
                cur.close()
                raise HTTPException(status_code=404, detail="User not found")
            
            cur.execute("""
                UPDATE digital_twins 
                SET aggregates = jsonb_set(
                    COALESCE(aggregates, '{}')::jsonb,
                    '{today_minutes}',
                    '0'
                ),
                last_updated = NOW()
                WHERE user_id = %s
            """, (user_id,))
            cur.close()
            
            logger.info(f"🔄 Reset today_minutes for {user_id}")
        
        return {
            "status": "ok",
            "message": f"Reset today_minutes for {user_id}",
            "reset_at": datetime.utcnow().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error resetting today for {user_id}: {e}", exc_info=True)
        return {"status": "error", "message": str(e)}


@app.delete("/digital-twin/{user_id}/events")
def delete_user_events(user_id: str, date: Optional[str] = None):
    """Delete events for a user (optionally for a specific date)"""
    try:
        with get_db() as conn:
            cur = conn.cursor()
            
            if date:
                cur.execute("""
                    DELETE FROM events 
                    WHERE user_id = %s AND DATE(event_time) = %s
                """, (user_id, date))
            else:
                cur.execute("DELETE FROM events WHERE user_id = %s", (user_id,))
            
            deleted_count = cur.rowcount
            cur.close()
            
            logger.info(f"🗑️ Deleted {deleted_count} events for {user_id}")
        
        return {
            "status": "ok",
            "deleted_count": deleted_count,
            "user_id": user_id,
            "date_filter": date
        }
        
    except Exception as e:
        logger.error(f"Error deleting events for {user_id}: {e}", exc_info=True)
        return {"status": "error", "message": str(e)}


@app.get("/digital-twin/{user_id}/history")
def get_history(user_id: str, days: int = 7):
    """Get daily usage history for a user"""
    try:
        if days < 1 or days > 90:
            raise HTTPException(status_code=400, detail="Days must be between 1 and 90")
        
        with get_db() as conn:
            cur = conn.cursor()
            
            today_utc = datetime.utcnow().date()
            start_date = today_utc - timedelta(days=days - 1)
            
            cur.execute("""
                SELECT DATE(event_time) as date, 
                       COALESCE(SUM(duration), 0) as minutes,
                       COUNT(*) as sessions
                FROM events 
                WHERE user_id = %s 
                AND DATE(event_time) BETWEEN %s AND %s
                AND (status = 'STOP' OR status IS NULL)
                GROUP BY DATE(event_time)
                ORDER BY DATE(event_time) DESC
            """, (user_id, start_date, today_utc))
            
            rows = cur.fetchall()
            cur.close()
            
            history = [
                {
                    "date": str(row[0]),
                    "minutes": int(row[1]),
                    "sessions": int(row[2])
                }
                for row in rows
            ]
        
        return {
            "user_id": user_id,
            "days": days,
            "history": history
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting history for {user_id}: {e}", exc_info=True)
        return {"status": "error", "message": str(e)}


@app.get("/digital-twin/{user_id}/games")
def get_games_breakdown(user_id: str, days: int = 7):
    """Get games breakdown for a user"""
    try:
        if days < 1 or days > 90:
            raise HTTPException(status_code=400, detail="Days must be between 1 and 90")
        
        with get_db() as conn:
            cur = conn.cursor()
            
            today_utc = datetime.utcnow().date()
            start_date = today_utc - timedelta(days=days - 1)
            
            cur.execute("""
                SELECT game_name, 
                       COALESCE(SUM(duration), 0) as total_minutes,
                       COUNT(*) as sessions
                FROM events 
                WHERE user_id = %s 
                AND DATE(event_time) BETWEEN %s AND %s
                AND (status = 'STOP' OR status IS NULL)
                GROUP BY game_name
                ORDER BY total_minutes DESC
            """, (user_id, start_date, today_utc))
            
            rows = cur.fetchall()
            cur.close()
            
            games = [
                {
                    "game_name": row[0],
                    "total_minutes": int(row[1]),
                    "sessions": int(row[2])
                }
                for row in rows
            ]
        
        return {
            "user_id": user_id,
            "days": days,
            "games": games
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting games for {user_id}: {e}", exc_info=True)
        return {"status": "error", "message": str(e)}


@app.get("/admin/stats")
def admin_stats():
    """Get admin statistics"""
    try:
        with get_db() as conn:
            cur = conn.cursor()
            
            cur.execute("SELECT COUNT(*) FROM digital_twins")
            total_users = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM digital_twins WHERE is_online = TRUE")
            online_users = cur.fetchone()[0]
            
            today_utc = datetime.utcnow().date()
            cur.execute("""
                SELECT COUNT(*) FROM events WHERE DATE(event_time) = %s
            """, (today_utc,))
            events_today = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM events")
            total_events = cur.fetchone()[0]
            
            cur.close()
        
        return {
            "total_users": total_users,
            "online_users": online_users,
            "events_today": events_today,
            "total_events": total_events,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting admin stats: {e}", exc_info=True)
        return {"status": "error", "message": str(e)}


# ========== ERROR HANDLERS ==========

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Global exception handler - backward compatible error format"""
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"status": "error", "message": "Internal server error"}
    )
