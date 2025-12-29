from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from typing import Optional
from datetime import datetime, time, timedelta
import os
import psycopg2
import json
import requests

app = FastAPI(title="Gaming Twin Backend")
ML_URL = os.getenv("ML_API_URL", "http://localhost:9100/analyze-ml")

@app.middleware("http")
async def api_key_auth(request: Request, call_next):
    if request.url.path in ["/", "/health", "/parent/login"]:
        return await call_next(request)
    api_key_header = request.headers.get("X-API-KEY")
    expected = os.getenv("API_KEY")
    if not expected or api_key_header != expected:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return await call_next(request)

# ---------- Updated Models ----------
class GamingEvent(BaseModel):
    user_id: str
    childdeviceid: str
    package_name: str
    game_name: str
    start_time: int
    end_time: int
    duration: int
    timestamp: int
    status: str  # NEW: "START", "HEARTBEAT", "STOP"

class ThresholdUpdate(BaseModel):
    daily: Optional[int] = None
    night: Optional[int] = None

class ParentLoginRequest(BaseModel):
    username: str
    password: str

# ---------- Database ----------
def get_db_connection():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(db_url)

def is_night(now: datetime) -> bool:
    start = time(22, 0)
    end = time(6, 0)
    t = now.time()
    return t >= start or t < end

def cleanup_stale_sessions(conn):
    """Background task: Set offline if no heartbeat > 5 min"""
    cur = conn.cursor()
    five_min_ago = datetime.utcnow() - timedelta(minutes=5)
    
    cur.execute("""
        UPDATE digital_twins 
        SET is_online = FALSE, current_game = NULL 
        WHERE is_online = TRUE 
        AND (last_heartbeat_at IS NULL OR last_heartbeat_at < %s)
    """, (five_min_ago,))
    
    conn.commit()
    cur.close()

def check_and_trigger_alert(conn, user_id: str):
    """Check if daily limit exceeded and trigger alert logic"""
    cur = conn.cursor()
    cur.execute("""
        SELECT aggregates, thresholds 
        FROM digital_twins 
        WHERE user_id = %s
    """, (user_id,))
    row = cur.fetchone()
    cur.close()
    
    if row:
        aggregates = row[0] or {}
        thresholds = row[1] or {"daily": 120}
        today_minutes = aggregates.get("today_minutes", 0)
        daily_limit = thresholds.get("daily", 120)
        
        if today_minutes > daily_limit:
            # Alert logic: In production, send push notification here
            print(f"🚨 ALERT: {user_id} exceeded daily limit {today_minutes}/{daily_limit} min")
            # TODO: Integrate with parent push notification service

def update_aggregates(conn, user_id: str, duration: int, event_time: datetime, status: str):
    cur = conn.cursor()

    # Ensure twin row exists
    cur.execute("""
        INSERT INTO digital_twins (user_id, thresholds, aggregates, state, risk_level, alert_message, 
                                   is_online, current_game, last_heartbeat_at, created_at, last_updated)
        VALUES (%s, '{"daily":120,"night":60}', '{"today_minutes":0,"weekly_minutes":0,"night_minutes":0,"sessions_per_day":0}',
                'Healthy', 'Unknown', '', FALSE, NULL, NULL, NOW(), NOW())
        ON CONFLICT (user_id) DO NOTHING
    """, (user_id,))

    # Recompute aggregates from events
    today = event_time.date()
    week_start = today.fromordinal(today.toordinal() - 6)
    
    cur.execute("""
        SELECT duration, event_time FROM events 
        WHERE user_id = %s AND event_time::date BETWEEN %s AND %s
    """, (user_id, week_start, today))
    rows = cur.fetchall()

    today_minutes = 0
    weekly_minutes = 0
    night_minutes = 0
    sessions_per_day = 0
    day_sessions = {}

    for dur, ts in rows:
        d = ts.date()
        weekly_minutes += dur
        if d == today:
            today_minutes += dur
            day_sessions[d] = day_sessions.get(d, 0) + 1
        if is_night(ts):
            night_minutes += dur

    sessions_per_day = day_sessions.get(today, 0)

    aggregates = {
        "today_minutes": today_minutes,
        "weekly_minutes": weekly_minutes,
        "night_minutes": night_minutes,
        "sessions_per_day": sessions_per_day,
    }

    # Get thresholds
    cur.execute("SELECT thresholds FROM digital_twins WHERE user_id = %s", (user_id,))
    row = cur.fetchone()
    thresholds = row[0] if row and row[0] else {"daily": 120, "night": 60}

    # Fallback state
    daily_th = thresholds.get("daily", 120)
    if today_minutes <= daily_th * 0.5:
        state = "Healthy"
    elif today_minutes <= daily_th:
        state = "Moderate"
    else:
        state = "Excessive"

    risk_level = "Unknown"
    alert_message = ""

    # ML API call
    twin_json = {"user_id": user_id, "thresholds": thresholds, "aggregates": aggregates, "state": state}
    try:
        ml_response = requests.post(ML_URL, json={"digital_twin": twin_json}, timeout=3).json()
        state = ml_response.get("state", state)
        risk_level = ml_response.get("severity", risk_level)
        alert_message = ml_response.get("alertmessage", alert_message)
        if isinstance(alert_message, list):
            alert_message = "; ".join(alert_message)
    except Exception:
        pass

    # ---------- LIVE STATUS LOGIC ----------
    if status == "START":
        cur.execute("""
            UPDATE digital_twins 
            SET is_online = TRUE, current_game = %s, last_heartbeat_at = NOW()
            WHERE user_id = %s
        """, (event.game_name, user_id))
    elif status == "HEARTBEAT":
        cur.execute("""
            UPDATE digital_twins 
            SET last_heartbeat_at = NOW()
            WHERE user_id = %s
        """, (user_id,))
        check_and_trigger_alert(conn, user_id)  # Instant alert check
    elif status == "STOP":
        cur.execute("""
            UPDATE digital_twins 
            SET is_online = FALSE, current_game = NULL
            WHERE user_id = %s
        """, (user_id,))
    # --------------------------------------

    # Update aggregates and ML results
    cur.execute("""
        UPDATE digital_twins
        SET aggregates = %s, thresholds = %s, state = %s, risk_level = %s, 
            alert_message = %s, last_updated = NOW()
        WHERE user_id = %s
    """, (json.dumps(aggregates), json.dumps(thresholds), state, risk_level, alert_message, user_id))

    conn.commit()
    cur.close()

# ---------- Updated Endpoints ----------
@app.get("/health")
def health():
    try:
        conn = get_db_connection()
        cleanup_stale_sessions(conn)  # Clean stale sessions on health check
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        cur.close()
        conn.close()
        return {"status": "ok"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/events")
def ingest_event(event: GamingEvent):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        event_dt = datetime.utcfromtimestamp(event.timestamp / 1000.0)

        # Insert event
        cur.execute("""
            INSERT INTO events (user_id, childdeviceid, package_name, game_name, duration, 
                               event_time, start_time_bigint, end_time_bigint, event_timestamp_bigint)
            VALUES (%s, %s, %s, %s, %s, TO_TIMESTAMP(%s / 1000.0), %s, %s, %s)
        """, (event.user_id, event.childdeviceid, event.package_name, event.game_name, 
              event.duration, event.timestamp, event.start_time, event.end_time, event.timestamp))

        cur.execute("UPDATE events SET isnight = %s WHERE id = currval('events_id_seq')", (is_night(event_dt),))

        # Update twin with status logic
        update_aggregates(conn, event.user_id, event.duration, event_dt, event.status)

        conn.commit()
        cur.close()
        conn.close()
        return {"status": "processed", "user_id": event.user_id, "status": event.status}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/digital-twin/{user_id}")
def get_twin(user_id: str):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT user_id, thresholds, aggregates, state, risk_level, alert_message, 
                   is_online, current_game, last_heartbeat_at
            FROM digital_twins WHERE user_id = %s
        """, (user_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()

        if not row:
            raise HTTPException(status_code=404, detail="User not found")

        return {
            "user_id": row[0],
            "thresholds": row[1],
            "aggregates": row[2],
            "state": row[3],
            "risk_level": row[4],
            "alert_message": row[5],
            "is_online": row[6],           # NEW
            "current_game": row[7],         # NEW
            "last_heartbeat_at": row[8]     # NEW
        }
    except HTTPException:
        raise
    except Exception as e:
        return {"status": "error", "message": str(e)}

# Keep other endpoints same (reports, threshold, parent/login)
@app.get("/reports/{user_id}")
def get_reports(user_id: str):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT aggregates FROM digital_twins WHERE user_id = %s", (user_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()

        if not row:
            raise HTTPException(status_code=404, detail="User not found")

        aggregates = row[0] or {}
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
        return {"status": "error", "message": str(e)}

@app.post("/digital-twin/{user_id}/threshold")
def update_threshold(user_id: str, body: ThresholdUpdate):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT thresholds FROM digital_twins WHERE user_id = %s", (user_id,))
        row = cur.fetchone()
        thresholds = row[0] if row and row[0] else {"daily": 120, "night": 60}

        if body.daily is not None: thresholds["daily"] = body.daily
        if body.night is not None: thresholds["night"] = body.night

        cur.execute("UPDATE digital_twins SET thresholds = %s, last_updated = NOW() WHERE user_id = %s",
                   (json.dumps(thresholds), user_id))
        conn.commit()
        cur.close()
        conn.close()
        return {"user_id": user_id, "thresholds": thresholds}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/parent/login")
def parent_login(body: ParentLoginRequest):
    return {
        "success": True,
        "parent_id": "parent_001",
        "token": "jwt_token_here",
        "children": [{"child_id": "child_101", "name": "John"}]
    }
