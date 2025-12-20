from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from typing import Optional
from datetime import datetime, time
import os
import psycopg2
import json
import requests  # for ML API call
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Gaming Twin Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://localhost"
        "http://localhost:5173",   # Vite dev
        "http://localhost:3000",   # fallback
        "https://gaming-twin-backend.onrender.com"
        
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],  # IMPORTANT for X-API-KEY
)


# ML URL: default is local, can be overridden via env var ML_API_URL on Render
ML_URL = os.getenv("ML_API_URL", "http://localhost:9100/analyze-ml")


# ---------- API key middleware ----------
@app.middleware("http")
async def api_key_auth(request: Request, call_next):
    # Allow preflight requests
    if request.method == "OPTIONS":
        return await call_next(request)

    # Allow unauthenticated routes
    if request.url.path in ["/", "/health", "/parent/login", "/auth/register"]:
        return await call_next(request)

    api_key_header = request.headers.get("X-API-KEY")
    expected = os.getenv("API_KEY")
    if not expected or api_key_header != expected:
        raise HTTPException(status_code=401, detail="Unauthorized")

    return await call_next(request)


# ---------- Models ----------
class GamingEvent(BaseModel):
    user_id: str
    childdeviceid: str
    package_name: str
    game_name: str
    start_time: int       # epoch ms
    end_time: int         # epoch ms
    timestamp: int        # epoch ms (session end)
    duration: int         # minutes


class ThresholdUpdate(BaseModel):
    daily: Optional[int] = None
    night: Optional[int] = None


class ParentLoginRequest(BaseModel):
    username: str
    password: str

class ParentRegisterRequest(BaseModel):
    email: str
    password: str
    name: Optional[str] = ""
    child_name: Optional[str] = ""
    child_age: Optional[int] = None


class ParentLoginRequest(BaseModel):
    email: str
    password: str

class AlertCreate(BaseModel):
    severity: str
    message: str

# ---------- Database connection ----------
def get_db_connection():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(db_url)


# ---------- Helper functions ----------
def init_db():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS parents (
        parent_id SERIAL PRIMARY KEY,
        email TEXT UNIQUE,
        password TEXT,
        name TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS children (
        child_id TEXT PRIMARY KEY,
        parent_id INTEGER REFERENCES parents(parent_id),
        name TEXT,
        age INTEGER
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS digital_twins (
        user_id TEXT PRIMARY KEY,
        thresholds JSONB,
        aggregates JSONB,
        state TEXT,
        risk_level TEXT,
        alert_message TEXT,
        created_at TIMESTAMP,
        last_updated TIMESTAMP
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS events (
        id SERIAL PRIMARY KEY,
        user_id TEXT,
        childdeviceid TEXT,
        package_name TEXT,
        game_name TEXT,
        duration INTEGER,
        event_time TIMESTAMP,
        isnight BOOLEAN,
        start_time_bigint BIGINT,
        end_time_bigint BIGINT,
        event_timestamp_bigint BIGINT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS alerts (
        id SERIAL PRIMARY KEY,
        user_id TEXT,
        severity TEXT,
        message TEXT,
        created_at TIMESTAMP DEFAULT NOW()
    )
    """)

    conn.commit()
    cur.close()
    conn.close()


def is_night(now: datetime) -> bool:
    start = time(22, 0)
    end = time(6, 0)
    t = now.time()
    return t >= start or t < end


def update_aggregates(conn, user_id: str, duration: int, event_time: datetime):
    cur = conn.cursor()

    # ensure twin row exists
    cur.execute(
        """
        INSERT INTO digital_twins (user_id, thresholds, aggregates, state, risk_level, alert_message, created_at, last_updated)
        VALUES (%s, '{"daily":120,"night":60}',
                 '{"today_minutes":0,"weekly_minutes":0,"night_minutes":0,"sessions_per_day":0}',
                 'Healthy', 'Unknown', '', NOW(), NOW())
        ON CONFLICT (user_id) DO NOTHING
        """,
        (user_id,)
    )

    # today / week boundaries
    today = event_time.date()
    week_start = today.fromordinal(today.toordinal() - 6)

    # recompute aggregates from events
    cur.execute(
        """
        SELECT duration, event_time
        FROM events
        WHERE user_id = %s
        AND event_time::date BETWEEN %s AND %s
        """,
        (user_id, week_start, today)
    )
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

    # get thresholds
    cur.execute(
        "SELECT thresholds FROM digital_twins WHERE user_id = %s",
        (user_id,)
    )
    row = cur.fetchone()
    if row and row[0]:
        thresholds = row[0]
    else:
        thresholds = {"daily": 120, "night": 60}

    # fallback rule-based state
    daily_th = thresholds.get("daily", 120)
    if today_minutes <= daily_th * 0.5:
        state = "Healthy"
    elif today_minutes <= daily_th:
        state = "Moderate"
    else:
        state = "Excessive"

    risk_level = "Unknown"
    alert_message: list[str] | str = []

    # ---------- CALL MEMBER 3 ML API (FINAL CONTRACT) ----------
    twin_json = {
        "user_id": user_id,
        "thresholds": thresholds,
        "aggregates": aggregates,
        "state": state,
    }

    try:
        ml_response = requests.post(
            ML_URL,
            json={"digital_twin": twin_json},
            timeout=3,
        ).json()

        # Use ML output (confirmed keys)
        state = ml_response.get("state", state)
        risk_level = ml_response.get("severity", risk_level)
        alert_message = ml_response.get("alertmessage", alert_message)
        if isinstance(alert_message, list):
            alert_message = "; ".join(alert_message)
    except Exception:
        # ML down or error → keep fallback
        if isinstance(alert_message, list):
            alert_message = ""
    # -----------------------------------------------------------

    cur.execute(
        """
        UPDATE digital_twins
        SET aggregates = %s,
            thresholds = %s,
            state = %s,
            risk_level = %s,
            alert_message = %s,
            last_updated = NOW()
        WHERE user_id = %s
        """,
        (json.dumps(aggregates), json.dumps(thresholds), state, risk_level, alert_message, user_id)
    )

    conn.commit()
    cur.close()


# ---------- Endpoints ----------
@app.get("/health")
def health():
    try:
        conn = get_db_connection()
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

        cur.execute(
            """
            INSERT INTO events (
                user_id,
                childdeviceid,
                package_name,
                game_name,
                duration,
                event_time,
                start_time_bigint,
                end_time_bigint,
                event_timestamp_bigint
            )
            VALUES (%s, %s, %s, %s, %s,
                    TO_TIMESTAMP(%s / 1000.0),
                    %s, %s, %s)
            """,
            (
                event.user_id,
                event.childdeviceid,
                event.package_name,
                event.game_name,
                event.duration,
                event.timestamp,
                event.start_time,
                event.end_time,
                event.timestamp,
            )
        )

        # mark night usage based on event time
        cur.execute(
            "UPDATE events SET isnight = %s WHERE id = currval('events_id_seq')",
            (is_night(event_dt),)
        )

        update_aggregates(conn, event.user_id, event.duration, event_dt)

        conn.commit()
        cur.close()
        conn.close()
        return {"status": "processed", "user_id": event.user_id}
    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.get("/digital-twin/{user_id}")
def get_twin(user_id: str):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT user_id, thresholds, aggregates, state, risk_level, alert_message
            FROM digital_twins
            WHERE user_id = %s
            """,
            (user_id,)
        )
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
        }
    except HTTPException:
        raise
    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.get("/reports/{user_id}")
def get_reports(user_id: str):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT aggregates
            FROM digital_twins
            WHERE user_id = %s
            """,
            (user_id,)
        )
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

        cur.execute(
            "SELECT thresholds FROM digital_twins WHERE user_id = %s",
            (user_id,)
        )
        row = cur.fetchone()
        if row and row[0]:
            thresholds = row[0]
        else:
            thresholds = {"daily": 120, "night": 60}

        if body.daily is not None:
            thresholds["daily"] = body.daily
        if body.night is not None:
            thresholds["night"] = body.night

        cur.execute(
            """
            UPDATE digital_twins
            SET thresholds = %s,
                last_updated = NOW()
            WHERE user_id = %s
            """,
            (json.dumps(thresholds), user_id)
        )
        conn.commit()
        cur.close()
        conn.close()

        return {"user_id": user_id, "thresholds": thresholds}
    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.post("/parent/login")
def parent_login(body: ParentLoginRequest):
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT p.parent_id, c.child_id, c.name
            FROM parents p
            JOIN children c ON c.parent_id = p.parent_id
            WHERE p.email = %s AND p.password = %s
            """,
            (body.email, body.password)
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()

        if not rows:
            raise HTTPException(status_code=401, detail="Invalid credentials")

        children = [{"child_id": r[1], "name": r[2]} for r in rows]

        return {
            "success": True,
            "parent_id": rows[0][0],
            "token": "api-key-auth",  # frontend ignores this
            "children": children
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/auth/register")
def parent_register(body: ParentRegisterRequest):
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # create parent
        cur.execute(
            """
            INSERT INTO parents (email, password, name)
            VALUES (%s, %s, %s)
            RETURNING parent_id
            """,
            (body.email, body.password, body.name)
        )
        parent_id = cur.fetchone()[0]

        # create child
        child_id = f"child_{parent_id}"
        cur.execute(
            """
            INSERT INTO children (child_id, parent_id, name, age)
            VALUES (%s, %s, %s, %s)
            """,
            (child_id, parent_id, body.child_name, body.child_age)
        )

        # seed digital twin
        cur.execute(
            """
            INSERT INTO digital_twins
            (user_id, thresholds, aggregates, state, risk_level, alert_message, created_at, last_updated)
            VALUES (
                %s,
                '{"daily":120,"night":60}',
                '{"today_minutes":0,"weekly_minutes":0,"night_minutes":0,"sessions_per_day":0}',
                'Healthy','Unknown','',NOW(),NOW()
            )
            """,
            (child_id,)
        )

        conn.commit()
        cur.close()
        conn.close()

        return {"success": True, "parent_id": parent_id, "child_id": child_id}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/alerts/{user_id}")
def get_alerts(user_id: str):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT severity, message, created_at
            FROM alerts
            WHERE user_id = %s
            ORDER BY created_at DESC
            """,
            (user_id,)
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()

        return [
            {"severity": r[0], "message": r[1], "created_at": r[2]}
            for r in rows
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
        
@app.post("/alerts/{user_id}")
def create_alert(user_id: str, body: AlertCreate):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO alerts (user_id, severity, message)
            VALUES (%s, %s, %s)
            """,
            (user_id, body.severity, body.message)
        )
        conn.commit()
        cur.close()
        conn.close()

        return {"success": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.on_event("startup")
def startup_event():
    init_db()


