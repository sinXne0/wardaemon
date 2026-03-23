"""
Persistent SQLite database — WARDAEMON
Stores all discovered networks and named scan sessions across restarts.
"""
import os
import sqlite3
import threading
from datetime import datetime, timezone

_DB   = os.path.join(os.path.dirname(os.path.dirname(__file__)), "wardaemon.db")
_lock = threading.Lock()

_SCHEMA = """
CREATE TABLE IF NOT EXISTS sessions (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    name          TEXT    DEFAULT '',
    start_time    TEXT    NOT NULL,
    end_time      TEXT,
    network_count INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS networks (
    bssid           TEXT PRIMARY KEY,
    ssid            TEXT    DEFAULT '',
    auth_mode       TEXT    DEFAULT '',
    type            TEXT    DEFAULT 'WIFI',
    manufacturer    TEXT    DEFAULT '',
    first_seen      TEXT,
    last_seen       TEXT,
    best_rssi       INTEGER DEFAULT -100,
    sighting_count  INTEGER DEFAULT 1,
    session_id      INTEGER,
    lat             REAL    DEFAULT 0.0,
    lon             REAL    DEFAULT 0.0,
    alt             REAL    DEFAULT 0.0,
    channel         INTEGER DEFAULT 0,
    evil_twin       INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_nets_type    ON networks(type);
CREATE INDEX IF NOT EXISTS idx_nets_session ON networks(session_id);
CREATE INDEX IF NOT EXISTS idx_nets_ssid    ON networks(ssid);
"""


def _now() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None).strftime("%Y-%m-%dT%H:%M:%S")


def init():
    """Create tables if they don't exist."""
    with _lock:
        c = sqlite3.connect(_DB)
        c.executescript(_SCHEMA)
        c.commit()
        c.close()


def start_session(name: str = "") -> int:
    """Record a new scan session. Returns session ID."""
    now = _now()
    with _lock:
        c = sqlite3.connect(_DB)
        cur = c.execute("INSERT INTO sessions (name, start_time) VALUES (?,?)", (name, now))
        sid = cur.lastrowid
        c.commit()
        c.close()
    return sid


def end_session(sid: int, count: int):
    """Mark session as ended."""
    with _lock:
        c = sqlite3.connect(_DB)
        c.execute(
            "UPDATE sessions SET end_time=?, network_count=? WHERE id=?",
            (_now(), count, sid),
        )
        c.commit()
        c.close()


def upsert(net: dict, session_id=None):
    """Insert or update a network record in the persistent DB."""
    bssid = (net.get("bssid") or "").upper()
    if not bssid:
        return
    now = _now()
    with _lock:
        c = sqlite3.connect(_DB)
        row = c.execute(
            "SELECT best_rssi FROM networks WHERE bssid=?", (bssid,)
        ).fetchone()
        if row:
            best = max(row[0], net.get("rssi", -100))
            lat = net.get("lat", 0) or 0
            lon = net.get("lon", 0) or 0
            alt = net.get("alt", 0) or 0
            ch  = net.get("channel", 0) or 0
            mfr = net.get("manufacturer", "") or ""
            c.execute("""
                UPDATE networks SET
                    ssid=?, auth_mode=?, last_seen=?, best_rssi=?,
                    sighting_count=sighting_count+1,
                    lat=CASE WHEN ?!=0.0 THEN ? ELSE lat END,
                    lon=CASE WHEN ?!=0.0 THEN ? ELSE lon END,
                    alt=CASE WHEN ?!=0.0 THEN ? ELSE alt END,
                    channel=CASE WHEN ?!=0 THEN ? ELSE channel END,
                    manufacturer=CASE WHEN manufacturer='' AND ?!='' THEN ? ELSE manufacturer END
                WHERE bssid=?
            """, (
                net.get("ssid", ""), net.get("auth_mode", ""), now, best,
                lat, lat, lon, lon, alt, alt, ch, ch, mfr, mfr,
                bssid,
            ))
        else:
            c.execute("""
                INSERT INTO networks
                    (bssid, ssid, auth_mode, type, manufacturer,
                     first_seen, last_seen, best_rssi, sighting_count,
                     session_id, lat, lon, alt, channel)
                VALUES (?,?,?,?,?,?,?,?,1,?,?,?,?,?)
            """, (
                bssid,
                net.get("ssid", ""),
                net.get("auth_mode", ""),
                net.get("type", "WIFI"),
                net.get("manufacturer", ""),
                net.get("first_seen", now),
                now,
                net.get("rssi", -100),
                session_id,
                net.get("lat", 0) or 0,
                net.get("lon", 0) or 0,
                net.get("alt", 0) or 0,
                net.get("channel", 0) or 0,
            ))
        c.commit()
        c.close()


def mark_evil_twin(bssid: str):
    with _lock:
        c = sqlite3.connect(_DB)
        c.execute("UPDATE networks SET evil_twin=1 WHERE bssid=?", (bssid.upper(),))
        c.commit()
        c.close()


def get_network(bssid: str) -> dict:
    with _lock:
        c = sqlite3.connect(_DB)
        c.row_factory = sqlite3.Row
        row = c.execute(
            "SELECT * FROM networks WHERE bssid=?", (bssid.upper(),)
        ).fetchone()
        c.close()
    return dict(row) if row else {}


def get_sessions(limit: int = 50) -> list:
    with _lock:
        c = sqlite3.connect(_DB)
        c.row_factory = sqlite3.Row
        rows = c.execute(
            "SELECT * FROM sessions ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        c.close()
    return [dict(r) for r in rows]


def total_unique() -> int:
    with _lock:
        c = sqlite3.connect(_DB)
        n = c.execute("SELECT COUNT(*) FROM networks").fetchone()[0]
        c.close()
    return n
