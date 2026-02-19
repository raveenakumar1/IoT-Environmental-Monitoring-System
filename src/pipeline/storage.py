from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

from .config import DATA_DIR

DB_PATH = DATA_DIR / "pipeline.db"


def _connect() -> sqlite3.Connection:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(DB_PATH)


def init_db() -> None:
    with _connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS telemetry (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                device_id TEXT NOT NULL,
                lat REAL NOT NULL,
                lon REAL NOT NULL,
                ph REAL NOT NULL,
                turbidity REAL NOT NULL,
                temperature REAL NOT NULL,
                flow REAL NOT NULL,
                battery REAL NOT NULL,
                nonce TEXT NOT NULL,
                status TEXT NOT NULL,
                reason TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS security_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                event_type TEXT NOT NULL,
                device_id TEXT,
                severity TEXT NOT NULL,
                detail TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tls_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                handshake_ms REAL NOT NULL,
                cipher TEXT,
                tls_version TEXT,
                success INTEGER NOT NULL
            )
            """
        )
        conn.commit()


def insert_telemetry(record: Dict[str, Any]) -> None:
    init_db()
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO telemetry (
                ts, device_id, lat, lon, ph, turbidity, temperature, flow, battery, nonce, status, reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record["ts"],
                record["device_id"],
                record["lat"],
                record["lon"],
                record["ph"],
                record["turbidity"],
                record["temperature"],
                record["flow"],
                record["battery"],
                record["nonce"],
                record["status"],
                record.get("reason"),
            ),
        )
        conn.commit()


def insert_security_event(event: Dict[str, Any]) -> None:
    init_db()
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO security_events (ts, event_type, device_id, severity, detail)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                event["ts"],
                event["event_type"],
                event.get("device_id"),
                event["severity"],
                json.dumps(event.get("detail"), ensure_ascii=False),
            ),
        )
        conn.commit()


def insert_tls_metric(metric: Dict[str, Any]) -> None:
    init_db()
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO tls_metrics (ts, handshake_ms, cipher, tls_version, success)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                metric["ts"],
                metric["handshake_ms"],
                metric.get("cipher"),
                metric.get("tls_version"),
                1 if metric.get("success", True) else 0,
            ),
        )
        conn.commit()


def fetch_recent_telemetry(limit: int = 500) -> Iterable[Dict[str, Any]]:
    init_db()
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT ts, device_id, lat, lon, ph, turbidity, temperature, flow, battery, nonce, status, reason
            FROM telemetry
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    for row in rows:
        yield {
            "ts": row[0],
            "device_id": row[1],
            "lat": row[2],
            "lon": row[3],
            "ph": row[4],
            "turbidity": row[5],
            "temperature": row[6],
            "flow": row[7],
            "battery": row[8],
            "nonce": row[9],
            "status": row[10],
            "reason": row[11],
        }


def fetch_security_events(limit: int = 200) -> Iterable[Dict[str, Any]]:
    init_db()
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT ts, event_type, device_id, severity, detail
            FROM security_events
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    for row in rows:
        yield {
            "ts": row[0],
            "event_type": row[1],
            "device_id": row[2],
            "severity": row[3],
            "detail": json.loads(row[4]) if row[4] else None,
        }


def fetch_tls_metrics(limit: int = 200) -> Iterable[Dict[str, Any]]:
    init_db()
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT ts, handshake_ms, cipher, tls_version, success
            FROM tls_metrics
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    for row in rows:
        yield {
            "ts": row[0],
            "handshake_ms": row[1],
            "cipher": row[2],
            "tls_version": row[3],
            "success": bool(row[4]),
        }


def fetch_last_telemetry(device_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
    init_db()
    with _connect() as conn:
        if device_id:
            row = conn.execute(
                """
                SELECT ts, device_id, lat, lon, ph, turbidity, temperature, flow, battery, nonce, status, reason
                FROM telemetry
                WHERE device_id = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (device_id,),
            ).fetchone()
        else:
            row = conn.execute(
                """
                SELECT ts, device_id, lat, lon, ph, turbidity, temperature, flow, battery, nonce, status, reason
                FROM telemetry
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()
    if not row:
        return None
    return {
        "ts": row[0],
        "device_id": row[1],
        "lat": row[2],
        "lon": row[3],
        "ph": row[4],
        "turbidity": row[5],
        "temperature": row[6],
        "flow": row[7],
        "battery": row[8],
        "nonce": row[9],
        "status": row[10],
        "reason": row[11],
    }
