from __future__ import annotations

import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parents[1]
if str(SRC_DIR) not in sys.path:
    sys.path.append(str(SRC_DIR))

from pipeline.storage import insert_security_event, insert_telemetry, insert_tls_metric, init_db

DEVICES = ["device_001", "device_002", "device_003"]
LOCATIONS = {
    "device_001": (36.7783, -119.4179),
    "device_002": (34.0522, -118.2437),
    "device_003": (37.7749, -122.4194),
}


def _iso(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "Z")


def seed() -> None:
    init_db()
    now = datetime.now(timezone.utc)
    for i in range(240):
        dt = now - timedelta(minutes=240 - i)
        for device in DEVICES:
            lat, lon = LOCATIONS[device]
            ph = random.uniform(6.8, 8.2)
            turbidity = random.uniform(1.0, 40.0)
            temperature = random.uniform(12.0, 26.0)
            flow = random.uniform(20.0, 140.0)
            battery = max(20.0, 100.0 - i * 0.2 + random.uniform(-1, 1))

            status = "ok"
            reason = None
            if random.random() < 0.05:
                status = "anomaly"
                turbidity = random.uniform(350.0, 900.0)
                reason = "out_of_range:turbidity"

            insert_telemetry(
                {
                    "ts": _iso(dt),
                    "device_id": device,
                    "lat": lat + random.uniform(-0.01, 0.01),
                    "lon": lon + random.uniform(-0.01, 0.01),
                    "ph": round(ph, 2),
                    "turbidity": round(turbidity, 2),
                    "temperature": round(temperature, 2),
                    "flow": round(flow, 2),
                    "battery": round(battery, 2),
                    "nonce": f"seed-{device}-{i}",
                    "status": status,
                    "reason": reason,
                }
            )

    for i in range(15):
        insert_security_event(
            {
                "ts": _iso(now - timedelta(minutes=random.randint(1, 180))),
                "event_type": random.choice([
                    "replay_detected",
                    "unauthorized_device",
                    "stale_message",
                    "invalid_payload",
                ]),
                "device_id": random.choice(DEVICES),
                "severity": random.choice(["low", "medium", "high", "critical"]),
                "detail": {"note": "mock event"},
            }
        )

    for i in range(30):
        insert_tls_metric(
            {
                "ts": _iso(now - timedelta(minutes=random.randint(1, 240))),
                "handshake_ms": random.uniform(20.0, 140.0),
                "cipher": "TLS_AES_256_GCM_SHA384",
                "tls_version": "TLSv1.3",
                "success": True,
            }
        )


if __name__ == "__main__":
    seed()
    print("[seed] mock data generated")
