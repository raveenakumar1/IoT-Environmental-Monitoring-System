from __future__ import annotations

import json
import sys
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Deque, Dict, Set

import paho.mqtt.client as mqtt

SRC_DIR = Path(__file__).resolve().parents[1]
if str(SRC_DIR) not in sys.path:
    sys.path.append(str(SRC_DIR))

from pipeline.config import MQTT, SECURITY
from pipeline.storage import insert_security_event, insert_telemetry, init_db
from pipeline.validator import is_timestamp_recent, validate_payload

ALLOWED_DEVICES = {"device_001", "device_002", "device_003"}


class NonceCache:
    def __init__(self, max_size: int) -> None:
        self.max_size = max_size
        self.order: Deque[str] = deque()
        self.cache: Set[str] = set()

    def seen(self, nonce: str) -> bool:
        return nonce in self.cache

    def add(self, nonce: str) -> None:
        if nonce in self.cache:
            return
        self.cache.add(nonce)
        self.order.append(nonce)
        if len(self.order) > self.max_size:
            oldest = self.order.popleft()
            self.cache.discard(oldest)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class IngestService:
    def __init__(self) -> None:
        self.nonce_cache = NonceCache(SECURITY["nonce_cache_size"])
        self.client = mqtt.Client(client_id=MQTT["client_id"], protocol=mqtt.MQTTv311)
        self.client.tls_set(
            ca_certs=MQTT["ca_cert"],
            certfile=MQTT["client_cert"],
            keyfile=MQTT["client_key"],
        )
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

    def on_connect(self, client: mqtt.Client, userdata, flags, rc) -> None:
        if rc == 0:
            client.subscribe(MQTT["topic"])
            print("[pipeline] connected and subscribed")
        else:
            print(f"[pipeline] connection failed: {rc}")

    def on_message(self, client: mqtt.Client, userdata, msg: mqtt.MQTTMessage) -> None:
        payload_raw = msg.payload.decode("utf-8", errors="ignore")
        try:
            payload: Dict = json.loads(payload_raw)
        except json.JSONDecodeError:
            insert_security_event(
                {
                    "ts": _now_iso(),
                    "event_type": "invalid_payload",
                    "device_id": None,
                    "severity": "high",
                    "detail": {"reason": "invalid_json"},
                }
            )
            return

        valid, errors, range_flags = validate_payload(payload)
        device_id = payload.get("device_id")
        if not valid:
            insert_security_event(
                {
                    "ts": _now_iso(),
                    "event_type": "invalid_payload",
                    "device_id": device_id,
                    "severity": "high",
                    "detail": {"errors": errors},
                }
            )
            return

        if device_id not in ALLOWED_DEVICES:
            insert_security_event(
                {
                    "ts": _now_iso(),
                    "event_type": "unauthorized_device",
                    "device_id": device_id,
                    "severity": "high",
                    "detail": {"reason": "device_not_whitelisted"},
                }
            )
            return

        if not is_timestamp_recent(payload["ts"], SECURITY["max_skew_seconds"]):
            insert_security_event(
                {
                    "ts": _now_iso(),
                    "event_type": "stale_message",
                    "device_id": device_id,
                    "severity": "medium",
                    "detail": {"reason": "timestamp_skew"},
                }
            )

        nonce = str(payload.get("nonce"))
        if self.nonce_cache.seen(nonce):
            insert_security_event(
                {
                    "ts": _now_iso(),
                    "event_type": "replay_detected",
                    "device_id": device_id,
                    "severity": "critical",
                    "detail": {"nonce": nonce},
                }
            )
            return

        self.nonce_cache.add(nonce)

        status = "ok"
        reason = None
        if range_flags:
            status = "anomaly"
            reason = ",".join(range_flags)

        record = {
            "ts": payload["ts"],
            "device_id": device_id,
            "lat": float(payload["lat"]),
            "lon": float(payload["lon"]),
            "ph": float(payload["ph"]),
            "turbidity": float(payload["turbidity"]),
            "temperature": float(payload["temperature"]),
            "flow": float(payload["flow"]),
            "battery": float(payload["battery"]),
            "nonce": nonce,
            "status": status,
            "reason": reason,
        }
        insert_telemetry(record)

    def start(self) -> None:
        init_db()
        self.client.connect(MQTT["host"], MQTT["port"], keepalive=60)
        self.client.loop_forever()


if __name__ == "__main__":
    service = IngestService()
    while True:
        try:
            service.start()
        except Exception as exc:
            print(f"[pipeline] error: {exc}")
            time.sleep(3)
