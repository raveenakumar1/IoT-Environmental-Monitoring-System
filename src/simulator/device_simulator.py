from __future__ import annotations

import argparse
import json
import random
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import paho.mqtt.client as mqtt

SRC_DIR = Path(__file__).resolve().parents[1]
if str(SRC_DIR) not in sys.path:
    sys.path.append(str(SRC_DIR))

from pipeline.config import MQTT, CERTS_DIR

DEVICE_CERTS = {
    "device_001": (CERTS_DIR / "device_001.crt", CERTS_DIR / "device_001.key"),
    "device_002": (CERTS_DIR / "device_002.crt", CERTS_DIR / "device_002.key"),
    "device_003": (CERTS_DIR / "device_003.crt", CERTS_DIR / "device_003.key"),
}

DEVICE_LOCATIONS = {
    "device_001": (36.7783, -119.4179),
    "device_002": (34.0522, -118.2437),
    "device_003": (37.7749, -122.4194),
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def generate_payload(device_id: str, anomaly_rate: float) -> dict:
    base_lat, base_lon = DEVICE_LOCATIONS[device_id]
    lat = base_lat + random.uniform(-0.01, 0.01)
    lon = base_lon + random.uniform(-0.01, 0.01)

    ph = random.uniform(6.5, 8.5)
    turbidity = random.uniform(1.0, 50.0)
    temperature = random.uniform(10.0, 28.0)
    flow = random.uniform(10.0, 120.0)
    battery = random.uniform(40.0, 100.0)

    if random.random() < anomaly_rate:
        ph = random.uniform(0.5, 14.0)
        turbidity = random.uniform(200.0, 1200.0)

    return {
        "ts": _now_iso(),
        "device_id": device_id,
        "lat": round(lat, 6),
        "lon": round(lon, 6),
        "ph": round(ph, 2),
        "turbidity": round(turbidity, 2),
        "temperature": round(temperature, 2),
        "flow": round(flow, 2),
        "battery": round(battery, 2),
        "nonce": uuid.uuid4().hex,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Simulated water sensor device")
    parser.add_argument("--device", default="device_001", choices=DEVICE_CERTS.keys())
    parser.add_argument("--interval", type=float, default=2.0)
    parser.add_argument("--anomaly-rate", type=float, default=0.08)
    args = parser.parse_args()

    cert_path, key_path = DEVICE_CERTS[args.device]

    client = mqtt.Client(client_id=args.device, protocol=mqtt.MQTTv311)
    client.tls_set(
        ca_certs=str(CERTS_DIR / "ca.crt"),
        certfile=str(cert_path),
        keyfile=str(key_path),
    )
    client.connect(MQTT["host"], MQTT["port"], keepalive=60)
    client.loop_start()

    print(f"[simulator] publishing as {args.device}")
    try:
        while True:
            payload = generate_payload(args.device, args.anomaly_rate)
            client.publish(MQTT["topic"], json.dumps(payload), qos=1)
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("[simulator] stopped")
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
