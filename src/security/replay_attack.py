from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import paho.mqtt.client as mqtt

SRC_DIR = Path(__file__).resolve().parents[1]
if str(SRC_DIR) not in sys.path:
    sys.path.append(str(SRC_DIR))

from pipeline.config import CERTS_DIR, MQTT
from pipeline.storage import fetch_last_telemetry


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def main() -> None:
    parser = argparse.ArgumentParser(description="Simulate an MQTT replay attack")
    parser.add_argument("--device", default="device_001")
    args = parser.parse_args()

    last = fetch_last_telemetry(args.device)
    if not last:
        print("[replay] no telemetry found to replay")
        return

    payload = {
        "ts": _now_iso(),
        "device_id": last["device_id"],
        "lat": last["lat"],
        "lon": last["lon"],
        "ph": last["ph"],
        "turbidity": last["turbidity"],
        "temperature": last["temperature"],
        "flow": last["flow"],
        "battery": last["battery"],
        "nonce": last["nonce"],
    }

    client = mqtt.Client(client_id="replay-attacker", protocol=mqtt.MQTTv311)
    client.tls_set(
        ca_certs=str(CERTS_DIR / "ca.crt"),
        certfile=str(CERTS_DIR / f"{args.device}.crt"),
        keyfile=str(CERTS_DIR / f"{args.device}.key"),
    )
    client.connect(MQTT["host"], MQTT["port"], keepalive=60)
    client.loop_start()
    client.publish(MQTT["topic"], json.dumps(payload), qos=1)
    client.loop_stop()
    client.disconnect()
    print("[replay] attack payload published")


if __name__ == "__main__":
    main()
