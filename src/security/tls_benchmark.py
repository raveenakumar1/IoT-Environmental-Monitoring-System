from __future__ import annotations

import argparse
import socket
import ssl
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parents[1]
if str(SRC_DIR) not in sys.path:
    sys.path.append(str(SRC_DIR))

from pipeline.config import CERTS_DIR, MQTT
from pipeline.storage import insert_tls_metric, init_db


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def run_once(host: str, port: int) -> dict:
    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=str(CERTS_DIR / "ca.crt"))
    context.load_cert_chain(
        certfile=str(CERTS_DIR / "pipeline_client.crt"),
        keyfile=str(CERTS_DIR / "pipeline_client.key"),
    )
    start = time.perf_counter()
    try:
        with socket.create_connection((host, port), timeout=5) as sock:
            with context.wrap_socket(sock, server_hostname=host) as ssock:
                handshake_ms = (time.perf_counter() - start) * 1000.0
                cipher = ssock.cipher()[0] if ssock.cipher() else None
                tls_version = ssock.version()
                return {
                    "ts": _now_iso(),
                    "handshake_ms": handshake_ms,
                    "cipher": cipher,
                    "tls_version": tls_version,
                    "success": True,
                }
    except Exception as exc:
        handshake_ms = (time.perf_counter() - start) * 1000.0
        return {
            "ts": _now_iso(),
            "handshake_ms": handshake_ms,
            "cipher": None,
            "tls_version": None,
            "success": False,
            "error": str(exc),
        }


def main() -> None:
    parser = argparse.ArgumentParser(description="TLS benchmark for MQTT broker")
    parser.add_argument("--iterations", type=int, default=25)
    parser.add_argument("--host", default=MQTT["host"])
    parser.add_argument("--port", type=int, default=MQTT["port"])
    args = parser.parse_args()

    init_db()
    for _ in range(args.iterations):
        metric = run_once(args.host, args.port)
        insert_tls_metric(metric)
        status = "ok" if metric.get("success") else "fail"
        print(f"[tls] {status} {metric['handshake_ms']:.2f}ms")
        time.sleep(0.1)


if __name__ == "__main__":
    main()
