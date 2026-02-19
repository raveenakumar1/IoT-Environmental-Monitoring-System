from __future__ import annotations

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data"
CERTS_DIR = BASE_DIR / "certs"

MQTT = {
    "host": "localhost",
    "port": 8883,
    "topic": "water/telemetry",
    "client_id": "pipeline-ingestor",
    "ca_cert": str(CERTS_DIR / "ca.crt"),
    "client_cert": str(CERTS_DIR / "pipeline_client.crt"),
    "client_key": str(CERTS_DIR / "pipeline_client.key"),
}

SECURITY = {
    "max_skew_seconds": 300,
    "nonce_cache_size": 10000,
}
