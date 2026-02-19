from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, List, Tuple

REQUIRED_FIELDS = [
    "ts",
    "device_id",
    "lat",
    "lon",
    "ph",
    "turbidity",
    "temperature",
    "flow",
    "battery",
    "nonce",
]

RANGES = {
    "ph": (0.0, 14.0),
    "turbidity": (0.0, 2000.0),
    "temperature": (-5.0, 50.0),
    "flow": (0.0, 500.0),
    "battery": (0.0, 100.0),
    "lat": (-90.0, 90.0),
    "lon": (-180.0, 180.0),
}


def validate_payload(payload: Dict) -> Tuple[bool, List[str], List[str]]:
    errors: List[str] = []
    range_flags: List[str] = []
    for field in REQUIRED_FIELDS:
        if field not in payload:
            errors.append(f"missing:{field}")

    if errors:
        return False, errors, range_flags

    for key, (min_v, max_v) in RANGES.items():
        value = payload.get(key)
        if value is None:
            errors.append(f"missing:{key}")
            continue
        try:
            value = float(value)
        except (TypeError, ValueError):
            errors.append(f"invalid:{key}")
            continue
        if not (min_v <= value <= max_v):
            range_flags.append(f"out_of_range:{key}")

    try:
        datetime.fromisoformat(str(payload["ts"]).replace("Z", "+00:00"))
    except Exception:
        errors.append("invalid:ts")

    if not str(payload["device_id"]).strip():
        errors.append("invalid:device_id")

    if not str(payload["nonce"]).strip():
        errors.append("invalid:nonce")

    return len(errors) == 0, errors, range_flags


def is_timestamp_recent(ts_iso: str, max_skew_seconds: int) -> bool:
    try:
        ts = datetime.fromisoformat(str(ts_iso).replace("Z", "+00:00")).astimezone(timezone.utc)
        now = datetime.now(timezone.utc)
        skew = abs((now - ts).total_seconds())
        return skew <= max_skew_seconds
    except Exception:
        return False
