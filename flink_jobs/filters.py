import os
from datetime import datetime, timedelta, timezone


def _parse_ingested_at(raw_value):
    if raw_value is None:
        return None

    if isinstance(raw_value, (int, float)):
        return datetime.fromtimestamp(float(raw_value) / 1000.0, tz=timezone.utc)

    if isinstance(raw_value, str):
        try:
            parsed = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
        except ValueError:
            return None

        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    return None


def is_recent_record(data, max_age_minutes=None):
    if max_age_minutes is None:
        max_age_minutes = int(os.getenv("MAX_RECORD_AGE_MINUTES", "30"))

    parsed = _parse_ingested_at(data.get("ingested_at"))
    if parsed is None:
        return False

    now = datetime.now(timezone.utc)
    return now - parsed <= timedelta(minutes=max_age_minutes)