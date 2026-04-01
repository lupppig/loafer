"""Transform: MongoDB Analytics → CSV

Flattens nested metadata and geo objects into top-level columns,
converts duration to minutes, adds a session_bucket for grouping,
and drops rows without a valid user_id.
"""


def transform(data: list[dict]) -> list[dict]:
    result = []
    for row in data:
        user_id = row.get("user_id")
        if user_id is None:
            continue

        metadata = row.get("metadata") or {}
        if not isinstance(metadata, dict):
            metadata = {}

        geo = row.get("geo") or {}
        if not isinstance(geo, dict):
            geo = {}

        duration_seconds = row.get("duration_seconds", 0)
        try:
            duration_seconds = int(duration_seconds) if duration_seconds is not None else 0
        except (ValueError, TypeError):
            duration_seconds = 0

        duration_minutes = round(duration_seconds / 60, 2)

        occurred_at = row.get("occurred_at")
        if occurred_at and hasattr(occurred_at, "isoformat"):
            occurred_at = occurred_at.isoformat()

        session_bucket = (
            f"bucket_{(int(user_id) // 1000) * 1000}-{(int(user_id) // 1000 + 1) * 1000 - 1}"
        )

        result.append(
            {
                "event_id": row.get("_id"),
                "user_id": user_id,
                "event_type": row.get("event_type"),
                "page": row.get("page"),
                "duration_seconds": duration_seconds,
                "duration_minutes": duration_minutes,
                "device": row.get("device"),
                "browser": row.get("browser"),
                "occurred_at": occurred_at,
                "session_id": metadata.get("session_id"),
                "app_version": metadata.get("version"),
                "referrer": metadata.get("referrer"),
                "latitude": geo.get("lat"),
                "longitude": geo.get("lng"),
                "session_bucket": session_bucket,
            }
        )
    return result
