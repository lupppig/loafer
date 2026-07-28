def transform(data):
    """Add a boolean `is_active` column derived from the status field."""
    return [{**row, "is_active": row.get("status") == "active"} for row in data]
