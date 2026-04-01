def transform(data):
    """Add a letter grade based on score and filter inactive rows."""
    return [
        {
            **row,
            "grade": "A"
            if float(row["score"]) >= 90
            else "B"
            if float(row["score"]) >= 80
            else "C",
        }
        for row in data
        if row["status"] == "active"
    ]
