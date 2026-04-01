"""Transform: PostgreSQL Users → JSON

Combines first_name + last_name into full_name, normalizes email to lowercase,
filters to active/basic/premium users only, adds age_group, and drops
suspended/pending accounts.
"""


def transform(data: list[dict]) -> list[dict]:
    result = []
    for row in data:
        if row.get("status") in ("suspended", "pending"):
            continue

        age = row.get("age")
        if age is not None:
            if age < 25:
                age_group = "18-24"
            elif age < 35:
                age_group = "25-34"
            elif age < 50:
                age_group = "35-49"
            elif age < 65:
                age_group = "50-64"
            else:
                age_group = "65+"
        else:
            age_group = "unknown"

        result.append(
            {
                "id": row.get("id"),
                "full_name": f"{row.get('first_name', '')} {row.get('last_name', '')}".strip(),
                "email": str(row.get("email", "")).lower(),
                "status": row.get("status"),
                "country": row.get("country"),
                "age": age,
                "age_group": age_group,
                "tier": row.get("tier"),
                "signup_date": row.get("signup_date"),
                "last_login": row.get("last_login"),
            }
        )
    return result
