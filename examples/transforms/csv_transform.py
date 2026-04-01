"""Transform: CSV Sales Data → PostgreSQL

Normalizes email to lowercase, computes gross_amount (before discount),
adds a processed_at timestamp, standardizes channel and status values,
and drops rows where total is missing or <= 0.
"""

import datetime


def transform(data: list[dict]) -> list[dict]:
    result = []
    for row in data:
        total = row.get("total")
        try:
            total = float(total) if total is not None else 0
        except (ValueError, TypeError):
            total = 0

        if total <= 0:
            continue

        quantity = row.get("quantity", 1)
        unit_price = row.get("unit_price", 0)
        try:
            quantity = int(quantity)
            unit_price = float(unit_price)
        except (ValueError, TypeError):
            quantity = 1
            unit_price = 0

        gross_amount = round(quantity * unit_price, 2)
        discount_amount = round(gross_amount - total, 2)

        channel = str(row.get("channel", "")).lower().strip()
        if channel not in ("online", "store", "phone", "marketplace"):
            channel = "other"

        status = str(row.get("status", "")).lower().strip()
        if status not in ("completed", "pending", "cancelled", "refunded"):
            status = "unknown"

        result.append(
            {
                "transaction_id": row.get("transaction_id"),
                "customer_name": row.get("customer_name"),
                "email": str(row.get("email", "")).lower().strip(),
                "product": row.get("product"),
                "category": row.get("category"),
                "quantity": quantity,
                "unit_price": unit_price,
                "discount_pct": row.get("discount_pct"),
                "gross_amount": gross_amount,
                "discount_amount": discount_amount,
                "total": total,
                "sale_date": row.get("sale_date"),
                "channel": channel,
                "region": row.get("region"),
                "status": status,
                "processed_at": datetime.datetime.utcnow().isoformat(),
            }
        )
    return result
