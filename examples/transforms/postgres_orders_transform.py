"""Transform: PostgreSQL Orders → CSV

Drops cancelled/refunded orders, computes margin estimate (30% of total_price),
converts timestamps to date-only strings, and adds a days_to_ship column
when shipped_date is available.
"""

import datetime


def transform(data: list[dict]) -> list[dict]:
    result = []
    for row in data:
        if row.get("status") in ("cancelled", "refunded"):
            continue

        order_date = row.get("order_date")
        shipped_date = row.get("shipped_date")

        if isinstance(order_date, str):
            try:
                order_date = datetime.datetime.fromisoformat(order_date)
            except (ValueError, TypeError):
                order_date = None

        days_to_ship = None
        if order_date and shipped_date:
            if isinstance(shipped_date, str):
                try:
                    shipped_date = datetime.datetime.fromisoformat(shipped_date)
                except (ValueError, TypeError):
                    shipped_date = None
            if shipped_date and order_date:
                days_to_ship = (shipped_date - order_date).days

        total_price = row.get("total_price", 0)
        if total_price is not None:
            total_price = round(float(total_price), 2)

        result.append(
            {
                "order_id": row.get("id"),
                "user_id": row.get("user_id"),
                "product_id": row.get("product_id"),
                "quantity": row.get("quantity"),
                "unit_price": row.get("unit_price"),
                "total_price": total_price,
                "estimated_margin": round(total_price * 0.3, 2) if total_price else 0,
                "status": row.get("status"),
                "order_date": order_date.strftime("%Y-%m-%d") if order_date else None,
                "shipped_date": shipped_date.strftime("%Y-%m-%d")
                if isinstance(shipped_date, datetime.datetime)
                else None,
                "region": row.get("region"),
                "days_to_ship": days_to_ship,
            }
        )
    return result
