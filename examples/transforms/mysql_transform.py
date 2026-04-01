"""Transform: MySQL Sales → JSON

Normalizes email, computes tax_rate_pct, adds a price_tier based on final_amount,
standardizes channel names, and drops rows with missing customer info.
"""


def transform(data: list[dict]) -> list[dict]:
    result = []
    for row in data:
        customer_name = row.get("customer_name")
        if not customer_name or str(customer_name).strip() == "":
            continue

        amount = row.get("amount", 0)
        tax = row.get("tax", 0)
        discount = row.get("discount", 0)
        final = row.get("final_amount", 0)
        try:
            amount = float(amount) if amount is not None else 0
            tax = float(tax) if tax is not None else 0
            discount = float(discount) if discount is not None else 0
            final = float(final) if final is not None else 0
        except (ValueError, TypeError):
            amount = tax = discount = final = 0

        tax_rate_pct = round((tax / amount * 100), 2) if amount > 0 else 0

        if final >= 500:
            price_tier = "high"
        elif final >= 100:
            price_tier = "medium"
        else:
            price_tier = "low"

        channel = str(row.get("channel", "")).lower().strip()
        channel_map = {
            "online": "web",
            "store": "retail",
            "phone": "telesales",
            "marketplace": "marketplace",
        }

        sale_date = row.get("sale_date")
        if sale_date and hasattr(sale_date, "strftime"):
            sale_date = sale_date.strftime("%Y-%m-%d %H:%M:%S")

        result.append(
            {
                "sale_id": row.get("id"),
                "customer_id": row.get("customer_id"),
                "customer_name": str(customer_name).strip(),
                "email": str(row.get("email", "")).lower().strip(),
                "product_name": row.get("product_name"),
                "category": row.get("category"),
                "amount": amount,
                "tax": tax,
                "tax_rate_pct": tax_rate_pct,
                "discount": discount,
                "final_amount": final,
                "price_tier": price_tier,
                "sale_date": sale_date,
                "channel": channel_map.get(channel, channel),
            }
        )
    return result
