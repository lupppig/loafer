"""Transform: Excel Inventory → CSV

Flattens the dimensions string into separate length/width/height columns,
computes margin_pct, adds a needs_reorder flag, and filters out inactive products.
"""


def transform(data: list[dict]) -> list[dict]:
    result = []
    for row in data:
        is_active = row.get("is_active")
        if is_active is False or (
            isinstance(is_active, str) and is_active.lower() in ("false", "0", "no")
        ):
            continue

        cost = row.get("cost_price", 0)
        retail = row.get("retail_price", 0)
        try:
            cost = float(cost) if cost is not None else 0
            retail = float(retail) if retail is not None else 0
        except (ValueError, TypeError):
            cost = 0
            retail = 0

        margin_pct = round(((retail - cost) / retail * 100), 2) if retail > 0 else 0

        stock = row.get("stock_qty", 0)
        reorder = row.get("reorder_level", 0)
        try:
            stock = int(stock) if stock is not None else 0
            reorder = int(reorder) if reorder is not None else 0
        except (ValueError, TypeError):
            stock = 0
            reorder = 0

        needs_reorder = stock <= reorder

        dimensions = str(row.get("dimensions", ""))
        length = width = height = None
        parts = dimensions.replace("cm", "").strip().split("x")
        if len(parts) == 3:
            try:
                length = float(parts[0].strip())
                width = float(parts[1].strip())
                height = float(parts[2].strip())
            except (ValueError, TypeError):
                pass

        result.append(
            {
                "product_id": row.get("product_id"),
                "product_name": row.get("product_name"),
                "category": row.get("category"),
                "supplier": row.get("supplier"),
                "cost_price": cost,
                "retail_price": retail,
                "margin_pct": margin_pct,
                "stock_qty": stock,
                "reorder_level": reorder,
                "needs_reorder": needs_reorder,
                "warehouse": row.get("warehouse"),
                "last_restocked": row.get("last_restocked"),
                "weight_kg": row.get("weight_kg"),
                "length_cm": length,
                "width_cm": width,
                "height_cm": height,
            }
        )
    return result
