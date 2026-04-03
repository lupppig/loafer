#!/usr/bin/env python3
"""Generate sample CSV and Excel files for ETL demo pipelines."""

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

import openpyxl

OUTPUT_DIR = Path(__file__).parent.parent / "examples" / "data"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

FIRST_NAMES = [
    "Alice",
    "Bob",
    "Charlie",
    "Diana",
    "Eve",
    "Frank",
    "Grace",
    "Henry",
    "Iris",
    "Jack",
    "Karen",
    "Leo",
    "Mona",
    "Nate",
    "Olivia",
    "Paul",
    "Quinn",
    "Rita",
    "Sam",
    "Tina",
]
LAST_NAMES = [
    "Smith",
    "Johnson",
    "Williams",
    "Brown",
    "Jones",
    "Garcia",
    "Miller",
    "Davis",
    "Rodriguez",
    "Martinez",
    "Wilson",
    "Anderson",
    "Taylor",
    "Thomas",
    "Moore",
    "Jackson",
    "Martin",
    "Lee",
    "Thompson",
    "White",
]
COUNTRIES = ["US", "UK", "CA", "DE", "FR", "JP", "AU", "BR", "IN", "MX"]
CATEGORIES = [
    "Electronics",
    "Clothing",
    "Books",
    "Home",
    "Sports",
    "Food",
    "Toys",
    "Beauty",
    "Auto",
    "Garden",
]
REGIONS = ["North", "South", "East", "West", "Central"]
STATUSES = ["active", "active", "active", "inactive", "suspended", "pending"]
TIERS = ["free", "free", "basic", "premium", "enterprise"]
CHANNELS = ["online", "store", "phone", "marketplace"]


def generate_csv_sales(path: str, num_rows: int = 50000) -> None:
    """Generate a CSV file with sales transaction data."""
    print(f"Generating {num_rows:,} rows of sales data → {path}")
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "transaction_id",
                "customer_name",
                "email",
                "product",
                "category",
                "quantity",
                "unit_price",
                "discount_pct",
                "total",
                "sale_date",
                "channel",
                "region",
                "status",
            ]
        )
        for i in range(1, num_rows + 1):
            fn = FIRST_NAMES[i % 20]
            ln = LAST_NAMES[(i // 20) % 20]
            qty = random.randint(1, 10)
            unit_price = round(random.uniform(5, 500), 2)
            discount = round(random.uniform(0, 25), 2)
            total = round(qty * unit_price * (1 - discount / 100), 2)
            writer.writerow(
                [
                    f"TXN-{i:06d}",
                    f"{fn} {ln}",
                    f"{fn.lower()}.{ln.lower()}{i}@example.com",
                    f"Product-{i}",
                    CATEGORIES[i % 10],
                    qty,
                    unit_price,
                    discount,
                    total,
                    (datetime.now() - timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d"),
                    CHANNELS[i % 4],
                    REGIONS[i % 5],
                    random.choice(
                        ["completed", "completed", "completed", "pending", "cancelled", "refunded"]
                    ),
                ]
            )
    print(f"  Done: {path}")


def generate_excel_inventory(path: str, num_rows: int = 30000) -> None:
    """Generate an Excel file with product inventory data."""
    print(f"Generating {num_rows:,} rows of inventory data → {path}")
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Inventory"

    ws.append(
        [
            "product_id",
            "product_name",
            "category",
            "supplier",
            "cost_price",
            "retail_price",
            "stock_qty",
            "reorder_level",
            "warehouse",
            "last_restocked",
            "is_active",
            "weight_kg",
            "dimensions",
        ]
    )

    for i in range(1, num_rows + 1):
        cost = round(random.uniform(2, 200), 2)
        retail = round(cost * random.uniform(1.2, 3.0), 2)
        ws.append(
            [
                f"PRD-{i:06d}",
                f"Product-{i}",
                CATEGORIES[i % 10],
                f"Supplier-{(i % 100) + 1}",
                cost,
                retail,
                random.randint(0, 10000),
                random.randint(10, 500),
                f"Warehouse-{chr(65 + i % 5)}",
                (datetime.now() - timedelta(days=random.randint(0, 180))).strftime("%Y-%m-%d"),
                i % 8 != 0,
                round(random.uniform(0.1, 50), 2),
                f"{random.randint(1, 100)}x{random.randint(1, 100)}x{random.randint(1, 100)}cm",
            ]
        )

    wb.save(path)
    print(f"  Done: {path}")


if __name__ == "__main__":
    csv_path = OUTPUT_DIR / "sales_data.csv"
    xlsx_path = OUTPUT_DIR / "inventory_data.xlsx"
    generate_csv_sales(str(csv_path))
    generate_excel_inventory(str(xlsx_path))
    print("\nAll files generated successfully!")
