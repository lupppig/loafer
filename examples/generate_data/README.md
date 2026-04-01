# Loafer ETL Demo

Complete ETL demo with 10M PostgreSQL records, plus MySQL, MongoDB, CSV, and Excel sources.

## Quick Start

### 1. Start databases

```bash
docker compose -f docker/docker-compose.yml up -d postgres mongo
```

For MySQL, add this service to `docker/docker-compose.yml`:

```yaml
  mysql:
    image: mysql:8
    environment:
      MYSQL_ROOT_PASSWORD: root
      MYSQL_DATABASE: loafer_source
      MYSQL_USER: loafer
      MYSQL_PASSWORD: loafer
    ports:
      - "3306:3306"
    healthcheck:
      test: ["CMD", "mysqladmin", "ping", "-h", "localhost"]
      interval: 5s
      timeout: 3s
      retries: 5
```

Then:

```bash
docker compose -f docker/docker-compose.yml up -d mysql
```

### 2. Seed all databases and generate files

```bash
bash bin/setup_databases.sh
```

This creates:
- **PostgreSQL**: 10M records (3M users, 1M products, 4M orders, 2M events)
- **MySQL**: 100K records (50K customers, 50K sales)
- **MongoDB**: 50K documents (20K users, 30K analytics events)
- **CSV file**: 50K sales transactions
- **Excel file**: 30K inventory records

### 3. Run all pipelines

```bash
bash bin/run_all.sh
```

Or run individually:

```bash
uv run loafer run examples/pipelines/postgres_users_to_json.yaml
uv run loafer run examples/pipelines/postgres_orders_to_csv.yaml
uv run loafer run examples/pipelines/csv_to_postgres.yaml
uv run loafer run examples/pipelines/excel_to_csv.yaml
uv run loafer run examples/pipelines/mysql_to_json.yaml
uv run loafer run examples/pipelines/mongo_to_csv.yaml
```

## Pipelines

| # | Pipeline | Source | Target | Transform |
|---|----------|--------|--------|-----------|
| 1 | Users → JSON | PostgreSQL (users) | JSON | Custom Python: combine names, filter by status, add age_group |
| 2 | Orders → CSV | PostgreSQL (orders) | CSV | Custom Python: drop cancelled, compute margin, days_to_ship |
| 3 | CSV → PostgreSQL | CSV file | PostgreSQL | Custom Python: normalize fields, compute gross_amount, add processed_at |
| 4 | Excel → CSV | Excel file | CSV | Custom Python: flatten dimensions, compute margin_pct, needs_reorder flag |
| 5 | MySQL → JSON | MySQL (sales+customers) | JSON | Custom Python: price tiers, tax rate, channel mapping |
| 6 | MongoDB → CSV | MongoDB (analytics) | CSV | Custom Python: flatten nested docs, duration in minutes, session buckets |

## File Structure

```
bin/
├── setup_postgres.sql    # 10M record seed for PostgreSQL
├── setup_mysql.sql       # 100K record seed for MySQL
├── setup_mongo.js        # 50K document seed for MongoDB
├── generate_files.py     # Generates CSV and Excel source files
├── setup_databases.sh    # Runs all seeds + file generation
├── run_all.sh            # Runs all 6 pipelines
└── README.md             # This file

examples/
├── pipelines/
│   ├── postgres_users_to_json.yaml
│   ├── postgres_orders_to_csv.yaml
│   ├── csv_to_postgres.yaml
│   ├── excel_to_csv.yaml
│   ├── mysql_to_json.yaml
│   └── mongo_to_csv.yaml
├── transforms/
│   ├── postgres_users_transform.py
│   ├── postgres_orders_transform.py
│   ├── csv_transform.py
│   ├── excel_transform.py
│   ├── mysql_transform.py
│   └── mongo_transform.py
├── data/
│   ├── sales_data.csv        (generated, 50K rows)
│   └── inventory_data.xlsx   (generated, 30K rows)
└── output/                   (pipeline outputs)
```

## PostgreSQL Tables

| Table | Rows | Columns |
|-------|------|---------|
| users | 3,000,000 | id, first_name, last_name, email, status, country, signup_date, last_login, age, tier |
| products | 1,000,000 | id, name, category, price, stock, created_at, is_active, weight_kg, supplier_id |
| orders | 4,000,000 | id, user_id, product_id, quantity, unit_price, total_price, status, order_date, shipped_date, region |
| events | 2,000,000 | id, user_id, event_type, page, duration_seconds, device, browser, occurred_at, metadata |

## Notes

- Pipeline queries on PostgreSQL use `LIMIT 100000` to keep demo runs fast. Remove the LIMIT to process all 10M records.
- All transforms use **custom** mode (no LLM calls needed).
- Environment variables in pipeline configs (`${POSTGRES_URL}`) are resolved from `.env`.
