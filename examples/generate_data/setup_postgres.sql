-- PostgreSQL Seed Script: 10,000,000 records across 4 tables
-- Run: psql -U loafer -d loafer_dev -f bin/setup_postgres.sql

-- Drop existing tables if they exist
DROP TABLE IF EXISTS events CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS users CASCADE;

-- Table 1: users (~3,000,000 rows)
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    email TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    country TEXT,
    signup_date TIMESTAMP NOT NULL DEFAULT NOW(),
    last_login TIMESTAMP,
    age INT,
    tier TEXT DEFAULT 'free'
);

-- Table 2: products (~1,000,000 rows)
CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    category TEXT NOT NULL,
    price NUMERIC(12, 2) NOT NULL,
    stock INT NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    is_active BOOLEAN NOT NULL DEFAULT true,
    weight_kg NUMERIC(8, 2),
    supplier_id INT
);

-- Table 3: orders (~4,000,000 rows)
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INT NOT NULL REFERENCES users(id),
    product_id INT NOT NULL REFERENCES products(id),
    quantity INT NOT NULL DEFAULT 1,
    unit_price NUMERIC(12, 2) NOT NULL,
    total_price NUMERIC(12, 2) NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    order_date TIMESTAMP NOT NULL DEFAULT NOW(),
    shipped_date TIMESTAMP,
    region TEXT
);

-- Table 4: events (~2,000,000 rows)
CREATE TABLE events (
    id SERIAL PRIMARY KEY,
    user_id INT NOT NULL REFERENCES users(id),
    event_type TEXT NOT NULL,
    page TEXT,
    duration_seconds INT,
    device TEXT,
    browser TEXT,
    occurred_at TIMESTAMP NOT NULL DEFAULT NOW(),
    metadata JSONB
);

-- Seed users (3,000,000 rows)
INSERT INTO users (first_name, last_name, email, status, country, signup_date, last_login, age, tier)
SELECT
    (ARRAY['Alice', 'Bob', 'Charlie', 'Diana', 'Eve', 'Frank', 'Grace', 'Henry', 'Iris', 'Jack',
           'Karen', 'Leo', 'Mona', 'Nate', 'Olivia', 'Paul', 'Quinn', 'Rita', 'Sam', 'Tina'])[1 + (g % 20)],
    (ARRAY['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez',
           'Wilson', 'Anderson', 'Taylor', 'Thomas', 'Moore', 'Jackson', 'Martin', 'Lee', 'Thompson', 'White'])[1 + ((g / 20) % 20)],
    LOWER(
        (ARRAY['alice', 'bob', 'charlie', 'diana', 'eve', 'frank', 'grace', 'henry', 'iris', 'jack',
               'karen', 'leo', 'mona', 'nate', 'olivia', 'paul', 'quinn', 'rita', 'sam', 'tina'])[1 + (g % 20)]
        || '.' ||
        (ARRAY['smith', 'johnson', 'williams', 'brown', 'jones', 'garcia', 'miller', 'davis', 'rodriguez', 'martinez',
               'wilson', 'anderson', 'taylor', 'thomas', 'moore', 'jackson', 'martin', 'lee', 'thompson', 'white'])[1 + ((g / 20) % 20)]
        || g || '@example.com'
    ),
    (ARRAY['active', 'active', 'active', 'inactive', 'suspended', 'pending'])[1 + (g % 6)],
    (ARRAY['US', 'UK', 'CA', 'DE', 'FR', 'JP', 'AU', 'BR', 'IN', 'MX'])[1 + (g % 10)],
    NOW() - (RANDOM() * INTERVAL '730 days'),
    NOW() - (RANDOM() * INTERVAL '30 days'),
    18 + (g % 60),
    (ARRAY['free', 'free', 'basic', 'premium', 'enterprise'])[1 + (g % 5)]
FROM generate_series(1, 3000000) AS g;

-- Seed products (1,000,000 rows)
INSERT INTO products (name, category, price, stock, created_at, is_active, weight_kg, supplier_id)
SELECT
    'Product-' || g,
    (ARRAY['Electronics', 'Clothing', 'Books', 'Home', 'Sports', 'Food', 'Toys', 'Beauty', 'Auto', 'Garden'])[1 + (g % 10)],
    ROUND((RANDOM() * 999 + 1)::NUMERIC, 2),
    (RANDOM() * 10000)::INT,
    NOW() - (RANDOM() * INTERVAL '365 days'),
    (g % 10) != 0,
    ROUND((RANDOM() * 50 + 0.1)::NUMERIC, 2),
    1 + (g % 500)
FROM generate_series(1, 1000000) AS g;

-- Seed orders (4,000,000 rows)
INSERT INTO orders (user_id, product_id, quantity, unit_price, total_price, status, order_date, shipped_date, region)
SELECT
    1 + (g % 3000000),
    1 + (g % 1000000),
    1 + (g % 10),
    ROUND((RANDOM() * 500 + 5)::NUMERIC, 2),
    0,
    (ARRAY['pending', 'completed', 'completed', 'completed', 'cancelled', 'refunded'])[1 + (g % 6)],
    NOW() - (RANDOM() * INTERVAL '365 days'),
    CASE WHEN g % 3 != 0 THEN NOW() - (RANDOM() * INTERVAL '30 days') ELSE NULL END,
    (ARRAY['North', 'South', 'East', 'West', 'Central'])[1 + (g % 5)]
FROM generate_series(1, 4000000) AS g;

-- Update total_price = quantity * unit_price
UPDATE orders SET total_price = quantity * unit_price;

-- Seed events (2,000,000 rows)
INSERT INTO events (user_id, event_type, page, duration_seconds, device, browser, occurred_at, metadata)
SELECT
    1 + (g % 3000000),
    (ARRAY['page_view', 'click', 'scroll', 'form_submit', 'purchase', 'logout', 'login', 'search'])[1 + (g % 8)],
    (ARRAY['/home', '/products', '/cart', '/checkout', '/profile', '/settings', '/search', '/help'])[1 + (g % 8)],
    (RANDOM() * 600)::INT,
    (ARRAY['desktop', 'mobile', 'tablet'])[1 + (g % 3)],
    (ARRAY['Chrome', 'Firefox', 'Safari', 'Edge', 'Opera'])[1 + (g % 5)],
    NOW() - (RANDOM() * INTERVAL '90 days'),
    jsonb_build_object('session_id', 'sess_' || g, 'version', 1 + (g % 5))
FROM generate_series(1, 2000000) AS g;

-- Create indexes for performance
CREATE INDEX idx_orders_user_id ON orders(user_id);
CREATE INDEX idx_orders_status ON orders(status);
CREATE INDEX idx_orders_order_date ON orders(order_date);
CREATE INDEX idx_events_user_id ON events(user_id);
CREATE INDEX idx_events_event_type ON events(event_type);
CREATE INDEX idx_users_status ON users(status);
CREATE INDEX idx_users_country ON users(country);

-- Verify counts
SELECT 'users' AS table_name, COUNT(*) AS row_count FROM users
UNION ALL
SELECT 'products', COUNT(*) FROM products
UNION ALL
SELECT 'orders', COUNT(*) FROM orders
UNION ALL
SELECT 'events', COUNT(*) FROM events
UNION ALL
SELECT 'TOTAL', SUM(cnt) FROM (
    SELECT COUNT(*) AS cnt FROM users
    UNION ALL SELECT COUNT(*) FROM products
    UNION ALL SELECT COUNT(*) FROM orders
    UNION ALL SELECT COUNT(*) FROM events
) sub;
