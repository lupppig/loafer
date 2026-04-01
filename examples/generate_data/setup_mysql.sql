-- MySQL Seed Script: ~100,000 records for ETL demo
-- Run: mysql -u root -p < bin/setup_mysql.sql
-- Or: mysql -u loafer -ploafer loafer_dev < bin/setup_mysql.sql

CREATE DATABASE IF NOT EXISTS loafer_source;
USE loafer_source;

DROP TABLE IF EXISTS sales;
DROP TABLE IF EXISTS customers;

CREATE TABLE customers (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    phone VARCHAR(50),
    city VARCHAR(100),
    state VARCHAR(50),
    country VARCHAR(50) DEFAULT 'US',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_vip BOOLEAN DEFAULT FALSE
);

CREATE TABLE sales (
    id INT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    amount DECIMAL(12, 2) NOT NULL,
    tax DECIMAL(12, 2) DEFAULT 0.00,
    discount DECIMAL(5, 2) DEFAULT 0.00,
    final_amount DECIMAL(12, 2) NOT NULL,
    sale_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    channel VARCHAR(50) DEFAULT 'online',
    FOREIGN KEY (customer_id) REFERENCES customers(id)
);

-- Seed customers using a stored procedure for bulk inserts
DELIMITER //
CREATE PROCEDURE seed_customers()
BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE first_names VARCHAR(500) DEFAULT 'John,Jane,Mike,Sarah,Tom,Emma,David,Lisa,James,Amy,Chris,Kate,Dan,Ann,Pete,Laura,Steve,Jill,Bob,Ruth';
    DECLARE last_names VARCHAR(500) DEFAULT 'Smith,Jones,Brown,Taylor,Wilson,Moore,Clark,Hall,Allen,Young,King,Wright,Scott,Green,Adams,Baker,Nelson,Hill,Campbell,Mitchell';
    DECLARE cities VARCHAR(500) DEFAULT 'New York,Los Angeles,Chicago,Houston,Phoenix,Philadelphia,San Antonio,San Diego,Dallas,San Jose';
    DECLARE states VARCHAR(500) DEFAULT 'NY,CA,IL,TX,AZ,PA,TX,CA,TX,CA';
    
    WHILE i <= 50000 DO
        INSERT INTO customers (name, email, phone, city, state, country, is_vip)
        VALUES (
            CONCAT(
                SUBSTRING_INDEX(SUBSTRING_INDEX(first_names, ',', (i % 20) + 1), ',', -1), ' ',
                SUBSTRING_INDEX(SUBSTRING_INDEX(last_names, ',', (i / 20 % 20) + 1), ',', -1)
            ),
            CONCAT(
                LOWER(SUBSTRING_INDEX(SUBSTRING_INDEX(first_names, ',', (i % 20) + 1), ',', -1)), '.',
                LOWER(SUBSTRING_INDEX(SUBSTRING_INDEX(last_names, ',', (i / 20 % 20) + 1), ',', -1)),
                i, '@example.com'
            ),
            CONCAT('+1-555-', LPAD(i % 10000, 4, '0')),
            SUBSTRING_INDEX(SUBSTRING_INDEX(cities, ',', (i % 10) + 1), ',', -1),
            SUBSTRING_INDEX(SUBSTRING_INDEX(states, ',', (i % 10) + 1), ',', -1),
            'US',
            (i % 5 = 0)
        );
        SET i = i + 1;
    END WHILE;
END //
DELIMITER ;

CALL seed_customers();
DROP PROCEDURE IF EXISTS seed_customers;

-- Seed sales using a stored procedure
DELIMITER //
CREATE PROCEDURE seed_sales()
BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE categories VARCHAR(200) DEFAULT 'Electronics,Clothing,Books,Home,Sports';
    DECLARE channels VARCHAR(100) DEFAULT 'online,store,phone,marketplace';
    
    WHILE i <= 50000 DO
        SET @amount = ROUND(RAND() * 495 + 5, 2);
        SET @tax = ROUND(@amount * 0.08, 2);
        SET @discount = ROUND(RAND() * 20, 2);
        
        INSERT INTO sales (customer_id, product_name, category, amount, tax, discount, final_amount, channel)
        VALUES (
            1 + (i % 50000),
            CONCAT('Product-', i),
            SUBSTRING_INDEX(SUBSTRING_INDEX(categories, ',', (i % 5) + 1), ',', -1),
            @amount,
            @tax,
            @discount,
            ROUND(@amount + @tax - @discount, 2),
            SUBSTRING_INDEX(SUBSTRING_INDEX(channels, ',', (i % 4) + 1), ',', -1)
        );
        SET i = i + 1;
    END WHILE;
END //
DELIMITER ;

CALL seed_sales();
DROP PROCEDURE IF EXISTS seed_sales;

-- Verify
SELECT 'customers' AS table_name, COUNT(*) AS row_count FROM customers
UNION ALL
SELECT 'sales', COUNT(*) FROM sales;
