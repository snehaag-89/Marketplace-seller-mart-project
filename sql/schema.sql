-- Schema definition for Marketplace Seller Mart
-- Idempotent schema initialization

-- 1. Pipeline Metadata Tables
CREATE TABLE IF NOT EXISTS pipeline_batches (
    batch_id TEXT PRIMARY KEY,
    filename TEXT NOT NULL,
    status TEXT NOT NULL,
    error_details TEXT,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_rejections (
    rejection_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL,
    source_table TEXT NOT NULL,
    raw_record_json TEXT NOT NULL,
    rejection_reason TEXT NOT NULL,
    rejected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (batch_id) REFERENCES pipeline_batches(batch_id)
);

-- 2. Raw Layer Tables
-- All business columns are stored as TEXT.
CREATE TABLE IF NOT EXISTS raw_sellers (
    seller_id TEXT,
    seller_name TEXT,
    batch_id TEXT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_products (
    product_id TEXT,
    seller_id TEXT,
    product_name TEXT,
    batch_id TEXT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_orders (
    order_id TEXT,
    order_date TEXT,
    status TEXT,
    batch_id TEXT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_order_items (
    order_item_id TEXT,
    order_id TEXT,
    product_id TEXT,
    quantity TEXT,
    price TEXT,
    batch_id TEXT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_returns (
    return_id TEXT,
    order_id TEXT,
    return_date TEXT,
    batch_id TEXT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Staging Layer Tables
-- Cleaned, typed, and validated data.
CREATE TABLE IF NOT EXISTS stg_sellers (
    seller_id TEXT PRIMARY KEY,
    seller_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS stg_products (
    product_id TEXT PRIMARY KEY,
    seller_id TEXT NOT NULL,
    product_name TEXT,
    FOREIGN KEY (seller_id) REFERENCES stg_sellers(seller_id)
);

CREATE TABLE IF NOT EXISTS stg_orders (
    order_id TEXT PRIMARY KEY,
    order_date DATE NOT NULL,
    status TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS stg_order_items (
    order_item_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    price REAL NOT NULL,
    FOREIGN KEY (order_id) REFERENCES stg_orders(order_id),
    FOREIGN KEY (product_id) REFERENCES stg_products(product_id)
    -- Note on Single-Seller Constraint:
    -- SQLite foreign keys cannot easily enforce that all items in an order belong to the same seller.
    -- As per architecture docs, this is enforced via pipeline validation logic in Python,
    -- which diverts offending records to raw_rejections.
);

CREATE TABLE IF NOT EXISTS stg_returns (
    return_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    return_date DATE NOT NULL,
    FOREIGN KEY (order_id) REFERENCES stg_orders(order_id)
);

-- 4. Analytics Layer (Mart)
CREATE TABLE IF NOT EXISTS mart_seller_weekly_metrics (
    seller_id TEXT NOT NULL,
    reporting_week TEXT NOT NULL,
    sales_volume REAL,
    order_return_rate REAL,
    cancellation_rate REAL,
    ai_summary TEXT,
    PRIMARY KEY (seller_id, reporting_week),
    FOREIGN KEY (seller_id) REFERENCES stg_sellers(seller_id)
);
