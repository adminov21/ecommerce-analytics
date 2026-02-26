CREATE DATABASE IF NOT EXISTS marts;

CREATE TABLE IF NOT EXISTS marts.sales_daily (
    date Date,
    orders_count UInt32,
    revenue Float64,
    avg_order_value Float64
) ENGINE = MergeTree()
ORDER BY date;

CREATE TABLE IF NOT EXISTS marts.product_category_metrics (
    category_name String,
    month Date,
    orders_count UInt32,
    revenue Float64,
    avg_review_score Float64
) ENGINE = MergeTree()
ORDER BY (month, category_name);

CREATE TABLE IF NOT EXISTS marts.customer_rfm (
    customer_id String,
    recency UInt32,
    frequency UInt32,
    monetary Float64,
    segment String
) ENGINE = MergeTree()
ORDER BY (segment, customer_id);