-- SQL Transformations for Marketplace Seller Mart
-- Transforms cleaned data from the staging layer (stg_) to analytics marts (mart_)

-- 1. Seller-Weekly Metrics (mart_seller_weekly_metrics)
-- Pre-aggregates order items and deduplicates returns to prevent fan-out inflation.
-- Attributes all events to the ISO week (Monday-Sunday) of the original order_date (cohort reporting).
-- Handles zero denominators by producing NULL.
-- Note on Orders Without Items:
-- stg_orders does not carry seller_id. The single-seller invariant establishes that
-- seller attribution is derived exclusively from stg_order_items -> stg_products.
-- Staged orders lacking items cannot validly be attributed to any seller without
-- inventing attribution. Therefore, inner-joining to order_seller strictly bounds
-- seller metrics (sales volume, return rate, cancellation rate) to orders with valid
-- seller attribution, preserving the single-seller invariant and avoiding arbitrary attribution.

-- Prune stale seller-week rows where no attributable orders remain in that week
-- or the seller/week combination is no longer active.
WITH order_seller AS (
    SELECT 
        oi.order_id,
        p.seller_id
    FROM stg_order_items oi
    JOIN stg_products p ON oi.product_id = p.product_id
    GROUP BY oi.order_id
),
distinct_weeks AS (
    SELECT DISTINCT 
        strftime('%Y', date(o.order_date, '-3 days', 'weekday 4')) || '-W' || 
        printf('%02d', (strftime('%j', date(o.order_date, '-3 days', 'weekday 4')) - 1) / 7 + 1) AS reporting_week
    FROM stg_orders o
    JOIN order_seller os ON o.order_id = os.order_id
),
valid_seller_weeks AS (
    SELECT s.seller_id, w.reporting_week
    FROM stg_sellers s
    CROSS JOIN distinct_weeks w
)
DELETE FROM mart_seller_weekly_metrics
WHERE (seller_id, reporting_week) NOT IN (
    SELECT seller_id, reporting_week FROM valid_seller_weeks
);

-- Compute and upsert seller-weekly metrics for all active seller-weeks
INSERT INTO mart_seller_weekly_metrics (
    seller_id,
    reporting_week,
    sales_volume,
    order_return_rate,
    cancellation_rate
)
WITH order_seller AS (
    -- Single-seller invariant guarantees all items in an order belong to the same seller
    SELECT 
        oi.order_id,
        p.seller_id
    FROM stg_order_items oi
    JOIN stg_products p ON oi.product_id = p.product_id
    GROUP BY oi.order_id
),
distinct_weeks AS (
    SELECT DISTINCT 
        strftime('%Y', date(o.order_date, '-3 days', 'weekday 4')) || '-W' || 
        printf('%02d', (strftime('%j', date(o.order_date, '-3 days', 'weekday 4')) - 1) / 7 + 1) AS reporting_week
    FROM stg_orders o
    JOIN order_seller os ON o.order_id = os.order_id
),
seller_weeks AS (
    SELECT s.seller_id, w.reporting_week
    FROM stg_sellers s
    CROSS JOIN distinct_weeks w
),
order_sales AS (
    -- Pre-aggregate items to order level for eligible orders (SHIPPED or DELIVERED)
    SELECT 
        oi.order_id,
        SUM(oi.quantity * oi.price) AS eligible_sales
    FROM stg_order_items oi
    JOIN stg_orders o ON oi.order_id = o.order_id
    WHERE o.status IN ('SHIPPED', 'DELIVERED')
    GROUP BY oi.order_id
),
order_returns AS (
    -- Deduplicate return records to order level
    SELECT DISTINCT 
        order_id,
        1 AS has_return
    FROM stg_returns
),
order_summary AS (
    SELECT 
        o.order_id,
        os.seller_id,
        o.order_date,
        strftime('%Y', date(o.order_date, '-3 days', 'weekday 4')) || '-W' || 
        printf('%02d', (strftime('%j', date(o.order_date, '-3 days', 'weekday 4')) - 1) / 7 + 1) AS reporting_week,
        o.status,
        COALESCE(sales.eligible_sales, 0.0) AS sales_volume,
        CASE WHEN o.status IN ('SHIPPED', 'DELIVERED') THEN 1 ELSE 0 END AS is_eligible_order,
        CASE WHEN o.status IN ('SHIPPED', 'DELIVERED') AND ret.has_return = 1 THEN 1 ELSE 0 END AS is_returned_eligible_order,
        CASE WHEN o.status = 'CANCELLED' THEN 1 ELSE 0 END AS is_cancelled_order
    FROM stg_orders o
    JOIN order_seller os ON o.order_id = os.order_id
    LEFT JOIN order_sales sales ON o.order_id = sales.order_id
    LEFT JOIN order_returns ret ON o.order_id = ret.order_id
),
aggregated_orders AS (
    SELECT 
        seller_id,
        reporting_week,
        SUM(sales_volume) AS sales_volume,
        COUNT(order_id) AS total_orders,
        SUM(is_eligible_order) AS eligible_orders,
        SUM(is_returned_eligible_order) AS returned_eligible_orders,
        SUM(is_cancelled_order) AS cancelled_orders
    FROM order_summary
    GROUP BY seller_id, reporting_week
)
SELECT 
    sw.seller_id,
    sw.reporting_week,
    COALESCE(CAST(ao.sales_volume AS REAL), 0.0) AS sales_volume,
    CASE 
        WHEN ao.eligible_orders IS NULL OR ao.eligible_orders = 0 THEN NULL
        ELSE CAST(ao.returned_eligible_orders AS REAL) / ao.eligible_orders
    END AS order_return_rate,
    CASE 
        WHEN ao.total_orders IS NULL OR ao.total_orders = 0 THEN NULL
        ELSE CAST(ao.cancelled_orders AS REAL) / ao.total_orders
    END AS cancellation_rate
FROM seller_weeks sw
LEFT JOIN aggregated_orders ao 
    ON sw.seller_id = ao.seller_id AND sw.reporting_week = ao.reporting_week
ON CONFLICT(seller_id, reporting_week) DO UPDATE SET
    sales_volume = excluded.sales_volume,
    order_return_rate = excluded.order_return_rate,
    cancellation_rate = excluded.cancellation_rate;
