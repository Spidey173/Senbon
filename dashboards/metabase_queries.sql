-- =============================================================================
-- Senbon — Retail Intelligence & Analytics Engine - Production BI Queries
-- Optimized for PostgreSQL Data Warehouse (Star Schema in 'analytics' schema)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. EXECUTIVE KPI OVERVIEW (Header Metric Cards)
-- -----------------------------------------------------------------------------
-- Card 1A: Gross Revenue & Net Margin (CTE formulation to eliminate 1-to-N join fan-out)
WITH order_stats AS (
    SELECT
        COUNT(DISTINCT order_key) AS total_completed_orders,
        SUM(net_amount) AS total_order_amount,
        ROUND(AVG(net_amount), 2) AS average_order_value_usd
    FROM analytics.fct_orders
    WHERE order_status = 'COMPLETED'
),
sales_stats AS (
    SELECT
        ROUND(SUM(s.line_total), 2) AS total_revenue_usd,
        ROUND(SUM(s.line_profit), 2) AS total_gross_profit_usd,
        ROUND((SUM(s.line_profit) / NULLIF(SUM(s.line_total), 0)) * 100, 2) AS gross_profit_margin_pct
    FROM analytics.fct_sales s
    JOIN analytics.fct_orders o ON s.order_key = o.order_key
    WHERE o.order_status = 'COMPLETED'
)
SELECT
    s.total_revenue_usd,
    s.total_gross_profit_usd,
    s.gross_profit_margin_pct,
    o.total_completed_orders,
    o.average_order_value_usd
FROM order_stats o
CROSS JOIN sales_stats s;

-- -----------------------------------------------------------------------------
-- 2. SALES TREND: Monthly Revenue, Profit & Margin Velocity
-- -----------------------------------------------------------------------------
SELECT
    d.year,
    d.month,
    d.month_name,
    ROUND(SUM(s.line_total), 2) AS monthly_revenue_usd,
    ROUND(SUM(s.line_profit), 2) AS monthly_profit_usd,
    ROUND((SUM(s.line_profit) / NULLIF(SUM(s.line_total), 0)) * 100, 2) AS profit_margin_pct,
    COUNT(DISTINCT s.order_key) AS order_volume
FROM analytics.fct_sales s
JOIN analytics.dim_dates d ON s.date_key = d.date_key
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;

-- -----------------------------------------------------------------------------
-- 3. TOP 10 BEST-SELLING PRODUCTS & CATEGORY PERFORMANCE
-- -----------------------------------------------------------------------------
SELECT
    p.category,
    p.subcategory,
    p.product_name,
    SUM(s.quantity) AS total_units_sold,
    ROUND(SUM(s.line_total), 2) AS total_sales_usd,
    ROUND(SUM(s.line_profit), 2) AS total_profit_usd,
    ROUND((SUM(s.line_profit) / NULLIF(SUM(s.line_total), 0)) * 100, 2) AS product_margin_pct
FROM analytics.fct_sales s
JOIN analytics.dim_products p ON s.product_key = p.product_key
GROUP BY p.category, p.subcategory, p.product_name
ORDER BY total_sales_usd DESC
LIMIT 10;

-- -----------------------------------------------------------------------------
-- 4. TOP 10 HIGH-VALUE CUSTOMERS (VIP LTV)
-- -----------------------------------------------------------------------------
SELECT
    c.customer_id,
    c.full_name,
    c.email,
    c.city,
    c.state,
    c.customer_segment,
    COUNT(DISTINCT o.order_id) AS total_orders,
    ROUND(SUM(o.net_amount), 2) AS total_spend_usd,
    ROUND(AVG(o.net_amount), 2) AS average_order_value_usd
FROM analytics.fct_orders o
JOIN analytics.dim_customers c ON o.customer_key = c.customer_key
WHERE o.order_status = 'COMPLETED'
GROUP BY c.customer_id, c.full_name, c.email, c.city, c.state, c.customer_segment
ORDER BY total_spend_usd DESC
LIMIT 10;

-- -----------------------------------------------------------------------------
-- 5. PHYSICAL STORE PERFORMANCE & REVENUE PER SQUARE FOOT
-- -----------------------------------------------------------------------------
SELECT
    st.store_id,
    st.store_name,
    st.city,
    st.state,
    st.store_type,
    st.square_feet,
    COUNT(DISTINCT o.order_id) AS order_count,
    ROUND(SUM(o.net_amount), 2) AS store_revenue_usd,
    ROUND(SUM(o.net_amount) / NULLIF(st.square_feet, 0), 2) AS revenue_per_sq_ft
FROM analytics.fct_orders o
JOIN analytics.dim_stores st ON o.store_key = st.store_key
WHERE o.order_status = 'COMPLETED'
GROUP BY st.store_id, st.store_name, st.city, st.state, st.store_type, st.square_feet
ORDER BY store_revenue_usd DESC;

-- -----------------------------------------------------------------------------
-- 6. INVENTORY HEALTH, STOCKOUT RISK & VALUATION
-- -----------------------------------------------------------------------------
SELECT
    st.store_name,
    p.category,
    p.product_name,
    i.quantity_on_hand,
    i.reorder_level,
    i.stockout_risk,
    i.inventory_value_usd
FROM analytics.fct_inventory i
JOIN analytics.dim_stores st ON i.store_key = st.store_key
JOIN analytics.dim_products p ON i.product_key = p.product_key
WHERE i.stockout_risk = TRUE
ORDER BY i.quantity_on_hand ASC;

-- -----------------------------------------------------------------------------
-- 7. SCD TYPE 2 AUDIT: CUSTOMER ATTRIBUTE HISTORY
-- -----------------------------------------------------------------------------
SELECT
    customer_id,
    full_name,
    city,
    state,
    customer_segment,
    valid_from,
    valid_to,
    is_current
FROM analytics.dim_customers
ORDER BY customer_id, valid_from DESC;

-- -----------------------------------------------------------------------------
-- 8. PIPELINE OBSERVABILITY & SLA HEALTH DASHBOARD
-- -----------------------------------------------------------------------------
-- Metric 8A: Batch Execution Latency and Row Volume Over Time
SELECT
    batch_id,
    step_name,
    status,
    rows_out,
    execution_time_sec,
    logged_at
FROM monitoring.pipeline_health_metrics
ORDER BY logged_at DESC
LIMIT 20;

-- Metric 8B: Data Quality Pass Rates & Quarantine Counts
SELECT
    dataset_name,
    ROUND(AVG(pass_rate_pct), 2) AS average_dq_pass_rate,
    SUM(quarantine_count) AS total_records_quarantined_dlq,
    COUNT(*) AS total_validation_runs
FROM monitoring.data_quality_metrics
GROUP BY dataset_name
ORDER BY average_dq_pass_rate ASC;

-- Metric 8C: Late-Arriving Event Volume Audit
SELECT
    d.calendar_date,
    COUNT(CASE WHEN o.is_late_arriving THEN 1 END) AS late_arriving_orders,
    COUNT(*) AS total_orders,
    ROUND((COUNT(CASE WHEN o.is_late_arriving THEN 1 END)::NUMERIC / COUNT(*)) * 100, 2) AS late_arriving_pct
FROM analytics.fct_orders o
JOIN analytics.dim_dates d ON o.date_key = d.date_key
GROUP BY d.calendar_date
ORDER BY d.calendar_date DESC;

