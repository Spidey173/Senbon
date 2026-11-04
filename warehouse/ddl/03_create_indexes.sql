-- =============================================================================
-- Senbon — Retail Intelligence & Analytics Engine - Index Architecture
-- =============================================================================

-- Fact Table Foreign Key & Aggregation Indexes
CREATE INDEX IF NOT EXISTS idx_fct_orders_date_key ON analytics.fct_orders(date_key);
CREATE INDEX IF NOT EXISTS idx_fct_orders_customer_key ON analytics.fct_orders(customer_key);
CREATE INDEX IF NOT EXISTS idx_fct_orders_store_key ON analytics.fct_orders(store_key);
CREATE INDEX IF NOT EXISTS idx_fct_orders_status ON analytics.fct_orders(order_status);
CREATE INDEX IF NOT EXISTS idx_fct_orders_composite_date_status ON analytics.fct_orders(date_key, order_status);

CREATE INDEX IF NOT EXISTS idx_fct_sales_order_key ON analytics.fct_sales(order_key);
CREATE INDEX IF NOT EXISTS idx_fct_sales_date_key ON analytics.fct_sales(date_key);
CREATE INDEX IF NOT EXISTS idx_fct_sales_product_key ON analytics.fct_sales(product_key);
CREATE INDEX IF NOT EXISTS idx_fct_sales_store_key ON analytics.fct_sales(store_key);
CREATE INDEX IF NOT EXISTS idx_fct_sales_customer_key ON analytics.fct_sales(customer_key);

CREATE INDEX IF NOT EXISTS idx_fct_inventory_date_prod ON analytics.fct_inventory(date_key, product_key);
CREATE INDEX IF NOT EXISTS idx_fct_inventory_stockout ON analytics.fct_inventory(stockout_risk);

-- SCD Type 2 Optimization Indexes (Fast Point-in-time and Current Lookup)
CREATE INDEX IF NOT EXISTS idx_dim_cust_current ON analytics.dim_customers(customer_id, is_current);
CREATE INDEX IF NOT EXISTS idx_dim_cust_validity ON analytics.dim_customers(customer_id, valid_from, valid_to);

CREATE INDEX IF NOT EXISTS idx_dim_prod_current ON analytics.dim_products(product_id, is_current);
CREATE INDEX IF NOT EXISTS idx_dim_prod_category ON analytics.dim_products(category, subcategory);

-- Platform Monitoring Observability Indexes
CREATE INDEX IF NOT EXISTS idx_pipeline_metrics_batch ON monitoring.pipeline_health_metrics(batch_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_metrics_time ON monitoring.pipeline_health_metrics(logged_at DESC);
CREATE INDEX IF NOT EXISTS idx_dq_metrics_batch ON monitoring.data_quality_metrics(batch_id);

