-- =============================================================================
-- Senbon — Retail Intelligence & Analytics Engine - Star Schema DDL
-- =============================================================================

-- -----------------------------------------------------------------------------
-- DIMENSION: DimDate (Calendar & Fiscal Hierarchy)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.dim_dates (
    date_key INT PRIMARY KEY,
    calendar_date DATE NOT NULL UNIQUE,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    week_of_year INT NOT NULL,
    day_of_month INT NOT NULL,
    day_of_week INT NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    fiscal_year INT NOT NULL,
    fiscal_quarter INT NOT NULL
);

-- -----------------------------------------------------------------------------
-- DIMENSION: DimCustomer (SCD Type 2 Enabled)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.dim_customers (
    customer_key VARCHAR(64) PRIMARY KEY,     -- Hash surrogate key
    customer_id VARCHAR(32) NOT NULL,         -- Natural key
    full_name VARCHAR(120),
    email VARCHAR(120),
    phone VARCHAR(40),
    city VARCHAR(60),
    state VARCHAR(40),
    country VARCHAR(40),
    postal_code VARCHAR(20),
    customer_segment VARCHAR(40),
    valid_from TIMESTAMP WITH TIME ZONE NOT NULL,
    valid_to TIMESTAMP WITH TIME ZONE,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    lakehouse_updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- -----------------------------------------------------------------------------
-- DIMENSION: DimProduct (SCD Type 2 Enabled)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.dim_products (
    product_key VARCHAR(64) PRIMARY KEY,      -- Hash surrogate key
    product_id VARCHAR(32) NOT NULL,          -- Natural key
    product_name VARCHAR(150) NOT NULL,
    category VARCHAR(60) NOT NULL,
    subcategory VARCHAR(60) NOT NULL,
    unit_price NUMERIC(12, 2) NOT NULL,
    cost_price NUMERIC(12, 2) NOT NULL,
    gross_margin_usd NUMERIC(12, 2),
    sku VARCHAR(60) NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    valid_from TIMESTAMP WITH TIME ZONE NOT NULL,
    valid_to TIMESTAMP WITH TIME ZONE,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    lakehouse_updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- -----------------------------------------------------------------------------
-- DIMENSION: DimStore
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.dim_stores (
    store_key VARCHAR(64) PRIMARY KEY,
    store_id VARCHAR(32) NOT NULL UNIQUE,
    store_name VARCHAR(100) NOT NULL,
    store_type VARCHAR(40) NOT NULL,
    city VARCHAR(60) NOT NULL,
    state VARCHAR(40) NOT NULL,
    country VARCHAR(40) NOT NULL,
    square_feet INT NOT NULL,
    opened_date DATE NOT NULL,
    lakehouse_updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- -----------------------------------------------------------------------------
-- DIMENSION: DimEmployee
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.dim_employees (
    employee_key VARCHAR(64) PRIMARY KEY,
    employee_id VARCHAR(32) NOT NULL UNIQUE,
    store_id VARCHAR(32) NOT NULL,
    full_name VARCHAR(120) NOT NULL,
    role VARCHAR(60) NOT NULL,
    hourly_rate NUMERIC(10, 2) NOT NULL,
    annual_salary NUMERIC(12, 2) NOT NULL,
    hire_date DATE NOT NULL,
    lakehouse_updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- -----------------------------------------------------------------------------
-- DIMENSION: DimSupplier
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.dim_suppliers (
    supplier_key VARCHAR(64) PRIMARY KEY,
    supplier_id VARCHAR(32) NOT NULL UNIQUE,
    supplier_name VARCHAR(120) NOT NULL,
    contact_email VARCHAR(120),
    country VARCHAR(60),
    payment_terms VARCHAR(40),
    lakehouse_updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- -----------------------------------------------------------------------------
-- FACT: FactOrders (Order Header Grain)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.fct_orders (
    order_key VARCHAR(64) PRIMARY KEY,
    order_id VARCHAR(32) NOT NULL UNIQUE,
    date_key INT NOT NULL REFERENCES analytics.dim_dates(date_key),
    customer_key VARCHAR(64) NOT NULL REFERENCES analytics.dim_customers(customer_key),
    store_key VARCHAR(64) NOT NULL REFERENCES analytics.dim_stores(store_key),
    employee_key VARCHAR(64) NOT NULL REFERENCES analytics.dim_employees(employee_key),
    order_status VARCHAR(30) NOT NULL,
    total_amount NUMERIC(14, 2) NOT NULL,
    discount_amount NUMERIC(12, 2) DEFAULT 0.00,
    net_amount NUMERIC(14, 2) NOT NULL,
    is_late_arriving BOOLEAN DEFAULT FALSE,
    lakehouse_batch_id VARCHAR(64),
    lakehouse_created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- -----------------------------------------------------------------------------
-- FACT: FactSales (Line Item Sales Grain)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.fct_sales (
    sale_key VARCHAR(64) PRIMARY KEY,
    order_key VARCHAR(64) NOT NULL REFERENCES analytics.fct_orders(order_key),
    date_key INT NOT NULL REFERENCES analytics.dim_dates(date_key),
    customer_key VARCHAR(64) NOT NULL REFERENCES analytics.dim_customers(customer_key),
    store_key VARCHAR(64) NOT NULL REFERENCES analytics.dim_stores(store_key),
    product_key VARCHAR(64) NOT NULL REFERENCES analytics.dim_products(product_key),
    quantity INT NOT NULL,
    unit_price NUMERIC(12, 2) NOT NULL,
    line_total NUMERIC(14, 2) NOT NULL,
    line_cost NUMERIC(14, 2) NOT NULL,
    line_profit NUMERIC(14, 2) NOT NULL,
    profit_margin_pct NUMERIC(6, 2) NOT NULL,
    lakehouse_batch_id VARCHAR(64),
    lakehouse_created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- -----------------------------------------------------------------------------
-- FACT: FactInventory (Store x Product Daily Snapshot)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.fct_inventory (
    inventory_key VARCHAR(64) PRIMARY KEY,
    date_key INT NOT NULL REFERENCES analytics.dim_dates(date_key),
    store_key VARCHAR(64) NOT NULL REFERENCES analytics.dim_stores(store_key),
    product_key VARCHAR(64) NOT NULL REFERENCES analytics.dim_products(product_key),
    quantity_on_hand INT NOT NULL,
    reorder_level INT NOT NULL,
    stockout_risk BOOLEAN NOT NULL,
    inventory_value_usd NUMERIC(14, 2) NOT NULL,
    lakehouse_batch_id VARCHAR(64),
    snapshot_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- -----------------------------------------------------------------------------
-- PLATFORM OBSERVABILITY & MONITORING AUDIT TABLES
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS monitoring.pipeline_health_metrics (
    metric_id SERIAL PRIMARY KEY,
    batch_id VARCHAR(64) NOT NULL,
    step_name VARCHAR(80) NOT NULL,
    status VARCHAR(20) NOT NULL,     -- SUCCESS, WARNING, FAILED
    rows_in INT DEFAULT 0,
    rows_out INT DEFAULT 0,
    rows_quarantined INT DEFAULT 0,
    execution_time_sec NUMERIC(10, 3) NOT NULL,
    error_message TEXT,
    logged_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS monitoring.data_quality_metrics (
    test_id SERIAL PRIMARY KEY,
    batch_id VARCHAR(64) NOT NULL,
    dataset_name VARCHAR(80) NOT NULL,
    validation_suite VARCHAR(100) NOT NULL,
    tests_run INT NOT NULL,
    tests_passed INT NOT NULL,
    tests_failed INT NOT NULL,
    quarantine_count INT DEFAULT 0,
    pass_rate_pct NUMERIC(6, 2) NOT NULL,
    executed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

