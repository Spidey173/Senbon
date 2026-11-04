"""
Integration Test: Warehouse Star Schema Integrity & Kimball Compliance
======================================================================
Verifies dimensional model integrity:
- Primary/surrogate keys are unique and non-null
- Foreign key relationships hold across facts and dimensions
- SCD Type 2 tracking validity
- Zero negative amounts or corrupted data in facts
"""

import pytest
import sqlite3
import pandas as pd
from pathlib import Path


@pytest.fixture
def warehouse_db():
    db_path = Path("data/retail_dw_replica.db")
    if not db_path.exists():
        from scripts.postgres_warehouse_loader import WarehouseLoader
        loader = WarehouseLoader()
        loader.load_entire_warehouse()
    conn = sqlite3.connect(db_path)
    yield conn
    conn.close()


def test_fact_orders_referential_integrity(warehouse_db):
    cur = warehouse_db.cursor()
    # Check that date_key in fct_orders exists in dim_dates
    cur.execute("""
        SELECT COUNT(*)
        FROM fct_orders o
        LEFT JOIN dim_dates d ON o.date_key = d.date_key
        WHERE d.date_key IS NULL
    """)
    orphaned_dates = cur.fetchone()[0]
    assert orphaned_dates == 0, f"Found {orphaned_dates} orphaned date keys in fct_orders"


def test_fact_orders_non_negative_amounts(warehouse_db):
    cur = warehouse_db.cursor()
    cur.execute("SELECT COUNT(*) FROM fct_orders WHERE net_amount < 0")
    negative_counts = cur.fetchone()[0]
    assert negative_counts == 0, f"Found {negative_counts} negative orders in warehouse"


def test_fact_sales_profit_consistency(warehouse_db):
    cur = warehouse_db.cursor()
    # Check line_profit = line_total - line_cost (with floating point tolerance)
    cur.execute("""
        SELECT COUNT(*)
        FROM fct_sales
        WHERE ABS(line_profit - (line_total - line_cost)) > 0.05
    """)
    inconsistent = cur.fetchone()[0]
    assert inconsistent == 0, f"Found {inconsistent} inconsistent profit calculations in fct_sales"


def test_dim_customers_scd2_structure(warehouse_db):
    cur = warehouse_db.cursor()
    cur.execute("SELECT COUNT(*) FROM dim_customers WHERE is_current = 1")
    current_count = cur.fetchone()[0]
    assert current_count > 0, "No active customer records found in SCD Type 2 dimension"


def test_customer_foreign_key_integrity(warehouse_db):
    cur = warehouse_db.cursor()
    # fct_orders -> dim_customers
    cur.execute("""
        SELECT COUNT(*)
        FROM fct_orders o
        LEFT JOIN dim_customers c ON o.customer_key = c.customer_key
        WHERE c.customer_key IS NULL
    """)
    orphans = cur.fetchone()[0]
    assert orphans == 0, f"Found {orphans} orphaned customer_key in fct_orders"

    # fct_sales -> dim_customers
    cur.execute("""
        SELECT COUNT(*)
        FROM fct_sales s
        LEFT JOIN dim_customers c ON s.customer_key = c.customer_key
        WHERE c.customer_key IS NULL
    """)
    orphans = cur.fetchone()[0]
    assert orphans == 0, f"Found {orphans} orphaned customer_key in fct_sales"


def test_product_foreign_key_integrity(warehouse_db):
    cur = warehouse_db.cursor()
    # fct_sales -> dim_products
    cur.execute("""
        SELECT COUNT(*)
        FROM fct_sales s
        LEFT JOIN dim_products p ON s.product_key = p.product_key
        WHERE p.product_key IS NULL
    """)
    orphans = cur.fetchone()[0]
    assert orphans == 0, f"Found {orphans} orphaned product_key in fct_sales"

    # fct_inventory -> dim_products
    cur.execute("""
        SELECT COUNT(*)
        FROM fct_inventory i
        LEFT JOIN dim_products p ON i.product_key = p.product_key
        WHERE p.product_key IS NULL
    """)
    orphans = cur.fetchone()[0]
    assert orphans == 0, f"Found {orphans} orphaned product_key in fct_inventory"


def test_store_foreign_key_integrity(warehouse_db):
    cur = warehouse_db.cursor()
    for table_name in ["fct_orders", "fct_sales", "fct_inventory"]:
        cur.execute(f"""
            SELECT COUNT(*)
            FROM {table_name} t
            LEFT JOIN dim_stores s ON t.store_key = s.store_key
            WHERE s.store_key IS NULL
        """)
        orphans = cur.fetchone()[0]
        assert orphans == 0, f"Found {orphans} orphaned store_key in {table_name}"


def test_employee_foreign_key_integrity(warehouse_db):
    cur = warehouse_db.cursor()
    cur.execute("""
        SELECT COUNT(*)
        FROM fct_orders o
        LEFT JOIN dim_employees e ON o.employee_key = e.employee_key
        WHERE e.employee_key IS NULL
    """)
    orphans = cur.fetchone()[0]
    assert orphans == 0, f"Found {orphans} orphaned employee_key in fct_orders"


def test_supplier_dimension_keys(warehouse_db):
    cur = warehouse_db.cursor()
    cur.execute("SELECT COUNT(*) FROM dim_suppliers WHERE supplier_key IS NULL OR supplier_key = ''")
    null_keys = cur.fetchone()[0]
    assert null_keys == 0, f"Found {null_keys} invalid supplier_keys in dim_suppliers"


def test_date_foreign_key_integrity(warehouse_db):
    cur = warehouse_db.cursor()
    for table_name in ["fct_orders", "fct_sales", "fct_inventory"]:
        cur.execute(f"""
            SELECT COUNT(*)
            FROM {table_name} t
            LEFT JOIN dim_dates d ON t.date_key = d.date_key
            WHERE d.date_key IS NULL
        """)
        orphans = cur.fetchone()[0]
        assert orphans == 0, f"Found {orphans} orphaned date_key in {table_name}"


def test_scd2_date_interval_validity(warehouse_db):
    cur = warehouse_db.cursor()
    for scd_table in ["dim_customers", "dim_products"]:
        cur.execute(f"""
            SELECT COUNT(*)
            FROM {scd_table}
            WHERE valid_to IS NOT NULL AND valid_to < valid_from
        """)
        invalid_intervals = cur.fetchone()[0]
        assert invalid_intervals == 0, f"Found {invalid_intervals} invalid SCD2 intervals in {scd_table}"


