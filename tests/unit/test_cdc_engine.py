"""
Unit Tests: DuckDB Lakehouse Engine & CDC Logic
================================================
Verifies Change Data Capture (CDC) deduplication, late-arriving event stitching,
and columnar calculations.
"""

import pytest
import pandas as pd
from datetime import datetime, timezone, timedelta
from scripts.duckdb_lakehouse_engine import DuckDBLakehouseEngine


@pytest.fixture
def temp_engine_env(tmp_path):
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    bronze.mkdir()
    silver.mkdir()
    db_file = str(tmp_path / "test_lakehouse.duckdb")
    engine = DuckDBLakehouseEngine(db_path=db_file, bronze_dir=str(bronze), silver_dir=str(silver))
    return engine, bronze, silver


def test_cdc_deduplication_keeps_latest_record(temp_engine_env):
    engine, bronze, silver = temp_engine_env

    # Simulate 2 versions of the same customer (e.g., initial creation vs address change)
    cust_dir = bronze / "customers"
    cust_dir.mkdir()
    data = [
        {
            "customer_id": "CUST_001",
            "first_name": "Alice",
            "last_name": "Smith",
            "email": "alice@domain.com",
            "city": "Dallas",
            "state": "TX",
            "country": "USA",
            "postal_code": "75001",
            "customer_segment": "REGULAR",
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z",
        },
        {
            "customer_id": "CUST_001",
            "first_name": "Alice",
            "last_name": "Smith",
            "email": "alice@domain.com",
            "city": "Austin",  # Moved to Austin
            "state": "TX",
            "country": "USA",
            "postal_code": "78701",
            "customer_segment": "VIP",  # Promoted to VIP
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-02-15T12:00:00Z",  # Later updated_at
        },
    ]
    pd.DataFrame(data).to_csv(cust_dir / "customers.csv", index=False)

    count = engine._process_customers_silver()
    assert count == 1

    silver_csv = silver / "customers.csv"
    assert silver_csv.exists()
    df_silver = pd.read_csv(silver_csv)
    assert len(df_silver) == 1
    assert df_silver.iloc[0]["city"] == "Austin"
    assert df_silver.iloc[0]["customer_segment"] == "VIP"


def test_late_arriving_event_flagging(temp_engine_env):
    engine, bronze, silver = temp_engine_env

    orders_dir = bronze / "orders"
    orders_dir.mkdir()

    base_time = datetime(2026, 1, 1, tzinfo=timezone.utc)
    orders = [
        {
            "order_id": "ORD_NORMAL",
            "customer_id": "CUST_001",
            "store_id": "STR_001",
            "employee_id": "EMP_001",
            "order_status": "COMPLETED",
            "order_date": base_time.isoformat(),
            "total_amount": 100.0,
            "discount_amount": 0.0,
            "updated_at": (base_time + timedelta(hours=2)).isoformat(),  # 2 hours delta (Normal)
        },
        {
            "order_id": "ORD_LATE",
            "customer_id": "CUST_002",
            "store_id": "STR_001",
            "employee_id": "EMP_001",
            "order_status": "COMPLETED",
            "order_date": (base_time - timedelta(days=60)).isoformat(),  # 60 days old
            "total_amount": 250.0,
            "discount_amount": 0.0,
            "updated_at": base_time.isoformat(),  # Arrived recently
        },
    ]
    pd.DataFrame(orders).to_csv(orders_dir / "orders.csv", index=False)

    engine._process_orders_silver()
    df_silver = pd.read_csv(silver / "orders.csv")
    assert len(df_silver) == 2

    normal_row = df_silver[df_silver["order_id"] == "ORD_NORMAL"].iloc[0]
    late_row = df_silver[df_silver["order_id"] == "ORD_LATE"].iloc[0]

    assert normal_row["is_late_arriving"] == False
    assert late_row["is_late_arriving"] == True

