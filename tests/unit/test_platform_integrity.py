"""
Platform Integrity Test Suite
=============================
Verifies platform readiness across the data lakehouse:
1. Airflow DAG parsing, AST syntax verification, and task integrity
2. dbt project structure, configuration, and model compilation sanity
3. MinIO / S3 Lakehouse client promotions and parquet storage
4. Warehouse table load, surrogate key integrity, and conflict handling
"""

import ast
import os
import yaml
import pytest
from pathlib import Path
import pandas as pd
from scripts.minio_client import LakehouseStorageClient
from scripts.postgres_warehouse_loader import WarehouseLoader, generate_surrogate_key


# ==============================================================================
# 1. Airflow DAG Validation Tests
# ==============================================================================
def test_all_airflow_dags_parse_without_errors():
    """Ensure every Airflow DAG file parses via AST without SyntaxError."""
    dags_dir = Path("airflow/dags")
    dag_files = list(dags_dir.glob("*.py"))
    assert len(dag_files) >= 5, f"Expected at least 5 DAG files, found {len(dag_files)}"

    for dag_file in dag_files:
        content = dag_file.read_text(encoding="utf-8")
        parsed = ast.parse(content, filename=str(dag_file))
        assert parsed is not None, f"Failed to parse DAG file {dag_file.name}"


def test_airflow_dags_contain_valid_dag_definitions():
    """Verify DAG files define DAG objects or context managers."""
    dags_dir = Path("airflow/dags")
    expected_dags = [
        "dag_01_retail_landing_ingestion.py",
        "dag_02_retail_quality_validation.py",
        "dag_03_retail_duckdb_silver.py",
        "dag_04_retail_dbt_gold_warehouse.py",
        "dag_05_retail_monitoring_and_alerts.py",
    ]
    for filename in expected_dags:
        dag_path = dags_dir / filename
        assert dag_path.exists(), f"Missing expected DAG: {filename}"
        code = dag_path.read_text()
        assert "with DAG(" in code or "DAG(" in code, f"DAG definition missing in {filename}"
        assert "default_args" in code, f"default_args missing in {filename}"


# ==============================================================================
# 2. dbt Project & Model Validation Tests
# ==============================================================================
def test_dbt_project_configuration():
    """Ensure dbt_project.yml and profiles.yml are valid YAML configurations."""
    dbt_project_file = Path("dbt/dbt_project.yml")
    assert dbt_project_file.exists(), "dbt_project.yml not found"
    with open(dbt_project_file, "r") as f:
        project_config = yaml.safe_load(f)

    assert project_config["name"] == "retail_data_lakehouse"
    assert "models" in project_config

    profiles_file = Path("dbt/profiles.yml")
    assert profiles_file.exists(), "dbt/profiles.yml not found"
    with open(profiles_file, "r") as f:
        profiles_config = yaml.safe_load(f)
    assert "retail_data_lakehouse" in profiles_config


def test_dbt_snapshots_and_marts_exist():
    """Verify dbt SCD2 snapshots and core dimensional marts exist."""
    snapshots_dir = Path("dbt/snapshots")
    assert (snapshots_dir / "snap_customers_scd2.sql").exists()
    assert (snapshots_dir / "snap_products_scd2.sql").exists()

    marts_dir = Path("dbt/models/marts/core")
    required_marts = [
        "dim_customers.sql",
        "dim_products.sql",
        "dim_stores.sql",
        "dim_dates.sql",
        "fct_orders.sql",
        "fct_sales.sql",
        "fct_inventory.sql",
    ]
    for m in required_marts:
        mart_file = marts_dir / m
        assert mart_file.exists(), f"Required dbt mart model {m} is missing"


def test_dbt_point_in_time_join_implementation():
    """Verify fct_orders and fct_sales implement proper SCD2 point-in-time joins."""
    fct_orders_sql = (Path("dbt/models/marts/core/fct_orders.sql")).read_text()
    assert "valid_from" in fct_orders_sql
    assert "valid_to" in fct_orders_sql

    fct_sales_sql = (Path("dbt/models/marts/core/fct_sales.sql")).read_text()
    assert "valid_from" in fct_sales_sql
    assert "valid_to" in fct_sales_sql


# ==============================================================================
# 3. MinIO Storage Client Tests
# ==============================================================================
def test_minio_parquet_storage_roundtrip(tmp_path):
    """Verify dataframe can be persisted in lakehouse zones."""
    client = LakehouseStorageClient(base_data_dir=str(tmp_path))
    sample_df = pd.DataFrame({
        "order_id": [101, 102],
        "customer_id": [1, 2],
        "amount": [99.50, 149.00]
    })
    written_path = client.put_dataframe_as_parquet(sample_df, zone="bronze", object_name="orders/orders_batch_1")
    path_obj = Path(written_path)
    assert path_obj.exists()
    if path_obj.suffix == ".parquet":
        import duckdb
        read_df = duckdb.query(f"SELECT * FROM '{written_path}'").df()
    else:
        read_df = pd.read_csv(written_path)
    assert len(read_df) == 2
    assert list(read_df["order_id"]) == [101, 102]


# ==============================================================================
# 4. Warehouse Insertion & Surrogate Key Tests
# ==============================================================================
def test_deterministic_surrogate_key_generation():
    """Verify Kimball surrogate key hash reproducibility."""
    k1 = generate_surrogate_key("CUST001", "2026-01-01 00:00:00")
    k2 = generate_surrogate_key("CUST001", "2026-01-01 00:00:00")
    k3 = generate_surrogate_key("CUST001", "2026-06-01 00:00:00")

    assert k1 == k2, "Identical inputs must produce identical surrogate keys"
    assert k1 != k3, "Different SCD valid_from timestamps must produce different surrogate keys"


def test_warehouse_loader_table_population(tmp_path):
    """Verify WarehouseLoader successfully materializes star schema tables."""
    loader = WarehouseLoader()
    results = loader.load_entire_warehouse()

    assert results["dim_dates"] > 0
    assert results["dim_customers"] > 0
    assert results["dim_products"] > 0
    assert results["fct_orders"] > 0
    assert results["fct_sales"] > 0
    assert results["fct_inventory"] > 0


# ==============================================================================
# 5. DuckDB Engine & Great Expectations Tests
# ==============================================================================
def test_duckdb_lakehouse_engine_transformation():
    """Verify DuckDB Lakehouse Engine executes Silver CDC transformations."""
    from scripts.duckdb_lakehouse_engine import DuckDBLakehouseEngine
    engine = DuckDBLakehouseEngine()
    results = engine.run_silver_pipeline()
    assert results["customers"] > 0
    assert results["orders"] > 0
    assert results["products"] > 0


def test_great_expectations_suite_execution():
    """Verify Great Expectations runner evaluates suites and exports JSON audit report."""
    from scripts.validator import DataQualityValidator
    validator = DataQualityValidator()
    report = validator.execute_expectation_suites()
    assert len(report) >= 3
    assert all(res["failed"] == 0 for res in report.values())
    assert Path("data/validated/ge_validation_report.json").exists()


def test_minio_multizone_sync(tmp_path):
    """Verify MinIO client synchronizes Bronze, Silver, Gold, and Quarantine zones."""
    client = LakehouseStorageClient(base_data_dir=str(tmp_path))
    sync_report = client.sync_all_lakehouse_zones()
    assert "bronze" in sync_report
    assert "quarantine" in sync_report
    assert "silver" in sync_report
    assert "gold" in sync_report

