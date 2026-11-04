"""
DAG 03: Senbon — DuckDB Silver Transformation & CDC Reconciliation
==================================================================
Schedule: Event-driven or after Bronze Promotion
Purpose: Executes high-speed vectorized DuckDB transformations on MinIO/S3 Parquet:
         incremental loading, CDC deduplication, and late-arriving event stitching.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.plugins.alerting_plugin import on_failure_callback

default_args = {
    "owner": "data_platform_team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=20),
    "on_failure_callback": on_failure_callback,
    "execution_timeout": timedelta(minutes=15),
}

with DAG(
    dag_id="dag_03_retail_duckdb_silver",
    default_args=default_args,
    description="Vectorized DuckDB lakehouse processing for CDC, deduplication, and late-arriving events",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lakehouse", "silver", "duckdb", "cdc", "deduplication"],
) as dag:

    def execute_duckdb_silver(**context):
        from scripts.duckdb_lakehouse_engine import DuckDBLakehouseEngine
        engine = DuckDBLakehouseEngine()
        results = engine.run_silver_pipeline()
        context["ti"].xcom_push(key="silver_results", value=results)
        return results

    task_silver_processing = PythonOperator(
        task_id="run_duckdb_silver_transformations",
        python_callable=execute_duckdb_silver,
    )

