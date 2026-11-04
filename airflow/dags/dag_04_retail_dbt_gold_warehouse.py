"""
DAG 04: Senbon — dbt Gold Marts & Warehouse Materialization
===========================================================
Schedule: Event-driven or after Silver transformation
Purpose: Executes dbt Core transformations: Staging, Intermediate, Snapshots (SCD Type 2),
         Star Schema Facts & Dimensions, and rigorous dbt schema tests in PostgreSQL.
"""

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from airflow.plugins.alerting_plugin import on_failure_callback

default_args = {
    "owner": "data_platform_team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "on_failure_callback": on_failure_callback,
    "execution_timeout": timedelta(minutes=25),
}

with DAG(
    dag_id="dag_04_retail_dbt_gold_warehouse",
    default_args=default_args,
    description="Loads PostgreSQL warehouse & materializes Kimball Star Schema via dbt Core",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lakehouse", "gold", "warehouse", "dbt", "scd2", "star_schema"],
) as dag:

    def load_warehouse_staging(**context):
        from scripts.postgres_warehouse_loader import WarehouseLoader
        loader = WarehouseLoader()
        counts = loader.load_entire_warehouse()
        context["ti"].xcom_push(key="loaded_counts", value=counts)
        return counts

    task_stage_warehouse = PythonOperator(
        task_id="load_silver_to_warehouse_staging",
        python_callable=load_warehouse_staging,
    )

    dbt_dir = os.getenv("DBT_PROJECT_DIR", "/opt/airflow/dbt")
    dbt_profiles = os.getenv("DBT_PROFILES_DIR", dbt_dir)

    task_dbt_deps = BashOperator(
        task_id="dbt_deps_check",
        bash_command=f"cd {dbt_dir} && dbt deps --project-dir {dbt_dir} --profiles-dir {dbt_profiles}",
    )

    task_dbt_snapshots = BashOperator(
        task_id="dbt_run_scd2_snapshots",
        bash_command=f"cd {dbt_dir} && dbt snapshot --project-dir {dbt_dir} --profiles-dir {dbt_profiles}",
    )

    task_dbt_run_marts = BashOperator(
        task_id="dbt_run_gold_marts",
        bash_command=f"cd {dbt_dir} && dbt run --project-dir {dbt_dir} --profiles-dir {dbt_profiles}",
    )

    task_dbt_test = BashOperator(
        task_id="dbt_test_assertions",
        bash_command=f"cd {dbt_dir} && dbt test --project-dir {dbt_dir} --profiles-dir {dbt_profiles}",
    )

    task_stage_warehouse >> task_dbt_deps >> task_dbt_snapshots >> task_dbt_run_marts >> task_dbt_test

