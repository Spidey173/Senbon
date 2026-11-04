"""
Master Orchestration DAG: Senbon — Retail Intelligence & Analytics Engine
========================================================================
Coordinates the full Lakehouse Lifecycle:
Landing Ingestion -> Quality Gatekeeper -> Bronze Promotion -> DuckDB Silver -> dbt Gold Marts -> Observability
Uses TriggerDagRunOperator with blocking dependency cascades.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator

from airflow.plugins.alerting_plugin import on_failure_callback

default_args = {
    "owner": "data_platform_lead",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
    "on_failure_callback": on_failure_callback,
    "execution_timeout": timedelta(hours=1),
}

with DAG(
    dag_id="dag_retail_master_orchestrator",
    default_args=default_args,
    description="End-to-end master controller coordinating all Senbon Lakehouse pipelines with SLA tracking",
    schedule_interval="0 4 * * *",  # 4:00 AM UTC Daily Batch
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["senbon", "lakehouse", "master", "orchestration", "production"],
) as dag:

    start_pipeline = EmptyOperator(task_id="start_pipeline")

    trigger_landing = TriggerDagRunOperator(
        task_id="trigger_dag_01_landing",
        trigger_dag_id="dag_01_retail_landing_ingestion",
        wait_for_completion=True,
        poke_interval=10,
        reset_dag_run=True,
    )

    trigger_validation = TriggerDagRunOperator(
        task_id="trigger_dag_02_validation_and_bronze",
        trigger_dag_id="dag_02_retail_quality_validation",
        wait_for_completion=True,
        poke_interval=10,
        reset_dag_run=True,
    )

    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_dag_03_duckdb_silver",
        trigger_dag_id="dag_03_retail_duckdb_silver",
        wait_for_completion=True,
        poke_interval=10,
        reset_dag_run=True,
    )

    trigger_gold = TriggerDagRunOperator(
        task_id="trigger_dag_04_dbt_gold_warehouse",
        trigger_dag_id="dag_04_retail_dbt_gold_warehouse",
        wait_for_completion=True,
        poke_interval=10,
        reset_dag_run=True,
    )

    trigger_monitoring = TriggerDagRunOperator(
        task_id="trigger_dag_05_monitoring_and_alerts",
        trigger_dag_id="dag_05_retail_monitoring_and_alerts",
        wait_for_completion=True,
        poke_interval=10,
        reset_dag_run=True,
    )

    end_pipeline = EmptyOperator(task_id="end_pipeline")

    (
        start_pipeline
        >> trigger_landing
        >> trigger_validation
        >> trigger_silver
        >> trigger_gold
        >> trigger_monitoring
        >> end_pipeline
    )

