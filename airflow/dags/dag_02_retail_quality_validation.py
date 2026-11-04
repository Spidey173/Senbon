"""
DAG 02: Senbon — Data Quality Validation & Bronze Promotion
===========================================================
Schedule: Event-driven or after Landing Ingestion
Purpose: Executes Great Expectations validations, null checks, referential integrity,
         routes bad records to Dead Letter Queue (DLQ), and promotes clean data to Bronze.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException

from airflow.plugins.alerting_plugin import on_failure_callback

default_args = {
    "owner": "data_platform_team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=20),
    "on_failure_callback": on_failure_callback,
    "execution_timeout": timedelta(minutes=20),
}

with DAG(
    dag_id="dag_02_retail_quality_validation",
    default_args=default_args,
    description="Validates raw feeds against Great Expectations & promotes to Bronze Parquet",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lakehouse", "quality", "bronze", "validation", "dlq"],
) as dag:

    def validate_quality_gatekeeper(**context):
        from scripts.validator import DataQualityValidator
        validator = DataQualityValidator()
        report = validator.run_all_validations()

        pass_rate = report.get("overall_pass_rate_pct", 0.0)
        quarantined = report.get("total_quarantined", 0)

        # Circuit Breaker: Fail pipeline if data quality pass rate is unacceptable (< 85%)
        if pass_rate < 85.0:
            raise AirflowException(
                f"CIRCUIT BREAKER TRIPPED: Overall pass rate is {pass_rate:.2f}%, below minimum 85.0% threshold. "
                f"Quarantined {quarantined} records to DLQ."
            )

        context["ti"].xcom_push(key="validation_report", value=report)
        return report

    def promote_to_bronze_lakehouse(**context):
        from scripts.minio_client import LakehouseStorageClient
        client = LakehouseStorageClient()
        promoted = client.upload_validated_to_bronze()
        context["ti"].xcom_push(key="promoted_bronze", value=promoted)
        return promoted

    task_validate = PythonOperator(
        task_id="run_data_quality_gatekeeper",
        python_callable=validate_quality_gatekeeper,
    )

    task_promote_bronze = PythonOperator(
        task_id="promote_clean_to_bronze_lakehouse",
        python_callable=promote_to_bronze_lakehouse,
    )

    task_validate >> task_promote_bronze

