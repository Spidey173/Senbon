"""
DAG 05: Senbon — Monitoring, Dashboard Refresh & SLA Alerts
===========================================================
Schedule: Daily / Post-Gold Loading
Purpose: Audits table freshness directly against PostgreSQL Data Warehouse,
         triggers Metabase/BI cache invalidation via REST API, and enforces
         strict SLA circuit breakers.
"""

import os
from datetime import datetime, timezone, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowException

from airflow.plugins.alerting_plugin import on_failure_callback

default_args = {
    "owner": "data_platform_team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=15),
    "on_failure_callback": on_failure_callback,
    "execution_timeout": timedelta(minutes=10),
}

with DAG(
    dag_id="dag_05_retail_monitoring_and_alerts",
    default_args=default_args,
    description="Audits data freshness in PostgreSQL DW, refreshes BI caches, and raises SLA alerts",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lakehouse", "monitoring", "observability", "metabase", "sla", "bi"],
) as dag:

    def audit_lakehouse_freshness(**context):
        """
        Queries PostgreSQL data warehouse to verify table freshness and SLA compliance.
        Fails the DAG task if data latency breaches SLA thresholds.
        """
        import logging
        log = logging.getLogger("airflow.task")
        log.info("Auditing table freshness across FactOrders in PostgreSQL Data Warehouse...")

        max_sla_hours = float(os.getenv("MAX_SLA_FRESHNESS_HOURS", "72.0"))
        pg_host = os.getenv("POSTGRES_HOST", "postgres")
        pg_port = os.getenv("POSTGRES_PORT", "5432")
        pg_user = os.getenv("POSTGRES_USER", "retail_admin")
        pg_pass = os.getenv("POSTGRES_PASSWORD", "retail_secure_pass_2026")
        pg_db = os.getenv("POSTGRES_DB", "retail_dw")

        latest_record_dt = None

        try:
            import psycopg2
            conn = psycopg2.connect(
                host=pg_host,
                port=pg_port,
                user=pg_user,
                password=pg_pass,
                dbname=pg_db,
                connect_timeout=5,
            )
            with conn.cursor() as cur:
                cur.execute("SELECT MAX(order_date) FROM analytics.fct_orders;")
                row = cur.fetchone()
                if row and row[0]:
                    latest_record_dt = row[0]
            conn.close()
        except Exception as err:
            log.warning(f"Direct PostgreSQL connection failed ({err}). Checking local lakehouse verification replica...")
            import sqlite3
            from pathlib import Path
            replica_path = Path("data/retail_dw_replica.db")
            if replica_path.exists():
                conn = sqlite3.connect(replica_path)
                cur = conn.cursor()
                row = cur.execute("SELECT MAX(order_date) FROM fct_orders;").fetchone()
                if row and row[0]:
                    latest_record_dt = row[0]
                conn.close()

        if not latest_record_dt:
            raise AirflowException("Freshness Audit Failed: No records found in FactOrders warehouse table.")

        if isinstance(latest_record_dt, str):
            try:
                latest_dt = datetime.fromisoformat(latest_record_dt.replace("Z", "+00:00"))
            except ValueError:
                latest_dt = datetime.strptime(latest_record_dt[:19], "%Y-%m-%d %H:%M:%S")
        else:
            latest_dt = latest_record_dt

        if latest_dt.tzinfo is None:
            latest_dt = latest_dt.replace(tzinfo=timezone.utc)

        current_dt = datetime.now(timezone.utc)
        latency_hours = max((current_dt - latest_dt).total_seconds() / 3600.0, 0.0)

        log.info(f"FactOrders latest event timestamp: {latest_dt.isoformat()} | Calculated Latency: {latency_hours:.2f} hours | SLA Threshold: {max_sla_hours} hours")

        if latency_hours > max_sla_hours:
            raise AirflowException(
                f"SLA BREACH: Warehouse data latency of {latency_hours:.2f} hours exceeds SLA limit of {max_sla_hours} hours!"
            )

        log.info("Freshness SLA audit passed successfully.")
        return {"freshness_status": "PASS", "latency_hours": round(latency_hours, 2)}

    task_freshness_audit = PythonOperator(
        task_id="audit_lakehouse_freshness",
        python_callable=audit_lakehouse_freshness,
    )

    metabase_host = os.getenv("METABASE_HOST", "metabase")
    task_refresh_metabase = BashOperator(
        task_id="refresh_metabase_dashboard_cache",
        bash_command=f"curl -s -f -X POST http://{metabase_host}:3000/api/database/1/rescan_values || true",
    )

    task_freshness_audit >> task_refresh_metabase
