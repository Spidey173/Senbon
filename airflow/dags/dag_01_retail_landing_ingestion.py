"""
DAG 01: Senbon — Retail Landing & API Ingestion
===============================================
Schedule: Hourly / Daily Micro-batch
Purpose: Fetches external APIs (Daily Sales, Currency, Weather, Marketing)
         and ingests incoming CSV files into Landing Zone (Raw S3/MinIO).
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.plugins.alerting_plugin import on_failure_callback

default_args = {
    "owner": "data_platform_team",
    "depends_on_past": False,
    "email": ["data-eng@retail-corp.internal"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_callback,
    "execution_timeout": timedelta(minutes=15),
}

with DAG(
    dag_id="dag_01_retail_landing_ingestion",
    default_args=default_args,
    description="Ingests streaming REST APIs and raw operational CSV feeds into Landing Zone",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "landing", "ingestion", "retail"],
) as dag:

    def ingest_mock_datasets(**context):
        from scripts.generate_mock_data import generate_all_datasets
        counts = generate_all_datasets(output_dir="data/raw")
        context["ti"].xcom_push(key="raw_counts", value=counts)
        return counts

    def fetch_external_apis(**context):
        import json
        from pathlib import Path
        from scripts.mock_api_server import (
            get_daily_sales,
            get_currency_rates,
            get_store_weather,
            get_marketing_campaigns,
        )

        api_dir = Path("data/raw/api")
        api_dir.mkdir(parents=True, exist_ok=True)

        sales_records = [
            r.model_dump() if hasattr(r, "model_dump") else r.dict()
            for r in get_daily_sales(limit=100)
        ]
        with open(api_dir / "daily_sales.json", "w") as f:
            json.dump(sales_records, f, indent=2)

        rates_records = [
            r.model_dump() if hasattr(r, "model_dump") else r.dict()
            for r in get_currency_rates()
        ]
        with open(api_dir / "currency_rates.json", "w") as f:
            json.dump(rates_records, f, indent=2)

        weather_records = [
            r.model_dump() if hasattr(r, "model_dump") else r.dict()
            for r in get_store_weather()
        ]
        with open(api_dir / "store_weather.json", "w") as f:
            json.dump(weather_records, f, indent=2)

        marketing_records = [
            r.model_dump() if hasattr(r, "model_dump") else r.dict()
            for r in get_marketing_campaigns()
        ]
        with open(api_dir / "marketing_campaigns.json", "w") as f:
            json.dump(marketing_records, f, indent=2)

        summary = {
            "daily_sales": len(sales_records),
            "currency_rates": len(rates_records),
            "store_weather": len(weather_records),
            "marketing_campaigns": len(marketing_records),
        }
        context["ti"].xcom_push(key="api_counts", value=summary)
        return summary

    task_ingest_raw = PythonOperator(
        task_id="ingest_raw_csv_sources",
        python_callable=ingest_mock_datasets,
    )

    task_fetch_apis = PythonOperator(
        task_id="fetch_external_apis",
        python_callable=fetch_external_apis,
    )

    task_ingest_raw >> task_fetch_apis

