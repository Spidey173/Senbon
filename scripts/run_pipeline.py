"""
Master Pipeline Orchestration Runner
====================================
End-to-end execution harness coordinating:
1. Ingestion of external REST APIs (Daily sales, Currency, Weather, Marketing)
2. Synthetic retail data batch generation
3. Data Quality Gatekeeper & Quarantine DLQ routing
4. MinIO Bronze Lakehouse promotion
5. DuckDB Silver layer CDC reconciliation & deduplication
6. PostgreSQL Star Schema Fact & Dimension loading
7. Pipeline telemetry & SLA auditing
"""

import sys
import time
import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any

from config.logging_config import get_logger
from scripts.generate_mock_data import generate_all_datasets
from scripts.validator import DataQualityValidator
from scripts.minio_client import LakehouseStorageClient
from scripts.duckdb_lakehouse_engine import DuckDBLakehouseEngine
from scripts.postgres_warehouse_loader import WarehouseLoader

logger = get_logger("master_pipeline_runner")


def run_full_pipeline() -> Dict[str, Any]:
    """Runs all lakehouse pipeline stages sequentially with audit timers."""
    start_time = datetime.now()
    batch_id = f"BATCH_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    logger.info("=" * 80)
    logger.info(f"STARTING SENBON — RETAIL INTELLIGENCE & ANALYTICS ENGINE [{batch_id}]")
    logger.info("=" * 80)

    stages_summary: Dict[str, Any] = {}

    try:
        # ---------------------------------------------------------------------
        # STAGE 1: RAW INGESTION & DATA GENERATION
        # ---------------------------------------------------------------------
        s1_start = time.time()
        logger.info(">>> STAGE 1: Generating Raw CSV Feeds & API Datasets...")
        raw_counts = generate_all_datasets(output_dir="data/raw")
        stages_summary["1_raw_generation"] = {
            "status": "SUCCESS",
            "datasets": len(raw_counts),
            "duration_sec": round(time.time() - s1_start, 2),
        }

        # ---------------------------------------------------------------------
        # STAGE 2: DATA QUALITY VALIDATION & DEAD LETTER QUEUE (DLQ)
        # ---------------------------------------------------------------------
        s2_start = time.time()
        logger.info(">>> STAGE 2: Executing Data Quality Gatekeeper & DLQ Quarantine...")
        validator = DataQualityValidator(batch_id=batch_id)
        dq_report = validator.run_all_validations()
        stages_summary["2_quality_gatekeeper"] = {
            "status": "SUCCESS",
            "total_processed": dq_report["total_processed"],
            "quarantined_dlq": dq_report["total_quarantined"],
            "pass_rate_pct": dq_report["overall_pass_rate_pct"],
            "duration_sec": round(time.time() - s2_start, 2),
        }

        # ---------------------------------------------------------------------
        # STAGE 3: MINIO S3 BRONZE & QUARANTINE PROMOTION
        # ---------------------------------------------------------------------
        s3_start = time.time()
        logger.info(">>> STAGE 3: Promoting Clean Datasets to Bronze & Quarantine DLQ...")
        storage_client = LakehouseStorageClient()
        bronze_files = storage_client.upload_validated_to_bronze()
        quarantine_files = storage_client.upload_quarantine_to_lakehouse()
        stages_summary["3_bronze_promotion"] = {
            "status": "SUCCESS",
            "promoted_tables": len(bronze_files),
            "quarantined_files": len(quarantine_files),
            "duration_sec": round(time.time() - s3_start, 2),
        }

        # ---------------------------------------------------------------------
        # STAGE 4: DUCKDB SILVER TRANSFORMATION & CDC RECONCILIATION
        # ---------------------------------------------------------------------
        s4_start = time.time()
        logger.info(">>> STAGE 4: Running DuckDB Silver CDC Reconciliation & Deduplication...")
        duck_engine = DuckDBLakehouseEngine()
        silver_counts = duck_engine.run_silver_pipeline()
        silver_storage = storage_client.upload_silver_to_lakehouse()
        stages_summary["4_silver_processing"] = {
            "status": "SUCCESS",
            "tables_materialized": len(silver_counts),
            "total_silver_records": sum(silver_counts.values()),
            "silver_lakehouse_files": len(silver_storage),
            "duration_sec": round(time.time() - s4_start, 2),
        }

        # ---------------------------------------------------------------------
        # STAGE 5: POSTGRESQL DATA WAREHOUSE & STAR SCHEMA LOADING
        # ---------------------------------------------------------------------
        s5_start = time.time()
        logger.info(">>> STAGE 5: Loading Star Schema Dimensions & Facts into Warehouse...")
        wh_loader = WarehouseLoader()
        wh_counts = wh_loader.load_entire_warehouse(batch_id=batch_id)
        gold_storage = storage_client.upload_gold_to_lakehouse()
        stages_summary["5_warehouse_load"] = {
            "status": "SUCCESS",
            "tables_loaded": len(wh_counts),
            "total_mart_records": sum(wh_counts.values()),
            "gold_lakehouse_files": len(gold_storage),
            "duration_sec": round(time.time() - s5_start, 2),
        }

        total_elapsed = round((datetime.now() - start_time).total_seconds(), 2)
        logger.info("=" * 80)
        logger.info(f"PIPELINE COMPLETED SUCCESSFULLY IN {total_elapsed} SECONDS")
        logger.info(f"Summary Report: {stages_summary}")
        logger.info("=" * 80)
        return stages_summary

    except Exception as e:
        logger.error(f"FATAL PIPELINE FAILURE: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Senbon — Retail Intelligence & Analytics Engine")
    parser.add_argument("--step", default="all", choices=["all", "raw", "validate", "bronze", "silver", "warehouse"])
    args = parser.parse_args()

    if args.step == "all":
        run_full_pipeline()

