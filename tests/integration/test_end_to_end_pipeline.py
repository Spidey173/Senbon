"""
Integration Test: End-to-End Senbon Pipeline
============================================
Executes full data progression:
Raw Ingestion -> Quality Gatekeeper -> DLQ Isolation -> Bronze S3 -> DuckDB Silver -> Star Schema Warehouse
"""

import pytest
from pathlib import Path
from scripts.run_pipeline import run_full_pipeline


def test_full_pipeline_execution():
    summary = run_full_pipeline()

    assert "1_raw_generation" in summary
    assert "2_quality_gatekeeper" in summary
    assert "3_bronze_promotion" in summary
    assert "4_silver_processing" in summary
    assert "5_warehouse_load" in summary

    assert summary["1_raw_generation"]["status"] == "SUCCESS"
    assert summary["2_quality_gatekeeper"]["status"] == "SUCCESS"
    assert summary["2_quality_gatekeeper"]["quarantined_dlq"] > 0
    assert summary["4_silver_processing"]["total_silver_records"] > 0
    assert summary["5_warehouse_load"]["total_mart_records"] > 0

    # Verify Dead Letter Queue files exist on disk
    rej_dir = Path("data/rejected")
    rejected_files = list(rej_dir.glob("*_rejected.csv"))
    assert len(rejected_files) > 0

    # Verify Silver curated assets exist
    silver_dir = Path("data/silver")
    assert (silver_dir / "customers.csv").exists()
    assert (silver_dir / "orders.csv").exists()
    assert (silver_dir / "products.csv").exists()

