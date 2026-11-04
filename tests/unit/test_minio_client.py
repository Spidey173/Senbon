"""
Unit Tests: MinIO S3 Lakehouse Storage Client
=============================================
Tests bucket configuration, resilient file promotion, and local lakehouse fallback.
"""

import pytest
import pandas as pd
from pathlib import Path
from scripts.minio_client import LakehouseStorageClient, resilient_retry


def test_storage_client_initialization(tmp_path):
    client = LakehouseStorageClient(base_data_dir=str(tmp_path))
    assert client.buckets["raw"] == "retail-raw"
    assert client.buckets["bronze"] == "retail-bronze"
    assert (tmp_path / "bronze").exists()
    assert (tmp_path / "silver").exists()


def test_put_dataframe_local_lakehouse(tmp_path):
    client = LakehouseStorageClient(base_data_dir=str(tmp_path))
    test_df = pd.DataFrame([{"id": 1, "name": "Item A"}, {"id": 2, "name": "Item B"}])

    saved_path = client.put_dataframe_as_parquet(test_df, zone="bronze", object_name="test_items/items")
    assert Path(saved_path).exists()


def test_retry_decorator_succeeds_after_transient_failure():
    attempts = 0

    @resilient_retry(max_attempts=3, initial_delay=0.01)
    def flaky_network_call():
        nonlocal attempts
        attempts += 1
        if attempts < 2:
            raise ConnectionError("Transient network drop")
        return "SUCCESS"

    result = flaky_network_call()
    assert result == "SUCCESS"
    assert attempts == 2

