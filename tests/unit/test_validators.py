"""
Unit Tests: Data Quality Validator & Dead Letter Queue (DLQ)
============================================================
Verifies schema enforcement, nullability constraints, foreign key validation,
range checks, and Dead Letter Queue isolation.
"""

import pytest
import pandas as pd
from pathlib import Path
from scripts.validator import DataQualityValidator
from config.pipeline_constants import (
    ERROR_CODE_NULL_PRIMARY_KEY,
    ERROR_CODE_DUPLICATE_RECORD,
    ERROR_CODE_BUSINESS_RULE_VIOLATION,
    AUDIT_COL_QUARANTINE_REASON,
)


@pytest.fixture
def temp_lakehouse_dir(tmp_path):
    raw_dir = tmp_path / "raw"
    val_dir = tmp_path / "validated"
    rej_dir = tmp_path / "rejected"
    raw_dir.mkdir()
    val_dir.mkdir()
    rej_dir.mkdir()
    return raw_dir, val_dir, rej_dir


def test_customer_validation_quarantines_null_id(temp_lakehouse_dir):
    raw_dir, val_dir, rej_dir = temp_lakehouse_dir

    # Create test customers with 1 clean and 1 corrupt row (missing customer_id)
    cust_df = pd.DataFrame([
        {
            "customer_id": "CUST_001",
            "first_name": "Valid",
            "last_name": "User",
            "email": "user@domain.com",
            "phone": "555-0100",
            "city": "Austin",
            "state": "TX",
            "country": "USA",
            "postal_code": "78701",
            "customer_segment": "VIP",
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z",
        },
        {
            "customer_id": "",  # Corrupt
            "first_name": "Ghost",
            "last_name": "Record",
            "email": "bad_email",
            "phone": "000-0000",
            "city": "Unknown",
            "state": "XX",
            "country": "USA",
            "postal_code": "00000",
            "customer_segment": "REGULAR",
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z",
        },
    ])
    cust_df.to_csv(raw_dir / "customers.csv", index=False)

    validator = DataQualityValidator(raw_dir=str(raw_dir), validated_dir=str(val_dir), rejected_dir=str(rej_dir))
    clean_df, bad_df = validator._validate_customers()

    assert len(clean_df) == 1
    assert clean_df.iloc[0]["customer_id"] == "CUST_001"
    assert len(bad_df) == 1
    assert ERROR_CODE_NULL_PRIMARY_KEY in bad_df.iloc[0][AUDIT_COL_QUARANTINE_REASON]


def test_products_validation_quarantines_duplicate_and_negative_price(temp_lakehouse_dir):
    raw_dir, val_dir, rej_dir = temp_lakehouse_dir

    prod_df = pd.DataFrame([
        {
            "product_id": "PRD_001",
            "product_name": "Laptop",
            "category": "Electronics",
            "subcategory": "Computers",
            "unit_price": 999.99,
            "cost_price": 600.00,
            "sku": "SKU-001",
            "is_active": True,
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z",
        },
        {
            "product_id": "PRD_001",  # Duplicate product_id
            "product_name": "Laptop Copy",
            "category": "Electronics",
            "subcategory": "Computers",
            "unit_price": 999.99,
            "cost_price": 600.00,
            "sku": "SKU-001-COPY",
            "is_active": True,
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z",
        },
        {
            "product_id": "PRD_002",
            "product_name": "Broken Item",
            "category": "Home",
            "subcategory": "Kitchen",
            "unit_price": -10.00,  # Negative price
            "cost_price": 5.00,
            "sku": "SKU-002",
            "is_active": True,
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z",
        },
    ])
    prod_df.to_csv(raw_dir / "products.csv", index=False)

    validator = DataQualityValidator(raw_dir=str(raw_dir), validated_dir=str(val_dir), rejected_dir=str(rej_dir))
    clean_df, bad_df = validator._validate_products()

    assert len(clean_df) == 1
    assert clean_df.iloc[0]["product_id"] == "PRD_001"
    assert len(bad_df) == 2
    reasons = " ".join(bad_df[AUDIT_COL_QUARANTINE_REASON].tolist())
    assert ERROR_CODE_DUPLICATE_RECORD in reasons


def test_orders_validation_quarantines_negative_total(temp_lakehouse_dir):
    raw_dir, val_dir, rej_dir = temp_lakehouse_dir

    orders_df = pd.DataFrame([
        {
            "order_id": "ORD_OK_01",
            "customer_id": "CUST_001",
            "store_id": "STR_001",
            "employee_id": "EMP_001",
            "order_status": "COMPLETED",
            "order_date": "2026-01-01T00:00:00Z",
            "total_amount": 120.00,
            "discount_amount": 0.0,
            "updated_at": "2026-01-01T00:00:00Z",
        },
        {
            "order_id": "ORD_BAD_02",
            "customer_id": "CUST_001",
            "store_id": "STR_001",
            "employee_id": "EMP_001",
            "order_status": "COMPLETED",
            "order_date": "2026-01-01T00:00:00Z",
            "total_amount": -50.00,  # Negative amount
            "discount_amount": 0.0,
            "updated_at": "2026-01-01T00:00:00Z",
        },
    ])
    orders_df.to_csv(raw_dir / "orders.csv", index=False)

    valid_custs = pd.DataFrame([{"customer_id": "CUST_001"}])
    valid_stores = pd.DataFrame([{"store_id": "STR_001"}])
    valid_emps = pd.DataFrame([{"employee_id": "EMP_001"}])

    validator = DataQualityValidator(raw_dir=str(raw_dir), validated_dir=str(val_dir), rejected_dir=str(rej_dir))
    clean_df, bad_df = validator._validate_orders(valid_custs, valid_stores, valid_emps)

    assert len(clean_df) == 1
    assert len(bad_df) == 1
    assert ERROR_CODE_BUSINESS_RULE_VIOLATION in bad_df.iloc[0][AUDIT_COL_QUARANTINE_REASON]

