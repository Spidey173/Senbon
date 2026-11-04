"""
Pipeline Constants and Enterprise Schema Specifications
======================================================
Defines column specifications, data types, error taxonomy for Dead Letter Queues (DLQ),
and audit metadata constants.
"""

from typing import Dict, List, Any

# Audit Column Definitions (Standard across Netflix/Uber Lakehouses)
AUDIT_COL_INGESTED_AT = "_lakehouse_ingested_at"
AUDIT_COL_BATCH_ID = "_lakehouse_batch_id"
AUDIT_COL_SOURCE_FILE = "_lakehouse_source_file"
AUDIT_COL_CDC_OP = "_cdc_operation"  # INSERT, UPDATE, DELETE
AUDIT_COL_QUARANTINE_REASON = "_quarantine_reason"
AUDIT_COL_QUARANTINE_AT = "_quarantined_at"

# DLQ Error Taxonomy
ERROR_CODE_NULL_PRIMARY_KEY = "ERR_NULL_PRIMARY_KEY"
ERROR_CODE_DUPLICATE_RECORD = "ERR_DUPLICATE_RECORD"
ERROR_CODE_INVALID_SCHEMA = "ERR_INVALID_SCHEMA_TYPE"
ERROR_CODE_INVALID_RANGE = "ERR_OUT_OF_BOUNDS_VALUE"
ERROR_CODE_FOREIGN_KEY_VIOLATION = "ERR_ORPHAN_FOREIGN_KEY"
ERROR_CODE_CORRUPT_JSON = "ERR_CORRUPT_PAYLOAD"
ERROR_CODE_BUSINESS_RULE_VIOLATION = "ERR_BUSINESS_RULE_VIOLATION"

# Complete Schema Definitions for Raw Files
DATASET_SCHEMAS: Dict[str, Dict[str, str]] = {
    "customers": {
        "customer_id": "string",
        "first_name": "string",
        "last_name": "string",
        "email": "string",
        "phone": "string",
        "city": "string",
        "state": "string",
        "country": "string",
        "postal_code": "string",
        "customer_segment": "string",
        "created_at": "timestamp",
        "updated_at": "timestamp",
    },
    "products": {
        "product_id": "string",
        "product_name": "string",
        "category": "string",
        "subcategory": "string",
        "unit_price": "float64",
        "cost_price": "float64",
        "sku": "string",
        "is_active": "boolean",
        "created_at": "timestamp",
        "updated_at": "timestamp",
    },
    "stores": {
        "store_id": "string",
        "store_name": "string",
        "store_type": "string",
        "city": "string",
        "state": "string",
        "country": "string",
        "square_feet": "int64",
        "opened_date": "date",
    },
    "employees": {
        "employee_id": "string",
        "store_id": "string",
        "first_name": "string",
        "last_name": "string",
        "role": "string",
        "hourly_rate": "float64",
        "hire_date": "date",
    },
    "orders": {
        "order_id": "string",
        "customer_id": "string",
        "store_id": "string",
        "employee_id": "string",
        "order_status": "string",
        "order_date": "timestamp",
        "total_amount": "float64",
        "discount_amount": "float64",
        "updated_at": "timestamp",
    },
    "order_items": {
        "order_item_id": "string",
        "order_id": "string",
        "product_id": "string",
        "quantity": "int64",
        "unit_price": "float64",
        "total_line_amount": "float64",
    },
    "payments": {
        "payment_id": "string",
        "order_id": "string",
        "payment_method": "string",
        "payment_status": "string",
        "amount": "float64",
        "currency": "string",
        "payment_date": "timestamp",
    },
    "inventory": {
        "inventory_id": "string",
        "store_id": "string",
        "product_id": "string",
        "quantity_on_hand": "int64",
        "reorder_level": "int64",
        "last_restocked_at": "timestamp",
    },
    "suppliers": {
        "supplier_id": "string",
        "supplier_name": "string",
        "contact_email": "string",
        "country": "string",
        "payment_terms": "string",
    },
    "reviews": {
        "review_id": "string",
        "product_id": "string",
        "customer_id": "string",
        "rating": "int64",
        "review_text": "string",
        "review_date": "timestamp",
    },
}

# Accepted Domain Values for Categoricals
ACCEPTED_VALUES = {
    "order_status": ["COMPLETED", "PENDING", "CANCELLED", "REFUNDED", "SHIPPED"],
    "payment_method": ["CREDIT_CARD", "DEBIT_CARD", "PAYPAL", "APPLE_PAY", "CASH"],
    "payment_status": ["SUCCESS", "FAILED", "PENDING", "REVERSED"],
    "customer_segment": ["VIP", "REGULAR", "CHURNED", "NEW", "WHOLESALE"],
    "store_type": ["FLAGSHIP", "OUTLET", "EXPRESS", "ONLINE_FULFILLMENT"],
}

