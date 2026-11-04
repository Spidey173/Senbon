"""
Enterprise Data Quality Gatekeeper & Dead Letter Queue (DLQ) Engine
===================================================================
Applies schema assertions, nullability constraints, foreign key referential integrity,
and domain business rules. Automatically routes corrupted records to the DLQ quarantine
with attached forensic metadata while allowing clean data to stream forward.
"""

import os
import json
import uuid
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional

import pandas as pd
from config.logging_config import get_logger
from config.pipeline_constants import (
    AUDIT_COL_INGESTED_AT,
    AUDIT_COL_BATCH_ID,
    AUDIT_COL_SOURCE_FILE,
    AUDIT_COL_QUARANTINE_REASON,
    AUDIT_COL_QUARANTINE_AT,
    ERROR_CODE_NULL_PRIMARY_KEY,
    ERROR_CODE_DUPLICATE_RECORD,
    ERROR_CODE_INVALID_RANGE,
    ERROR_CODE_FOREIGN_KEY_VIOLATION,
    ERROR_CODE_BUSINESS_RULE_VIOLATION,
    ACCEPTED_VALUES,
)

logger = get_logger("data_quality_validator")


class DataQualityValidator:
    """
    Validates ingested raw retail datasets against strict enterprise quality suites.
    Implements the Circuit Breaker and Dead Letter Queue (DLQ) patterns.
    """

    def __init__(
        self,
        raw_dir: str = "data/raw",
        validated_dir: str = "data/validated",
        rejected_dir: str = "data/rejected",
        batch_id: Optional[str] = None,
    ):
        self.raw_dir = Path(raw_dir)
        self.validated_dir = Path(validated_dir)
        self.rejected_dir = Path(rejected_dir)
        self.batch_id = batch_id or f"BATCH_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"

        self.validated_dir.mkdir(parents=True, exist_ok=True)
        self.rejected_dir.mkdir(parents=True, exist_ok=True)

        self.profiling_stats: Dict[str, Any] = {}

    def run_all_validations(self) -> Dict[str, Any]:
        """Runs validation sequence respecting entity dependency ordering."""
        logger.info(f"Initiating Data Quality Gatekeeper for Batch [{self.batch_id}]")
        now_iso = datetime.now(timezone.utc).isoformat()

        # Step 1: Load and Validate Primary Reference Entities
        stores_df, stores_bad = self._validate_stores()
        employees_df, employees_bad = self._validate_employees(stores_df)
        suppliers_df, suppliers_bad = self._validate_suppliers()
        products_df, products_bad = self._validate_products()
        customers_df, customers_bad = self._validate_customers()

        # Step 2: Validate Dependent Inventory
        inventory_df, inventory_bad = self._validate_inventory(stores_df, products_df)

        # Step 3: Validate Transactional Entities (Orders, Items, Payments)
        orders_df, orders_bad = self._validate_orders(customers_df, stores_df, employees_df)
        order_items_df, order_items_bad = self._validate_order_items(orders_df, products_df)
        payments_df, payments_bad = self._validate_payments(orders_df)
        reviews_df, reviews_bad = self._validate_reviews(products_df, customers_df)

        datasets = {
            "stores": (stores_df, stores_bad),
            "employees": (employees_df, employees_bad),
            "suppliers": (suppliers_df, suppliers_bad),
            "products": (products_df, products_bad),
            "customers": (customers_df, customers_bad),
            "inventory": (inventory_df, inventory_bad),
            "orders": (orders_df, orders_bad),
            "order_items": (order_items_df, order_items_bad),
            "payments": (payments_df, payments_bad),
            "reviews": (reviews_df, reviews_bad),
        }

        total_records_processed = 0
        total_records_quarantined = 0

        for name, (clean_df, bad_df) in datasets.items():
            # Add Lakehouse Audit Columns to clean dataset
            if not clean_df.empty:
                clean_df[AUDIT_COL_INGESTED_AT] = now_iso
                clean_df[AUDIT_COL_BATCH_ID] = self.batch_id
                clean_df[AUDIT_COL_SOURCE_FILE] = f"{name}.csv"
                clean_path = self.validated_dir / f"{name}_clean.csv"
                clean_df.to_csv(clean_path, index=False)

            # Route bad records to Dead Letter Queue (DLQ)
            if not bad_df.empty:
                bad_df[AUDIT_COL_QUARANTINE_AT] = now_iso
                bad_df[AUDIT_COL_BATCH_ID] = self.batch_id
                bad_path = self.rejected_dir / f"{name}_rejected.csv"
                bad_df.to_csv(bad_path, index=False)

            total_records_processed += len(clean_df) + len(bad_df)
            total_records_quarantined += len(bad_df)

            pass_rate = round((len(clean_df) / (len(clean_df) + len(bad_df)) * 100), 2) if (len(clean_df) + len(bad_df)) > 0 else 100.0
            self.profiling_stats[name] = {
                "total_rows": len(clean_df) + len(bad_df),
                "clean_rows": len(clean_df),
                "quarantined_rows": len(bad_df),
                "pass_rate_pct": pass_rate,
            }
            logger.info(f"Dataset [{name:<12}] Clean: {len(clean_df):<5} | DLQ Quarantined: {len(bad_df):<3} | Pass Rate: {pass_rate:.1f}%")

        report = {
            "batch_id": self.batch_id,
            "timestamp": now_iso,
            "total_processed": total_records_processed,
            "total_quarantined": total_records_quarantined,
            "overall_pass_rate_pct": round(((total_records_processed - total_records_quarantined) / total_records_processed * 100), 2) if total_records_processed > 0 else 100.0,
            "dataset_breakdown": self.profiling_stats,
        }

        # Step 4: Execute Great Expectations Suites
        ge_results = self.execute_expectation_suites({
            "customers": customers_df,
            "orders": orders_df,
            "products": products_df
        })
        report["great_expectations_validation"] = ge_results

        # Circuit breaker: fail if pass rate falls below acceptable threshold
        if report["overall_pass_rate_pct"] < 85.0:
            raise ValueError(
                f"CIRCUIT BREAKER TRIPPED: Overall batch pass rate {report['overall_pass_rate_pct']:.2f}% "
                f"fell below required 85.0% threshold. Quarantined {total_records_quarantined} records to DLQ."
            )

        # Persist data profiling manifesto
        with open(self.validated_dir / "dq_profiling_report.json", "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)

        logger.info(f"Data Quality Gatekeeper completed. Total Quarantined in DLQ: {total_records_quarantined}")
        return report

    def execute_expectation_suites(self, clean_datasets: Optional[Dict[str, pd.DataFrame]] = None) -> Dict[str, Any]:
        """Loads and executes Great Expectations suites against clean data assets."""
        suites_dir = Path("expectations/suites")
        suite_results = {}
        if not suites_dir.exists():
            return suite_results

        if clean_datasets is None:
            clean_datasets = {}
            for f in self.validated_dir.glob("*_clean.csv"):
                dataset_name = f.stem.replace("_clean", "")
                clean_datasets[dataset_name] = pd.read_csv(f)

        for dataset_name, df in clean_datasets.items():
            suite_file = suites_dir / f"retail_{dataset_name}_suite.json"
            if not suite_file.exists() or df.empty:
                continue

            try:
                with open(suite_file, "r", encoding="utf-8") as f:
                    suite_def = json.load(f)

                expectations = suite_def.get("expectations", [])
                passed = 0
                failed = 0
                details = []

                for exp in expectations:
                    exp_type = exp.get("expectation_type")
                    kwargs = exp.get("kwargs", {})
                    col = kwargs.get("column")
                    exp_success = True

                    if exp_type == "expect_column_values_to_not_be_null" and col in df.columns:
                        exp_success = bool(df[col].notna().all())
                    elif exp_type == "expect_column_values_to_be_unique" and col in df.columns:
                        exp_success = bool(df[col].is_unique)
                    elif exp_type == "expect_table_columns_to_match_ordered_list":
                        exp_success = all(c in df.columns for c in kwargs.get("column_list", []))
                    elif exp_type == "expect_column_values_to_be_between" and col in df.columns:
                        col_vals = pd.to_numeric(df[col], errors="coerce")
                        min_v = kwargs.get("min_value", -float("inf"))
                        max_v = kwargs.get("max_value", float("inf"))
                        exp_success = bool(((col_vals >= min_v) & (col_vals <= max_v)).all())

                    if exp_success:
                        passed += 1
                    else:
                        failed += 1
                    details.append({"expectation": exp_type, "column": col, "success": exp_success})

                suite_results[dataset_name] = {
                    "suite_name": suite_def.get("expectation_suite_name"),
                    "total_expectations": len(expectations),
                    "passed": passed,
                    "failed": failed,
                    "success": failed == 0,
                    "details": details
                }
            except Exception as e:
                logger.warning(f"Error evaluating GE suite for {dataset_name}: {e}")

        # Persist Great Expectations validation result report
        ge_report_path = self.validated_dir / "ge_validation_report.json"
        with open(ge_report_path, "w", encoding="utf-8") as f:
            json.dump(suite_results, f, indent=2)

        return suite_results

    # -------------------------------------------------------------------------
    # Dataset Specific Validation Rules (Vectorized Implementation)
    # -------------------------------------------------------------------------

    def _validate_customers(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        df = self._read_raw("customers.csv")
        if df.empty:
            return pd.DataFrame(), pd.DataFrame()

        c_id = df["customer_id"].astype(str).str.strip()
        email = df["email"].astype(str).str.strip()

        null_id_mask = (c_id == "") | (c_id == "nan") | df["customer_id"].isna()
        bad_email_mask = ~email.str.contains(r"^[^@]+@[^@]+\.[^@]+$", regex=True, na=True)
        bad_segment_mask = ~df["customer_segment"].isin(ACCEPTED_VALUES["customer_segment"])

        reasons = pd.Series("", index=df.index)
        reasons = reasons.mask(null_id_mask, reasons + f"{ERROR_CODE_NULL_PRIMARY_KEY}: customer_id is null/empty; ")
        reasons = reasons.mask(bad_email_mask, reasons + f"{ERROR_CODE_BUSINESS_RULE_VIOLATION}: invalid email format; ")
        reasons = reasons.mask(bad_segment_mask, reasons + f"{ERROR_CODE_INVALID_RANGE}: unknown customer_segment; ")

        return self._split_clean_bad_vectorized(df, reasons.str.rstrip("; "))

    def _validate_products(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        df = self._read_raw("products.csv")
        if df.empty:
            return pd.DataFrame(), pd.DataFrame()

        p_id = df["product_id"].astype(str).str.strip()
        unit_price = pd.to_numeric(df["unit_price"], errors="coerce")
        cost_price = pd.to_numeric(df["cost_price"], errors="coerce")

        null_id_mask = (p_id == "") | (p_id == "nan") | df["product_id"].isna()
        dup_id_mask = df["product_id"].duplicated(keep="first") & ~null_id_mask
        bad_price_mask = unit_price.isna() | (unit_price <= 0)
        bad_cost_mask = cost_price.isna() | (cost_price <= 0)

        reasons = pd.Series("", index=df.index)
        reasons = reasons.mask(null_id_mask, reasons + f"{ERROR_CODE_NULL_PRIMARY_KEY}: product_id is null/empty; ")
        reasons = reasons.mask(dup_id_mask, reasons + f"{ERROR_CODE_DUPLICATE_RECORD}: duplicate product_id in batch; ")
        reasons = reasons.mask(bad_price_mask, reasons + f"{ERROR_CODE_INVALID_RANGE}: unit_price must be > 0; ")
        reasons = reasons.mask(bad_cost_mask, reasons + f"{ERROR_CODE_INVALID_RANGE}: cost_price must be > 0; ")

        return self._split_clean_bad_vectorized(df, reasons.str.rstrip("; "))

    def _validate_stores(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        df = self._read_raw("stores.csv")
        if df.empty:
            return pd.DataFrame(), pd.DataFrame()

        s_id = df["store_id"].astype(str).str.strip()
        null_id_mask = (s_id == "") | (s_id == "nan") | df["store_id"].isna()
        bad_type_mask = ~df["store_type"].isin(ACCEPTED_VALUES["store_type"])

        reasons = pd.Series("", index=df.index)
        reasons = reasons.mask(null_id_mask, reasons + f"{ERROR_CODE_NULL_PRIMARY_KEY}: store_id is empty; ")
        reasons = reasons.mask(bad_type_mask, reasons + f"{ERROR_CODE_INVALID_RANGE}: invalid store_type; ")

        return self._split_clean_bad_vectorized(df, reasons.str.rstrip("; "))

    def _validate_employees(self, stores_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        df = self._read_raw("employees.csv")
        if df.empty:
            return pd.DataFrame(), pd.DataFrame()

        e_id = df["employee_id"].astype(str).str.strip()
        rate = pd.to_numeric(df["hourly_rate"], errors="coerce")
        valid_stores = set(stores_df["store_id"]) if not stores_df.empty else set()

        null_id_mask = (e_id == "") | (e_id == "nan") | df["employee_id"].isna()
        bad_store_mask = ~df["store_id"].isin(valid_stores)
        bad_rate_mask = rate.isna() | (rate < 7.25)

        reasons = pd.Series("", index=df.index)
        reasons = reasons.mask(null_id_mask, reasons + f"{ERROR_CODE_NULL_PRIMARY_KEY}: employee_id is empty; ")
        reasons = reasons.mask(bad_store_mask, reasons + f"{ERROR_CODE_FOREIGN_KEY_VIOLATION}: unknown store_id; ")
        reasons = reasons.mask(bad_rate_mask, reasons + f"{ERROR_CODE_BUSINESS_RULE_VIOLATION}: hourly_rate below legal minimum wage; ")

        return self._split_clean_bad_vectorized(df, reasons.str.rstrip("; "))

    def _validate_suppliers(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        df = self._read_raw("suppliers.csv")
        if df.empty:
            return pd.DataFrame(), pd.DataFrame()

        sup_id = df["supplier_id"].astype(str).str.strip()
        null_id_mask = (sup_id == "") | (sup_id == "nan") | df["supplier_id"].isna()

        reasons = pd.Series("", index=df.index)
        reasons = reasons.mask(null_id_mask, reasons + f"{ERROR_CODE_NULL_PRIMARY_KEY}: supplier_id is empty; ")

        return self._split_clean_bad_vectorized(df, reasons.str.rstrip("; "))

    def _validate_inventory(self, stores_df: pd.DataFrame, products_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        df = self._read_raw("inventory.csv")
        if df.empty:
            return pd.DataFrame(), pd.DataFrame()

        valid_stores = set(stores_df["store_id"]) if not stores_df.empty else set()
        valid_prods = set(products_df["product_id"]) if not products_df.empty else set()
        qty = pd.to_numeric(df["quantity_on_hand"], errors="coerce")

        bad_store_mask = ~df["store_id"].isin(valid_stores)
        bad_prod_mask = ~df["product_id"].isin(valid_prods)
        bad_qty_mask = qty.isna() | (qty < 0)

        reasons = pd.Series("", index=df.index)
        reasons = reasons.mask(bad_store_mask, reasons + f"{ERROR_CODE_FOREIGN_KEY_VIOLATION}: unknown store_id; ")
        reasons = reasons.mask(bad_prod_mask, reasons + f"{ERROR_CODE_FOREIGN_KEY_VIOLATION}: unknown product_id; ")
        reasons = reasons.mask(bad_qty_mask, reasons + f"{ERROR_CODE_INVALID_RANGE}: quantity_on_hand cannot be negative; ")

        return self._split_clean_bad_vectorized(df, reasons.str.rstrip("; "))

    def _validate_orders(
        self, customers_df: pd.DataFrame, stores_df: pd.DataFrame, employees_df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        df = self._read_raw("orders.csv")
        if df.empty:
            return pd.DataFrame(), pd.DataFrame()

        valid_customers = set(customers_df["customer_id"]) if not customers_df.empty else set()
        valid_stores = set(stores_df["store_id"]) if not stores_df.empty else set()
        valid_employees = set(employees_df["employee_id"]) if not employees_df.empty else set()
        tot = pd.to_numeric(df["total_amount"], errors="coerce")
        o_id = df["order_id"].astype(str).str.strip()

        null_id_mask = (o_id == "") | (o_id == "nan") | df["order_id"].isna()
        bad_cust_mask = ~df["customer_id"].isin(valid_customers)
        bad_store_mask = ~df["store_id"].isin(valid_stores)
        bad_emp_mask = ~df["employee_id"].isin(valid_employees)
        bad_status_mask = ~df["order_status"].isin(ACCEPTED_VALUES["order_status"])
        bad_tot_mask = tot.isna() | (tot < 0)

        reasons = pd.Series("", index=df.index)
        reasons = reasons.mask(null_id_mask, reasons + f"{ERROR_CODE_NULL_PRIMARY_KEY}: order_id is empty; ")
        reasons = reasons.mask(bad_cust_mask, reasons + f"{ERROR_CODE_FOREIGN_KEY_VIOLATION}: customer_id does not exist; ")
        reasons = reasons.mask(bad_store_mask, reasons + f"{ERROR_CODE_FOREIGN_KEY_VIOLATION}: store_id does not exist; ")
        reasons = reasons.mask(bad_emp_mask, reasons + f"{ERROR_CODE_FOREIGN_KEY_VIOLATION}: employee_id does not exist; ")
        reasons = reasons.mask(bad_status_mask, reasons + f"{ERROR_CODE_INVALID_RANGE}: unknown order_status; ")
        reasons = reasons.mask(bad_tot_mask, reasons + f"{ERROR_CODE_BUSINESS_RULE_VIOLATION}: total_amount must be >= 0; ")

        return self._split_clean_bad_vectorized(df, reasons.str.rstrip("; "))

    def _validate_order_items(self, orders_df: pd.DataFrame, products_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        df = self._read_raw("order_items.csv")
        if df.empty:
            return pd.DataFrame(), pd.DataFrame()

        valid_orders = set(orders_df["order_id"]) if not orders_df.empty else set()
        valid_prods = set(products_df["product_id"]) if not products_df.empty else set()
        qty = pd.to_numeric(df["quantity"], errors="coerce")
        unit_price = pd.to_numeric(df["unit_price"], errors="coerce")

        bad_order_mask = ~df["order_id"].isin(valid_orders)
        bad_prod_mask = ~df["product_id"].isin(valid_prods)
        bad_qty_mask = qty.isna() | (qty <= 0)
        bad_price_mask = unit_price.isna() | (unit_price <= 0)

        reasons = pd.Series("", index=df.index)
        reasons = reasons.mask(bad_order_mask, reasons + f"{ERROR_CODE_FOREIGN_KEY_VIOLATION}: parent order_id missing or quarantined; ")
        reasons = reasons.mask(bad_prod_mask, reasons + f"{ERROR_CODE_FOREIGN_KEY_VIOLATION}: product_id missing or quarantined; ")
        reasons = reasons.mask(bad_qty_mask, reasons + f"{ERROR_CODE_INVALID_RANGE}: quantity must be > 0; ")
        reasons = reasons.mask(bad_price_mask, reasons + f"{ERROR_CODE_INVALID_RANGE}: unit_price must be > 0; ")

        return self._split_clean_bad_vectorized(df, reasons.str.rstrip("; "))

    def _validate_payments(self, orders_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        df = self._read_raw("payments.csv")
        if df.empty:
            return pd.DataFrame(), pd.DataFrame()

        valid_orders = set(orders_df["order_id"]) if not orders_df.empty else set()
        amt = pd.to_numeric(df["amount"], errors="coerce")

        bad_order_mask = ~df["order_id"].isin(valid_orders)
        bad_method_mask = ~df["payment_method"].isin(ACCEPTED_VALUES["payment_method"])
        bad_status_mask = ~df["payment_status"].isin(ACCEPTED_VALUES["payment_status"])
        bad_amt_mask = amt.isna() | (amt < 0)

        reasons = pd.Series("", index=df.index)
        reasons = reasons.mask(bad_order_mask, reasons + f"{ERROR_CODE_FOREIGN_KEY_VIOLATION}: order_id not found in valid orders; ")
        reasons = reasons.mask(bad_method_mask, reasons + f"{ERROR_CODE_INVALID_RANGE}: invalid payment_method; ")
        reasons = reasons.mask(bad_status_mask, reasons + f"{ERROR_CODE_INVALID_RANGE}: invalid payment_status; ")
        reasons = reasons.mask(bad_amt_mask, reasons + f"{ERROR_CODE_INVALID_RANGE}: payment amount cannot be negative; ")

        return self._split_clean_bad_vectorized(df, reasons.str.rstrip("; "))

    def _validate_reviews(self, products_df: pd.DataFrame, customers_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        df = self._read_raw("reviews.csv")
        if df.empty:
            return pd.DataFrame(), pd.DataFrame()

        valid_prods = set(products_df["product_id"]) if not products_df.empty else set()
        valid_custs = set(customers_df["customer_id"]) if not customers_df.empty else set()
        rating = pd.to_numeric(df["rating"], errors="coerce")

        bad_prod_mask = ~df["product_id"].isin(valid_prods)
        bad_cust_mask = ~df["customer_id"].isin(valid_custs)
        bad_rating_mask = rating.isna() | (~rating.isin([1, 2, 3, 4, 5]))

        reasons = pd.Series("", index=df.index)
        reasons = reasons.mask(bad_prod_mask, reasons + f"{ERROR_CODE_FOREIGN_KEY_VIOLATION}: product_id missing or invalid; ")
        reasons = reasons.mask(bad_cust_mask, reasons + f"{ERROR_CODE_FOREIGN_KEY_VIOLATION}: customer_id missing or invalid; ")
        reasons = reasons.mask(bad_rating_mask, reasons + f"{ERROR_CODE_INVALID_RANGE}: rating must be 1-5; ")

        return self._split_clean_bad_vectorized(df, reasons.str.rstrip("; "))

    def _split_clean_bad_vectorized(self, df: pd.DataFrame, reasons: pd.Series) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Vectorized partition of clean vs bad records without iterrows."""
        is_bad = reasons != ""
        clean_df = df[~is_bad].copy()
        bad_df = df[is_bad].copy()
        if not bad_df.empty:
            bad_df[AUDIT_COL_QUARANTINE_REASON] = reasons[is_bad].values
        return clean_df, bad_df

    # -------------------------------------------------------------------------
    # Helper Utilities
    # -------------------------------------------------------------------------

    def _read_raw(self, filename: str) -> pd.DataFrame:
        file_path = self.raw_dir / filename
        if not file_path.exists():
            logger.warning(f"Raw source file missing: {file_path}")
            return pd.DataFrame()
        return pd.read_csv(file_path, dtype=str)

    def _split_clean_bad(self, df: pd.DataFrame, bad_reasons: List[Optional[str]]) -> Tuple[pd.DataFrame, pd.DataFrame]:
        if df.empty:
            return pd.DataFrame(), pd.DataFrame()

        df_copy = df.copy()
        is_bad = [r is not None for r in bad_reasons]
        clean_df = df_copy[~pd.Series(is_bad)].copy()

        bad_df = df_copy[pd.Series(is_bad)].copy()
        if not bad_df.empty:
            bad_df[AUDIT_COL_QUARANTINE_REASON] = [r for r in bad_reasons if r is not None]

        return clean_df, bad_df


if __name__ == "__main__":
    validator = DataQualityValidator()
    validator.run_all_validations()

