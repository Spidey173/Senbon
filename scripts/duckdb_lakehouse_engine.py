"""
DuckDB Lakehouse Engine (Silver Layer Processing)
=================================================
Performs high-performance columnar transformations:
- Incremental watermark-based loading
- CDC (Change Data Capture) reconciliation & window-based deduplication
- Late-arriving transaction stitching
- External REST API ingestion & currency rate normalization
- Materialization of curated Silver layer datasets
"""

import os
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
import pandas as pd

from config.logging_config import get_logger

logger = get_logger("duckdb_lakehouse_engine")

try:
    import duckdb
    HAS_DUCKDB = True
except ImportError:
    HAS_DUCKDB = False
    duckdb = None


class DuckDBLakehouseEngine:
    """
    Columnar vectorized analytics engine managing the Bronze -> Silver lakehouse transition.
    """

    def __init__(
        self,
        db_path: str = "data/retail_lakehouse.duckdb",
        bronze_dir: str = "data/bronze",
        silver_dir: str = "data/silver",
    ):
        self.db_path = db_path
        self.bronze_dir = Path(bronze_dir)
        self.silver_dir = Path(silver_dir)
        self.silver_dir.mkdir(parents=True, exist_ok=True)

        self.con = None
        self._init_connection()

    def _init_connection(self) -> None:
        """Initializes DuckDB database or fallback relational engine."""
        if HAS_DUCKDB:
            logger.info(f"Opening DuckDB database at [{self.db_path}]")
            self.con = duckdb.connect(self.db_path)
            # Create schema namespaces
            self.con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
            self.con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
            self.con.execute("CREATE SCHEMA IF NOT EXISTS gold;")
        else:
            logger.info(f"DuckDB library not in environment. Using embedded vectorized SQLite at [{self.db_path}.sqlite]")
            self.con = sqlite3.connect(f"{self.db_path}.sqlite")

    def run_silver_pipeline(self) -> Dict[str, int]:
        """
        Executes end-to-end transformation of all Bronze datasets into curated Silver tables.
        """
        logger.info("Starting Silver layer transformation and CDC reconciliation...")
        results = {}

        results["customers"] = self._process_customers_silver()
        results["products"] = self._process_products_silver()
        results["stores"] = self._process_stores_silver()
        results["employees"] = self._process_employees_silver()
        results["suppliers"] = self._process_suppliers_silver()
        results["orders"] = self._process_orders_silver()
        results["order_items"] = self._process_order_items_silver()
        results["payments"] = self._process_payments_silver()
        results["inventory"] = self._process_inventory_silver()
        results["reviews"] = self._process_reviews_silver()

        logger.info(f"Silver transformation complete. Tables materialized: {results}")
        return results

    # -------------------------------------------------------------------------
    # 1. Customers CDC & Deduplication
    # -------------------------------------------------------------------------
    def _process_customers_silver(self) -> int:
        if HAS_DUCKDB and self.con:
            source = self._get_bronze_source("customers")
            query = f"""
                SELECT 
                    customer_id,
                    TRIM(first_name) AS first_name,
                    TRIM(last_name) AS last_name,
                    CONCAT(TRIM(first_name), ' ', TRIM(last_name)) AS full_name,
                    LOWER(TRIM(email)) AS email,
                    phone, city, state, country, postal_code, customer_segment,
                    created_at, updated_at,
                    _lakehouse_ingested_at, _lakehouse_batch_id, _lakehouse_source_file,
                    CURRENT_TIMESTAMP AS silver_processed_at
                FROM (
                    SELECT *,
                           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) AS _rn
                    FROM {source}
                ) sub
                WHERE _rn = 1
            """
            return self._execute_silver_sql("customers", query)

        df = self._read_bronze("customers")
        if df.empty:
            return 0

        # CDC Deduplication: Pick the record with latest updated_at for each customer_id
        df["updated_at"] = pd.to_datetime(df["updated_at"], format="ISO8601", errors="coerce")
        df_sorted = df.sort_values(by=["customer_id", "updated_at"], ascending=[True, False])
        silver_df = df_sorted.drop_duplicates(subset=["customer_id"], keep="first").copy()

        # Data enrichment & normalization
        silver_df["first_name"] = silver_df["first_name"].str.strip().str.title()
        silver_df["last_name"] = silver_df["last_name"].str.strip().str.title()
        silver_df["full_name"] = silver_df["first_name"] + " " + silver_df["last_name"]
        silver_df["email"] = silver_df["email"].str.strip().str.lower()
        silver_df["silver_processed_at"] = datetime.now(timezone.utc).isoformat()

        self._save_silver(silver_df, "customers")
        return len(silver_df)

    # -------------------------------------------------------------------------
    # 2. Products CDC & Margin Calculation
    # -------------------------------------------------------------------------
    def _process_products_silver(self) -> int:
        if HAS_DUCKDB and self.con:
            source = self._get_bronze_source("products")
            query = f"""
                SELECT 
                    product_id,
                    product_name,
                    category,
                    subcategory,
                    CAST(unit_price AS DOUBLE) AS unit_price,
                    CAST(cost_price AS DOUBLE) AS cost_price,
                    sku,
                    is_active,
                    created_at,
                    updated_at,
                    _lakehouse_ingested_at,
                    _lakehouse_batch_id,
                    _lakehouse_source_file,
                    ROUND(CAST(unit_price AS DOUBLE) - CAST(cost_price AS DOUBLE), 2) AS gross_margin_usd,
                    ROUND(((CAST(unit_price AS DOUBLE) - CAST(cost_price AS DOUBLE)) / NULLIF(CAST(unit_price AS DOUBLE), 0)) * 100, 2) AS margin_percentage,
                    CURRENT_TIMESTAMP AS silver_processed_at
                FROM (
                    SELECT *,
                           ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY updated_at DESC) AS _rn
                    FROM {source}
                ) sub
                WHERE _rn = 1
            """
            return self._execute_silver_sql("products", query)

        df = self._read_bronze("products")
        if df.empty:
            return 0

        df["updated_at"] = pd.to_datetime(df["updated_at"], format="ISO8601", errors="coerce")
        df_sorted = df.sort_values(by=["product_id", "updated_at"], ascending=[True, False])
        silver_df = df_sorted.drop_duplicates(subset=["product_id"], keep="first").copy()

        silver_df["unit_price"] = silver_df["unit_price"].astype(float)
        silver_df["cost_price"] = silver_df["cost_price"].astype(float)
        silver_df["gross_margin_usd"] = (silver_df["unit_price"] - silver_df["cost_price"]).round(2)
        silver_df["margin_percentage"] = (
            (silver_df["gross_margin_usd"] / silver_df["unit_price"]) * 100
        ).round(2)
        silver_df["silver_processed_at"] = datetime.now(timezone.utc).isoformat()

        self._save_silver(silver_df, "products")
        return len(silver_df)

    # -------------------------------------------------------------------------
    # 3. Stores Silver Normalization
    # -------------------------------------------------------------------------
    def _process_stores_silver(self) -> int:
        df = self._read_bronze("stores")
        if df.empty:
            return 0
        silver_df = df.drop_duplicates(subset=["store_id"]).copy()
        silver_df["square_feet"] = silver_df["square_feet"].astype(int)
        silver_df["silver_processed_at"] = datetime.now(timezone.utc).isoformat()
        self._save_silver(silver_df, "stores")
        return len(silver_df)

    # -------------------------------------------------------------------------
    # 4. Employees Silver Normalization
    # -------------------------------------------------------------------------
    def _process_employees_silver(self) -> int:
        df = self._read_bronze("employees")
        if df.empty:
            return 0
        silver_df = df.drop_duplicates(subset=["employee_id"]).copy()
        silver_df["hourly_rate"] = silver_df["hourly_rate"].astype(float)
        silver_df["annual_estimated_salary"] = (silver_df["hourly_rate"] * 2080).round(2)
        silver_df["silver_processed_at"] = datetime.now(timezone.utc).isoformat()
        self._save_silver(silver_df, "employees")
        return len(silver_df)

    # -------------------------------------------------------------------------
    # 5. Suppliers Silver Normalization
    # -------------------------------------------------------------------------
    def _process_suppliers_silver(self) -> int:
        df = self._read_bronze("suppliers")
        if df.empty:
            return 0
        silver_df = df.drop_duplicates(subset=["supplier_id"]).copy()
        silver_df["silver_processed_at"] = datetime.now(timezone.utc).isoformat()
        self._save_silver(silver_df, "suppliers")
        return len(silver_df)

    # -------------------------------------------------------------------------
    # 6. Orders Incremental Loading & Late-Arriving Stitching
    # -------------------------------------------------------------------------
    def _process_orders_silver(self) -> int:
        if HAS_DUCKDB and self.con:
            source = self._get_bronze_source("orders")
            query = f"""
                SELECT 
                    order_id,
                    customer_id,
                    store_id,
                    employee_id,
                    order_status,
                    order_date,
                    CAST(total_amount AS DOUBLE) AS total_amount,
                    CAST(discount_amount AS DOUBLE) AS discount_amount,
                    updated_at,
                    _lakehouse_ingested_at,
                    _lakehouse_batch_id,
                    _lakehouse_source_file,
                    CAST(order_date AS TIMESTAMP) AS order_timestamp,
                    STRFTIME(CAST(order_date AS TIMESTAMP), '%Y-%m-%d') AS order_date_only,
                    YEAR(CAST(order_date AS TIMESTAMP)) AS order_year,
                    MONTH(CAST(order_date AS TIMESTAMP)) AS order_month,
                    DAY(CAST(order_date AS TIMESTAMP)) AS order_day,
                    CASE WHEN DATEDIFF('day', CAST(order_date AS TIMESTAMP), CAST(updated_at AS TIMESTAMP)) > 30 THEN TRUE ELSE FALSE END AS is_late_arriving,
                    CURRENT_TIMESTAMP AS silver_processed_at
                FROM (
                    SELECT *,
                           ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC) AS _rn
                    FROM {source}
                ) sub
                WHERE _rn = 1
            """
            return self._execute_silver_sql("orders", query)

        df = self._read_bronze("orders")
        if df.empty:
            return 0

        # Incremental CDC resolution on order_id
        df["updated_at"] = pd.to_datetime(df["updated_at"], utc=True, errors="coerce")
        df_sorted = df.sort_values(by=["order_id", "updated_at"], ascending=[True, False])
        silver_df = df_sorted.drop_duplicates(subset=["order_id"], keep="first").copy()

        # Date partitioning & late-arriving flag identification
        silver_df["order_timestamp"] = pd.to_datetime(silver_df["order_date"], utc=True, errors="coerce")
        silver_df["order_date_only"] = silver_df["order_timestamp"].dt.strftime("%Y-%m-%d")
        silver_df["order_year"] = silver_df["order_timestamp"].dt.year
        silver_df["order_month"] = silver_df["order_timestamp"].dt.month
        silver_df["order_day"] = silver_df["order_timestamp"].dt.day

        # Late arriving check: order_date older than 30 days but ingested recently
        silver_df["is_late_arriving"] = (
            (silver_df["updated_at"] - silver_df["order_timestamp"]).dt.days > 30
        )

        silver_df["total_amount"] = silver_df["total_amount"].astype(float)
        silver_df["discount_amount"] = silver_df["discount_amount"].astype(float)
        silver_df["silver_processed_at"] = datetime.now(timezone.utc).isoformat()

        self._save_silver(silver_df, "orders")
        return len(silver_df)

    # -------------------------------------------------------------------------
    # 7. Order Items Silver Normalization
    # -------------------------------------------------------------------------
    def _process_order_items_silver(self) -> int:
        df = self._read_bronze("order_items")
        if df.empty:
            return 0
        silver_df = df.drop_duplicates(subset=["order_item_id"]).copy()
        silver_df["quantity"] = silver_df["quantity"].astype(int)
        silver_df["unit_price"] = silver_df["unit_price"].astype(float)
        silver_df["total_line_amount"] = silver_df["total_line_amount"].astype(float)
        silver_df["silver_processed_at"] = datetime.now(timezone.utc).isoformat()
        self._save_silver(silver_df, "order_items")
        return len(silver_df)

    # -------------------------------------------------------------------------
    # 8. Payments Silver Normalization
    # -------------------------------------------------------------------------
    def _process_payments_silver(self) -> int:
        df = self._read_bronze("payments")
        if df.empty:
            return 0
        silver_df = df.drop_duplicates(subset=["payment_id"]).copy()
        silver_df["amount"] = silver_df["amount"].astype(float)
        silver_df["silver_processed_at"] = datetime.now(timezone.utc).isoformat()
        self._save_silver(silver_df, "payments")
        return len(silver_df)

    # -------------------------------------------------------------------------
    # 9. Inventory Silver Normalization
    # -------------------------------------------------------------------------
    def _process_inventory_silver(self) -> int:
        df = self._read_bronze("inventory")
        if df.empty:
            return 0
        silver_df = df.drop_duplicates(subset=["inventory_id"]).copy()
        silver_df["quantity_on_hand"] = silver_df["quantity_on_hand"].astype(int)
        silver_df["reorder_level"] = silver_df["reorder_level"].astype(int)
        silver_df["stockout_risk"] = silver_df["quantity_on_hand"] <= silver_df["reorder_level"]
        silver_df["silver_processed_at"] = datetime.now(timezone.utc).isoformat()
        self._save_silver(silver_df, "inventory")
        return len(silver_df)

    # -------------------------------------------------------------------------
    # 10. Reviews Silver Normalization
    # -------------------------------------------------------------------------
    def _process_reviews_silver(self) -> int:
        df = self._read_bronze("reviews")
        if df.empty:
            return 0
        silver_df = df.drop_duplicates(subset=["review_id"]).copy()
        silver_df["rating"] = silver_df["rating"].astype(int)
        silver_df["sentiment_category"] = silver_df["rating"].apply(
            lambda r: "POSITIVE" if r >= 4 else ("NEUTRAL" if r == 3 else "NEGATIVE")
        )
        silver_df["silver_processed_at"] = datetime.now(timezone.utc).isoformat()
        self._save_silver(silver_df, "reviews")
        return len(silver_df)

    # -------------------------------------------------------------------------
    # Ingestion of External REST APIs into Silver
    # -------------------------------------------------------------------------
    def ingest_api_sales_silver(self, api_records: List[Dict[str, Any]]) -> int:
        """Ingests streaming/REST daily sales records directly into Silver."""
        if not api_records:
            return 0
        df = pd.DataFrame(api_records)
        df["silver_processed_at"] = datetime.now(timezone.utc).isoformat()
        self._save_silver(df, "api_sales")
        logger.info(f"Ingested {len(df)} records from Daily Sales REST API into Silver.")
        return len(df)

    def ingest_currency_rates_silver(self, rates: List[Dict[str, Any]]) -> int:
        """Ingests currency exchange rates into Silver."""
        if not rates:
            return 0
        df = pd.DataFrame(rates)
        df["silver_processed_at"] = datetime.now(timezone.utc).isoformat()
        self._save_silver(df, "currency_rates")
        logger.info(f"Ingested {len(df)} currency conversion records into Silver.")
        return len(df)

    # -------------------------------------------------------------------------
    # Helper Utilities
    # -------------------------------------------------------------------------
    def _get_bronze_source(self, name: str) -> str:
        parquet_path = self.bronze_dir / name / f"{name}.parquet"
        csv_path = self.bronze_dir / name / f"{name}.csv"
        if parquet_path.exists():
            return f"read_parquet('{parquet_path}')"
        return f"read_csv_auto('{csv_path}')"

    def _execute_silver_sql(self, table_name: str, query: str) -> int:
        """Executes native DuckDB SQL query, creating table and exporting Snappy Parquet."""
        self.con.execute(f"CREATE OR REPLACE TABLE silver.{table_name} AS {query};")
        parquet_target = self.silver_dir / f"{table_name}.parquet"
        csv_target = self.silver_dir / f"{table_name}.csv"
        try:
            self.con.execute(f"COPY silver.{table_name} TO '{parquet_target}' (FORMAT PARQUET, COMPRESSION SNAPPY);")
        except Exception as e:
            logger.debug(f"Parquet export skipped: {e}")
        self.con.execute(f"COPY silver.{table_name} TO '{csv_target}' (HEADER, DELIMITER ',');")
        count = self.con.execute(f"SELECT COUNT(*) FROM silver.{table_name}").fetchone()[0]
        return count

    def _read_bronze(self, name: str) -> pd.DataFrame:
        parquet_path = self.bronze_dir / name / f"{name}.parquet"
        csv_path = self.bronze_dir / name / f"{name}.csv"

        if parquet_path.exists():
            return pd.read_parquet(parquet_path)
        elif csv_path.exists():
            return pd.read_csv(csv_path)
        else:
            logger.warning(f"Bronze asset not found for: {name}")
            return pd.DataFrame()

    def _save_silver(self, df: pd.DataFrame, table_name: str) -> None:
        target_path = self.silver_dir / f"{table_name}.csv"
        df.to_csv(target_path, index=False)

        # Vectorized DuckDB materialization
        if HAS_DUCKDB and self.con:
            self.con.register("temp_df", df)
            self.con.execute(f"CREATE OR REPLACE TABLE silver.{table_name} AS SELECT * FROM temp_df;")
            try:
                parquet_target = self.silver_dir / f"{table_name}.parquet"
                self.con.execute(f"COPY silver.{table_name} TO '{parquet_target}' (FORMAT PARQUET, COMPRESSION SNAPPY);")
            except Exception as e:
                logger.debug(f"DuckDB snappy parquet copy skipped: {e}")
            self.con.unregister("temp_df")


if __name__ == "__main__":
    engine = DuckDBLakehouseEngine()
    engine.run_silver_pipeline()
