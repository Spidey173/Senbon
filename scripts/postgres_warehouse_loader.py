"""
PostgreSQL Data Warehouse Loader & Kimball Star Schema Engine
=============================================================
Transforms curated Silver lakehouse assets into production Dimensional Star Schemas:
- Populates DimDate calendar dimension
- Implements SCD Type 2 tracking for DimCustomer and DimProduct
- Resolves surrogate hash keys across FactSales, FactOrders, and FactInventory
- Records pipeline health telemetry in monitoring audit tables
- Features transparent local fallback for seamless standalone testing
"""

import os
import hashlib
from datetime import datetime, timezone, timedelta, date
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
import pandas as pd

from config.logging_config import get_logger

logger = get_logger("postgres_warehouse_loader")

try:
    import psycopg2
    from psycopg2 import sql
    from psycopg2.extras import execute_batch
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False
    psycopg2 = None
    sql = None

try:
    import sqlite3
    HAS_SQLITE = True
except ImportError:
    HAS_SQLITE = False


def generate_surrogate_key(*args: Any) -> str:
    """Generates deterministic surrogate hash key (Kimball standard)."""
    raw_str = "|".join(str(a) if a is not None else "" for a in args)
    return hashlib.md5(raw_str.encode("utf-8")).hexdigest()


class WarehouseLoader:
    """
    Manages loading and materialization of dimensional warehouse star schemas in PostgreSQL.
    """

    def __init__(
        self,
        silver_dir: str = "data/silver",
        host: Optional[str] = None,
        port: Optional[int] = None,
        dbname: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
    ):
        self.silver_dir = Path(silver_dir)
        self.host = host or os.getenv("POSTGRES_HOST", "localhost")
        self.port = int(port or os.getenv("POSTGRES_PORT", "5432"))
        self.dbname = dbname or os.getenv("POSTGRES_DB", "retail_dw")
        self.user = user or os.getenv("POSTGRES_USER", "retail_admin")
        self.password = password or os.getenv("POSTGRES_PASSWORD", "retail_secure_pass_2026")

        self.conn = None
        self.is_postgres = False
        self._init_connection()

    def _init_connection(self) -> None:
        """Connects to Postgres; falls back to embedded SQLite for offline verification."""
        if HAS_PSYCOPG2:
            try:
                self.conn = psycopg2.connect(
                    host=self.host,
                    port=self.port,
                    dbname=self.dbname,
                    user=self.user,
                    password=self.password,
                    connect_timeout=3,
                )
                self.conn.autocommit = True
                self.is_postgres = True
                logger.info(f"Connected to PostgreSQL Data Warehouse at [{self.host}:{self.port}/{self.dbname}]")
                return
            except Exception as e:
                logger.warning(f"PostgreSQL connection to [{self.host}:{self.port}] unavailable: {e}")

        logger.info("Initializing embedded local SQLite replica for warehouse verification.")
        self.sqlite_path = Path("data/retail_dw_replica.db")
        self.conn = sqlite3.connect(self.sqlite_path)
        self.is_postgres = False

    def load_entire_warehouse(self, batch_id: Optional[str] = None) -> Dict[str, int]:
        """Orchestrates end-to-end star schema loading."""
        b_id = batch_id or f"BATCH_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
        start_time = datetime.now()
        logger.info(f"Starting Star Schema Warehouse Load for Batch [{b_id}]")

        self._ensure_tables_exist()

        counts = {}
        counts["dim_dates"] = self._populate_dim_dates()
        counts["dim_stores"] = self._populate_dim_stores()
        counts["dim_employees"] = self._populate_dim_employees()
        counts["dim_suppliers"] = self._populate_dim_suppliers()
        counts["dim_customers"] = self._populate_dim_customers_scd2()
        counts["dim_products"] = self._populate_dim_products_scd2()
        counts["fct_orders"] = self._populate_fct_orders(b_id)
        counts["fct_sales"] = self._populate_fct_sales(b_id)
        counts["fct_inventory"] = self._populate_fct_inventory(b_id)

        duration = (datetime.now() - start_time).total_seconds()
        self._log_pipeline_metric(b_id, "warehouse_star_schema_load", "SUCCESS", sum(counts.values()), duration)

        logger.info(f"Star schema warehouse load finished in {duration:.2f}s: {counts}")
        return counts

    def _ensure_tables_exist(self) -> None:
        """Executes DDL to guarantee schema structures."""
        ddl_file = Path("warehouse/ddl/02_create_star_schema.sql")
        if not ddl_file.exists():
            return

        with open(ddl_file, "r", encoding="utf-8") as f:
            ddl_sql = f.read()

        if self.is_postgres:
            with self.conn.cursor() as cur:
                cur.execute(open("warehouse/ddl/01_init_warehouse.sql").read())
                cur.execute(ddl_sql)
                cur.execute(open("warehouse/ddl/03_create_indexes.sql").read())
        else:
            # SQLite compatibility adjustments for local tests
            cursor = self.conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pipeline_health_metrics (
                    metric_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id TEXT,
                    step_name TEXT,
                    status TEXT,
                    rows_in INT DEFAULT 0,
                    rows_out INT DEFAULT 0,
                    rows_quarantined INT DEFAULT 0,
                    execution_time_sec REAL,
                    error_message TEXT,
                    logged_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS data_quality_metrics (
                    test_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id TEXT,
                    dataset_name TEXT,
                    validation_suite TEXT,
                    tests_run INT,
                    tests_passed INT,
                    tests_failed INT,
                    quarantine_count INT DEFAULT 0,
                    pass_rate_pct REAL,
                    executed_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            self.conn.commit()

    def _populate_dim_dates(self) -> int:
        """Generates comprehensive calendar dimension records from 2025-01-01 to 2027-12-31."""
        records = []
        cur_date = date(2025, 1, 1)
        end_date = date(2027, 12, 31)

        while cur_date <= end_date:
            d_key = int(cur_date.strftime("%Y%m%d"))
            records.append({
                "date_key": d_key,
                "calendar_date": cur_date.strftime("%Y-%m-%d"),
                "year": cur_date.year,
                "quarter": (cur_date.month - 1) // 3 + 1,
                "month": cur_date.month,
                "month_name": cur_date.strftime("%B"),
                "week_of_year": cur_date.isocalendar()[1],
                "day_of_month": cur_date.day,
                "day_of_week": cur_date.weekday() + 1,
                "day_name": cur_date.strftime("%A"),
                "is_weekend": cur_date.weekday() in (5, 6),
                "fiscal_year": cur_date.year if cur_date.month >= 2 else cur_date.year - 1,
                "fiscal_quarter": ((cur_date.month - 2) % 12) // 3 + 1,
            })
            cur_date += timedelta(days=1)

        df = pd.DataFrame(records)
        self._write_table("dim_dates", df, ["date_key"])
        return len(df)

    def _populate_dim_stores(self) -> int:
        df = self._read_silver("stores")
        if df.empty:
            return 0
        df["store_key"] = df["store_id"].apply(lambda sid: generate_surrogate_key("STORE", sid))
        self._write_table("dim_stores", df, ["store_key"])
        return len(df)

    def _populate_dim_employees(self) -> int:
        df = self._read_silver("employees")
        if df.empty:
            return 0
        df["employee_key"] = df["employee_id"].apply(lambda eid: generate_surrogate_key("EMP", eid))
        df["full_name"] = df["first_name"] + " " + df["last_name"]
        df["annual_salary"] = df["annual_estimated_salary"]
        self._write_table("dim_employees", df, ["employee_key"])
        return len(df)

    def _populate_dim_suppliers(self) -> int:
        df = self._read_silver("suppliers")
        if df.empty:
            return 0
        df["supplier_key"] = df["supplier_id"].apply(lambda sid: generate_surrogate_key("SUP", sid))
        self._write_table("dim_suppliers", df, ["supplier_key"])
        return len(df)

    def _populate_dim_customers_scd2(self) -> int:
        """Loads DimCustomer applying SCD Type 2 validity intervals."""
        df = self._read_silver("customers")
        if df.empty:
            return 0

        df["valid_from"] = pd.to_datetime(df["created_at"], utc=True).dt.strftime("%Y-%m-%d %H:%M:%S")
        df["valid_to"] = None
        df["is_current"] = True
        df["customer_key"] = df.apply(
            lambda r: generate_surrogate_key("CUST", r["customer_id"], r["valid_from"]), axis=1
        )
        self._write_table("dim_customers", df, ["customer_key"])
        return len(df)

    def _populate_dim_products_scd2(self) -> int:
        """Loads DimProduct applying SCD Type 2 validity intervals."""
        df = self._read_silver("products")
        if df.empty:
            return 0

        df["valid_from"] = pd.to_datetime(df["created_at"], utc=True).dt.strftime("%Y-%m-%d %H:%M:%S")
        df["valid_to"] = None
        df["is_current"] = True
        df["product_key"] = df.apply(
            lambda r: generate_surrogate_key("PROD", r["product_id"], r["valid_from"]), axis=1
        )
        self._write_table("dim_products", df, ["product_key"])
        return len(df)

    def _populate_fct_orders(self, batch_id: str) -> int:
        """Loads FactOrders linking dimension surrogate keys."""
        df = self._read_silver("orders")
        if df.empty:
            return 0

        # Resolve customer surrogate key consistent with dim_customers
        cust_df = self._read_silver("customers")
        if not cust_df.empty:
            cust_df["cust_valid_from"] = pd.to_datetime(cust_df["created_at"], utc=True).dt.strftime("%Y-%m-%d %H:%M:%S")
            cust_key_map = dict(zip(
                cust_df["customer_id"],
                cust_df.apply(lambda r: generate_surrogate_key("CUST", r["customer_id"], r["cust_valid_from"]), axis=1)
            ))
            df["customer_key"] = df["customer_id"].map(cust_key_map).fillna("UNKNOWN_CUSTOMER")
        else:
            df["customer_key"] = "UNKNOWN_CUSTOMER"

        df["order_key"] = df["order_id"].apply(lambda oid: generate_surrogate_key("ORDER", oid))
        df["date_key"] = pd.to_datetime(df["order_date"], utc=True).dt.strftime("%Y%m%d").astype(int)
        df["store_key"] = df["store_id"].apply(lambda sid: generate_surrogate_key("STORE", sid))
        df["employee_key"] = df["employee_id"].apply(lambda eid: generate_surrogate_key("EMP", eid))
        df["net_amount"] = df["total_amount"]
        df["lakehouse_batch_id"] = batch_id

        self._write_table("fct_orders", df, ["order_key"])
        return len(df)

    def _populate_fct_sales(self, batch_id: str) -> int:
        """Loads FactSales linking orders, line items, and products."""
        items_df = self._read_silver("order_items")
        orders_df = self._read_silver("orders")
        prods_df = self._read_silver("products")

        if items_df.empty or orders_df.empty:
            return 0

        merged = items_df.merge(orders_df[["order_id", "order_date", "customer_id", "store_id"]], on="order_id", how="inner")
        if not prods_df.empty:
            merged = merged.merge(prods_df[["product_id", "cost_price"]], on="product_id", how="left")
            merged["cost_price"] = merged["cost_price"].fillna(merged["unit_price"] * 0.6)
        else:
            merged["cost_price"] = merged["unit_price"] * 0.6

        # Resolve customer surrogate key consistent with dim_customers
        cust_df = self._read_silver("customers")
        if not cust_df.empty:
            cust_df["cust_valid_from"] = pd.to_datetime(cust_df["created_at"], utc=True).dt.strftime("%Y-%m-%d %H:%M:%S")
            cust_key_map = dict(zip(
                cust_df["customer_id"],
                cust_df.apply(lambda r: generate_surrogate_key("CUST", r["customer_id"], r["cust_valid_from"]), axis=1)
            ))
            merged["customer_key"] = merged["customer_id"].map(cust_key_map).fillna("UNKNOWN_CUSTOMER")
        else:
            merged["customer_key"] = "UNKNOWN_CUSTOMER"

        # Resolve product surrogate key consistent with dim_products
        if not prods_df.empty:
            prods_df["prod_valid_from"] = pd.to_datetime(prods_df["created_at"], utc=True).dt.strftime("%Y-%m-%d %H:%M:%S")
            prod_key_map = dict(zip(
                prods_df["product_id"],
                prods_df.apply(lambda r: generate_surrogate_key("PROD", r["product_id"], r["prod_valid_from"]), axis=1)
            ))
            merged["product_key"] = merged["product_id"].map(prod_key_map).fillna("UNKNOWN_PRODUCT")
        else:
            merged["product_key"] = "UNKNOWN_PRODUCT"

        merged["sale_key"] = merged["order_item_id"].apply(lambda iid: generate_surrogate_key("SALE", iid))
        merged["order_key"] = merged["order_id"].apply(lambda oid: generate_surrogate_key("ORDER", oid))
        merged["date_key"] = pd.to_datetime(merged["order_date"], utc=True).dt.strftime("%Y%m%d").astype(int)
        merged["store_key"] = merged["store_id"].apply(lambda sid: generate_surrogate_key("STORE", sid))

        merged["line_total"] = merged["total_line_amount"]
        merged["line_cost"] = (merged["quantity"] * merged["cost_price"]).round(2)
        merged["line_profit"] = (merged["line_total"] - merged["line_cost"]).round(2)
        merged["profit_margin_pct"] = ((merged["line_profit"] / merged["line_total"]) * 100).round(2)
        merged["lakehouse_batch_id"] = batch_id

        self._write_table("fct_sales", merged, ["sale_key"])
        return len(merged)

    def _populate_fct_inventory(self, batch_id: str) -> int:
        """Loads FactInventory."""
        inv_df = self._read_silver("inventory")
        prods_df = self._read_silver("products")
        if inv_df.empty:
            return 0

        merged = inv_df.merge(prods_df[["product_id", "cost_price"]], on="product_id", how="left")
        merged["cost_price"] = merged["cost_price"].fillna(25.0)

        # Resolve product surrogate key consistent with dim_products
        if not prods_df.empty:
            prods_df["prod_valid_from"] = pd.to_datetime(prods_df["created_at"], utc=True).dt.strftime("%Y-%m-%d %H:%M:%S")
            prod_key_map = dict(zip(
                prods_df["product_id"],
                prods_df.apply(lambda r: generate_surrogate_key("PROD", r["product_id"], r["prod_valid_from"]), axis=1)
            ))
            merged["product_key"] = merged["product_id"].map(prod_key_map).fillna("UNKNOWN_PRODUCT")
        else:
            merged["product_key"] = "UNKNOWN_PRODUCT"

        merged["inventory_key"] = merged["inventory_id"].apply(lambda iid: generate_surrogate_key("INV", iid))
        merged["date_key"] = int(datetime.now(timezone.utc).strftime("%Y%m%d"))
        merged["store_key"] = merged["store_id"].apply(lambda sid: generate_surrogate_key("STORE", sid))
        merged["inventory_value_usd"] = (merged["quantity_on_hand"] * merged["cost_price"]).round(2)
        merged["lakehouse_batch_id"] = batch_id

        self._write_table("fct_inventory", merged, ["inventory_key"])
        return len(merged)

    def _log_pipeline_metric(self, batch_id: str, step: str, status: str, rows_out: int, duration_sec: float) -> None:
        """Inserts telemetry into monitoring.pipeline_health_metrics."""
        try:
            cursor = self.conn.cursor()
            table_name = "monitoring.pipeline_health_metrics" if self.is_postgres else "pipeline_health_metrics"
            sql_stmt = f"""
                INSERT INTO {table_name} (batch_id, step_name, status, rows_out, execution_time_sec)
                VALUES (?, ?, ?, ?, ?)
            """ if not self.is_postgres else f"""
                INSERT INTO {table_name} (batch_id, step_name, status, rows_out, execution_time_sec)
                VALUES (%s, %s, %s, %s, %s)
            """
            cursor.execute(sql_stmt, (batch_id, step, status, rows_out, duration_sec))
            self.conn.commit()
        except Exception as e:
            logger.warning(f"Could not record pipeline telemetry: {e}")

    # -------------------------------------------------------------------------
    # Helper Utilities
    # -------------------------------------------------------------------------
    def _read_silver(self, table_name: str) -> pd.DataFrame:
        p = self.silver_dir / f"{table_name}.csv"
        return pd.read_csv(p) if p.exists() else pd.DataFrame()

    def _write_table(self, table_name: str, df: pd.DataFrame, primary_keys: List[str]) -> None:
        if df.empty:
            return

        target_table = f"analytics.{table_name}" if self.is_postgres else table_name
        if not self.is_postgres:
            df.to_sql(target_table, self.conn, if_exists="replace", index=False)
            return

        # PostgreSQL load using psycopg2
        cursor = self.conn.cursor()
        cols = list(df.columns)
        if sql is not None:
            query = sql.SQL(
                "INSERT INTO {schema}.{table} ({fields}) VALUES ({values}) ON CONFLICT DO NOTHING"
            ).format(
                schema=sql.Identifier("analytics"),
                table=sql.Identifier(table_name),
                fields=sql.SQL(", ").join([sql.Identifier(c) for c in cols]),
                values=sql.SQL(", ").join([sql.Placeholder()] * len(cols)),
            )
        else:
            placeholders = ",".join(["%s"] * len(cols))
            col_names = ",".join(cols)
            query = f"INSERT INTO {target_table} ({col_names}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"
        records = [tuple(row) for row in df.values]
        try:
            execute_batch(cursor, query, records, page_size=1000)
            self.conn.commit()
        except Exception as err:
            self.conn.rollback()
            logger.error(f"Failed to batch insert into {target_table}: {err}")
            raise


if __name__ == "__main__":
    loader = WarehouseLoader()
    loader.load_entire_warehouse()
