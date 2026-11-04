"""
Synthetic Retail Data Generator
================================
Generates enterprise-scale synthetic retail datasets:
- 10 core CSV tables with relational foreign keys
- Change Data Capture (CDC) updates (SCD Type 2 candidates)
- Intentional anomalies to validate Great Expectations & Dead Letter Queue (DLQ)
- Late-arriving transaction events to stress-test lakehouse out-of-order ingestion
"""

import os
import csv
import random
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Dict, Any

from config.logging_config import get_logger

logger = get_logger("mock_data_generator")


def generate_all_datasets(output_dir: str = "data/raw", seed: int = 42) -> Dict[str, int]:
    """Generates all 10 retail domain CSV files."""
    random.seed(seed)
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    summary: Dict[str, int] = {}

    base_time = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    # -------------------------------------------------------------------------
    # 1. STORES (10 stores across US)
    # -------------------------------------------------------------------------
    stores = [
        {"store_id": "STR_001", "store_name": "Manhattan Flagship", "store_type": "FLAGSHIP", "city": "New York", "state": "NY", "country": "USA", "square_feet": 45000, "opened_date": "2018-04-15"},
        {"store_id": "STR_002", "store_name": "Union Square Tech Hub", "store_type": "EXPRESS", "city": "San Francisco", "state": "CA", "country": "USA", "square_feet": 18000, "opened_date": "2019-08-20"},
        {"store_id": "STR_003", "store_name": "Michigan Ave Retail", "store_type": "FLAGSHIP", "city": "Chicago", "state": "IL", "country": "USA", "square_feet": 38000, "opened_date": "2017-11-10"},
        {"store_id": "STR_004", "store_name": "Domain Northside", "store_type": "OUTLET", "city": "Austin", "state": "TX", "country": "USA", "square_feet": 22000, "opened_date": "2021-02-14"},
        {"store_id": "STR_005", "store_name": "Pike Place Center", "store_type": "EXPRESS", "city": "Seattle", "state": "WA", "country": "USA", "square_feet": 16500, "opened_date": "2020-05-30"},
        {"store_id": "STR_006", "store_name": "Brickell City Center", "store_type": "FLAGSHIP", "city": "Miami", "state": "FL", "country": "USA", "square_feet": 34000, "opened_date": "2019-12-01"},
        {"store_id": "STR_007", "store_name": "Cherry Creek Hub", "store_type": "OUTLET", "city": "Denver", "state": "CO", "country": "USA", "square_feet": 20000, "opened_date": "2022-03-18"},
        {"store_id": "STR_008", "store_name": "Back Bay Boutique", "store_type": "EXPRESS", "city": "Boston", "state": "MA", "country": "USA", "square_feet": 15000, "opened_date": "2018-09-09"},
        {"store_id": "STR_009", "store_name": "Buckhead Plaza", "store_type": "FLAGSHIP", "city": "Atlanta", "state": "GA", "country": "USA", "square_feet": 31000, "opened_date": "2020-10-25"},
        {"store_id": "STR_010", "store_name": "Century City Center", "store_type": "ONLINE_FULFILLMENT", "city": "Los Angeles", "state": "CA", "country": "USA", "square_feet": 60000, "opened_date": "2021-07-04"},
    ]
    _write_csv(f"{output_dir}/stores.csv", stores, list(stores[0].keys()))
    summary["stores"] = len(stores)

    # -------------------------------------------------------------------------
    # 2. EMPLOYEES (30 retail associates & managers)
    # -------------------------------------------------------------------------
    roles = ["Sales Associate", "Store Manager", "Shift Supervisor", "Inventory Specialist", "Cashier"]
    employees = []
    for i in range(1, 31):
        store_id = f"STR_{random.randint(1, 10):03d}"
        role = random.choice(roles)
        rate = 45.0 if "Manager" in role else 22.5
        employees.append({
            "employee_id": f"EMP_{i:04d}",
            "store_id": store_id,
            "first_name": f"StaffFirst{i}",
            "last_name": f"StaffLast{i}",
            "role": role,
            "hourly_rate": rate,
            "hire_date": (base_time - timedelta(days=random.randint(200, 1500))).strftime("%Y-%m-%d"),
        })
    _write_csv(f"{output_dir}/employees.csv", employees, list(employees[0].keys()))
    summary["employees"] = len(employees)

    # -------------------------------------------------------------------------
    # 3. SUPPLIERS (15 global vendors)
    # -------------------------------------------------------------------------
    suppliers = []
    supplier_countries = ["USA", "Germany", "Japan", "South Korea", "Vietnam", "Canada", "Italy"]
    for i in range(1, 16):
        suppliers.append({
            "supplier_id": f"SUP_{i:03d}",
            "supplier_name": f"Global Vendor {i} Corp",
            "contact_email": f"procurement@vendor{i}.com",
            "country": random.choice(supplier_countries),
            "payment_terms": random.choice(["NET_30", "NET_60", "NET_90", "IMMEDIATE"]),
        })
    _write_csv(f"{output_dir}/suppliers.csv", suppliers, list(suppliers[0].keys()))
    summary["suppliers"] = len(suppliers)

    # -------------------------------------------------------------------------
    # 4. PRODUCTS (50 products across categories)
    # -------------------------------------------------------------------------
    categories = {
        "Electronics": ["Smartphones", "Laptops", "Audio", "Wearables"],
        "Apparel": ["Men's Wear", "Women's Wear", "Footwear", "Accessories"],
        "Home Goods": ["Kitchenware", "Bedding", "Smart Home", "Lighting"],
        "Outdoor": ["Camping", "Cycling", "Fitness Gear", "Footwear"],
    }
    products = []
    prod_counter = 1
    for cat, subcats in categories.items():
        for subcat in subcats:
            for item_idx in range(1, 4):
                cost = round(random.uniform(15.0, 350.0), 2)
                price = round(cost * random.uniform(1.4, 2.5), 2)
                p_id = f"PRD_{prod_counter:04d}"
                created = (base_time - timedelta(days=random.randint(100, 400))).isoformat()
                
                # Introduce SCD Type 2 update on selected products (price bump)
                updated = created
                if prod_counter in [3, 7, 12]:
                    updated = (base_time - timedelta(days=random.randint(5, 30))).isoformat()
                    price = round(price * 1.15, 2)

                products.append({
                    "product_id": p_id,
                    "product_name": f"{subcat} Model {item_idx}",
                    "category": cat,
                    "subcategory": subcat,
                    "unit_price": price,
                    "cost_price": cost,
                    "sku": f"SKU-{cat[:3].upper()}-{p_id}",
                    "is_active": True,
                    "created_at": created,
                    "updated_at": updated,
                })
                prod_counter += 1

    # Injected Corrupt Record 1: Product with duplicate ID
    duplicate_prod = dict(products[0])
    duplicate_prod["unit_price"] = 9999.99
    products.append(duplicate_prod)

    _write_csv(f"{output_dir}/products.csv", products, list(products[0].keys()))
    summary["products"] = len(products)

    # -------------------------------------------------------------------------
    # 5. CUSTOMERS (100 customers with SCD Type 2 updates)
    # -------------------------------------------------------------------------
    cities = [("New York", "NY"), ("Austin", "TX"), ("Seattle", "WA"), ("Chicago", "IL"), ("San Francisco", "CA")]
    segments = ["VIP", "REGULAR", "NEW", "WHOLESALE"]
    customers = []
    for i in range(1, 101):
        city, state = random.choice(cities)
        created = (base_time - timedelta(days=random.randint(50, 500))).isoformat()
        updated = created
        segment = random.choice(segments)

        # Simulate CDC address change / tier upgrade for SCD Type 2 tracking
        if i in [5, 10, 15, 20]:
            updated = (base_time - timedelta(days=random.randint(1, 15))).isoformat()
            segment = "VIP"
            city, state = "Miami", "FL"

        customers.append({
            "customer_id": f"CUST_{i:05d}",
            "first_name": f"UserFirst{i}",
            "last_name": f"UserLast{i}",
            "email": f"customer_{i}@retaildomain.org",
            "phone": f"+1-555-{random.randint(100, 999):03d}-{random.randint(1000, 9999):04d}",
            "city": city,
            "state": state,
            "country": "USA",
            "postal_code": f"{random.randint(10000, 99999)}",
            "customer_segment": segment,
            "created_at": created,
            "updated_at": updated,
        })

    # Injected Corrupt Record 2: Customer with malformed email & missing ID for DLQ
    customers.append({
        "customer_id": "",  # Violation: Null Primary Key!
        "first_name": "Ghost",
        "last_name": "Record",
        "email": "invalid_email_no_domain",
        "phone": "000-000",
        "city": "Unknown",
        "state": "XX",
        "country": "USA",
        "postal_code": "00000",
        "customer_segment": "REGULAR",
        "created_at": base_time.isoformat(),
        "updated_at": base_time.isoformat(),
    })
    _write_csv(f"{output_dir}/customers.csv", customers, list(customers[0].keys()))
    summary["customers"] = len(customers)

    # -------------------------------------------------------------------------
    # 6. INVENTORY (Stores x Products snapshot)
    # -------------------------------------------------------------------------
    inventory = []
    inv_id = 1
    for s in stores[:8]:
        for p in products[:25]:
            qty = random.randint(5, 150)
            inventory.append({
                "inventory_id": f"INV_{inv_id:06d}",
                "store_id": s["store_id"],
                "product_id": p["product_id"],
                "quantity_on_hand": qty,
                "reorder_level": 25,
                "last_restocked_at": (base_time - timedelta(days=random.randint(1, 30))).isoformat(),
            })
            inv_id += 1
    _write_csv(f"{output_dir}/inventory.csv", inventory, list(inventory[0].keys()))
    summary["inventory"] = len(inventory)

    # -------------------------------------------------------------------------
    # 7. ORDERS & 8. ORDER ITEMS (500 orders + line items)
    # -------------------------------------------------------------------------
    orders = []
    order_items = []
    payments = []
    item_id = 1
    pay_id = 1

    order_statuses = ["COMPLETED", "COMPLETED", "COMPLETED", "SHIPPED", "PENDING", "REFUNDED", "CANCELLED"]
    payment_methods = ["CREDIT_CARD", "APPLE_PAY", "DEBIT_CARD", "PAYPAL"]

    for ord_num in range(1, 501):
        o_id = f"ORD_{ord_num:06d}"
        c_id = f"CUST_{random.randint(1, 100):05d}"
        s_id = f"STR_{random.randint(1, 10):03d}"
        e_id = f"EMP_{random.randint(1, 30):04d}"

        # Generate realistic order dates spanning last 90 days
        order_days_ago = random.randint(1, 90)
        order_ts = base_time - timedelta(days=order_days_ago, hours=random.randint(1, 23), minutes=random.randint(0, 59))
        status = random.choice(order_statuses)

        # Generate 1 to 4 line items
        num_items = random.randint(1, 4)
        order_total = 0.0
        for _ in range(num_items):
            chosen_prod = random.choice(products[:45])
            quantity = random.randint(1, 3)
            unit_price = float(chosen_prod["unit_price"])
            line_tot = round(quantity * unit_price, 2)
            order_total += line_tot

            order_items.append({
                "order_item_id": f"ITEM_{item_id:07d}",
                "order_id": o_id,
                "product_id": chosen_prod["product_id"],
                "quantity": quantity,
                "unit_price": unit_price,
                "total_line_amount": line_tot,
            })
            item_id += 1

        discount = round(order_total * random.choice([0.0, 0.0, 0.05, 0.10]), 2)
        net_order_total = round(order_total - discount, 2)

        orders.append({
            "order_id": o_id,
            "customer_id": c_id,
            "store_id": s_id,
            "employee_id": e_id,
            "order_status": status,
            "order_date": order_ts.isoformat(),
            "total_amount": net_order_total,
            "discount_amount": discount,
            "updated_at": (order_ts + timedelta(hours=random.randint(1, 48))).isoformat(),
        })

        # Payment record
        payments.append({
            "payment_id": f"PAY_{pay_id:06d}",
            "order_id": o_id,
            "payment_method": random.choice(payment_methods),
            "payment_status": "SUCCESS" if status not in ["REFUNDED", "CANCELLED"] else "REVERSED",
            "amount": net_order_total,
            "currency": "USD",
            "payment_date": order_ts.isoformat(),
        })
        pay_id += 1

    # Injected Corrupt Records:
    # 3. Order with Negative Total (Business Rule Violation -> DLQ)
    orders.append({
        "order_id": "ORD_CORRUPT_NEG_001",
        "customer_id": "CUST_00001",
        "store_id": "STR_001",
        "employee_id": "EMP_0001",
        "order_status": "COMPLETED",
        "order_date": base_time.isoformat(),
        "total_amount": -149.99,  # Negative amount!
        "discount_amount": 0.0,
        "updated_at": base_time.isoformat(),
    })

    # 4. Late-Arriving Order (Out-of-order transaction 120 days old, arrived in current batch)
    orders.append({
        "order_id": "ORD_LATE_ARRIVING_999",
        "customer_id": "CUST_00002",
        "store_id": "STR_002",
        "employee_id": "EMP_0002",
        "order_status": "COMPLETED",
        "order_date": (base_time - timedelta(days=120)).isoformat(),  # Historical date
        "total_amount": 89.50,
        "discount_amount": 0.0,
        "updated_at": datetime.now(timezone.utc).isoformat(),  # Arrived today!
    })

    _write_csv(f"{output_dir}/orders.csv", orders, list(orders[0].keys()))
    summary["orders"] = len(orders)

    _write_csv(f"{output_dir}/order_items.csv", order_items, list(order_items[0].keys()))
    summary["order_items"] = len(order_items)

    _write_csv(f"{output_dir}/payments.csv", payments, list(payments[0].keys()))
    summary["payments"] = len(payments)

    # -------------------------------------------------------------------------
    # 9. REVIEWS (150 verified customer reviews)
    # -------------------------------------------------------------------------
    reviews = []
    review_comments = [
        "Exceptional quality, surpassed expectations!",
        "Arrived fast, packaging was solid.",
        "Good value for money, recommended.",
        "Average build quality, could be better.",
        "Terrible experience, broke after two days.",
    ]
    for r_idx in range(1, 151):
        chosen_ord = random.choice(orders[:400])
        reviews.append({
            "review_id": f"REV_{r_idx:06d}",
            "product_id": f"PRD_{random.randint(1, 40):04d}",
            "customer_id": chosen_ord["customer_id"],
            "rating": random.choices([5, 4, 3, 2, 1], weights=[50, 25, 12, 8, 5])[0],
            "review_text": random.choice(review_comments),
            "review_date": (base_time - timedelta(days=random.randint(1, 60))).isoformat(),
        })

    # Injected Corrupt Review: Out-of-bounds rating 9 (Valid range is 1-5)
    reviews.append({
        "review_id": "REV_CORRUPT_INVALID_RATING",
        "product_id": "PRD_0001",
        "customer_id": "CUST_00001",
        "rating": 9,  # Invalid!
        "review_text": "Impossible rating value",
        "review_date": base_time.isoformat(),
    })

    _write_csv(f"{output_dir}/reviews.csv", reviews, list(reviews[0].keys()))
    summary["reviews"] = len(reviews)

    logger.info(f"Successfully generated 10 raw retail datasets: {summary}")
    return summary


def _write_csv(file_path: str, records: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    """Helper to write records cleanly with header."""
    with open(file_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)


if __name__ == "__main__":
    generate_all_datasets()

