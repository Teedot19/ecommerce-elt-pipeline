import csv
import logging
import os
import random
import uuid
from datetime import date, datetime, timedelta
from typing import Dict, List

from faker import Faker
from faker_commerce import Provider


class EcommerceDataGenerator:
    """
    Generates incremental e-commerce datasets ( customers, products,orders,order_items,payments) with intentional data quality issues.

    Supports:
    - Initial load(large volume)
    - Daily incremental batches
    - Cached IDs for FK relationships
    - Local folder per day: YYYY-MM-DD/*.csv
    """

    CATEGORIES = [
        "Electronics",
        "Clothing",
        "Books",
        "Home & Garden",
        "Sports & Outdoors",
        "Toys & Games",
        "Food & Beverage",
        "Health & Beauty",
        "Automotive",
        "Office Supplies",
        "Furniture",
        "Jewelry",
        "Pet Supplies",
        "Apparel",
    ]
    COUNTRY = ["US", "UK", "CANADA", "EUROPE", "OTHER"]
    CAMPAIGN = [
        "Summer Sale ",
        "Black Friday Blitz",
        "Customer Acquisition",
        "Flash Friday",
        "Brand Refresh",
        "Holiday Special",
        "Loyalty Rewards",
        "Social Media Blast",
        "Q1 2024 Growth",
    ]

    def __init__(self, output_path: str, initial_rows: Dict[str, int], daily_rows: Dict[str, int]):
        self.output_path = output_path
        self.initial_rows = initial_rows
        self.daily_rows = daily_rows

        # faker & randomness
        self.fake = Faker()
        self.fake.add_provider(Provider)
        seed = int(datetime.utcnow().timestamp())
        Faker.seed(seed)
        random.seed(seed)

        # cached IDs (filled after generation)
        self.customers_ids: List[str] = []
        self.products_ids: List[str] = []
        self.orders_ids: List[str] = []

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[logging.StreamHandler(), logging.FileHandler("pipeline.log")],
        )

        self.logger = logging.getLogger(__name__)

    # Public methods
    def run_initial_load(self, day: datetime):
        self.logger.info(f"This is the initial load {day}")
        self._run_batch(day, initial=True)

    def run_incremental_batch(self, day: datetime):
        self.logger.info(f"This is an incremental load {day}")
        self._run_batch(day, initial=False)

    """
        Core engine of the whole class. Every data generation run, initial or Incremental comes through here
    """

    def _run_batch(self, day, initial: bool) -> None:
        """Generate and write all tables for a given batch day."""
        self.logger.info(f"Running {'initial' if initial else 'incremental'} load for {day}")

        # Ensure folder exists for this batch day
        self._ensure_day_folder(day)

        # --- Row counts ---
        num_customers = self._rows("customers", initial)
        num_products = self._rows("products", initial)
        num_orders = self._rows("orders", initial)
        num_order_items = num_orders * 3  # ~3 items per order
        num_payments = num_orders  # 1 payment per order

        # --- Customers ---
        customers = self.generate_customers(num_customers)
        self.customer_ids = [c["customer_id"] for c in customers]
        self._write_csv("customers", day, customers)

        # --- Products ---
        products = self.generate_products(num_products)
        self.product_ids = [p["product_id"] for p in products]
        self.product_price_map = {p["product_id"]: p.get("price") for p in products}
        self._write_csv("products", day, products)

        # --- Orders ---
        orders = self.generate_orders(num_orders)
        self.order_ids = [o["order_id"] for o in orders]
        self._write_csv("orders", day, orders)

        # --- Order Items ---
        order_items = self.generate_order_items(num_order_items)
        self._write_csv("order_items", day, order_items)

        # --- Payments ---
        payments = self.generate_payments(num_payments)
        self._write_csv("payments", day, payments)

    """
    Table Generators
    """

    def generate_customers(self, n: int) -> List[Dict]:
        rows = []

        for _ in range(n):
            row = {
                "customer_id": self._uuid(),
                "first_name": self.fake.first_name(),
                "last_name": self.fake.last_name(),
                "email": self.fake.email(),
                "country": self.fake.country(),
                "signup_date": self._random_date(),
            }
            row = self._maybe_inject_bad_customer_data(row)
            rows.append(row)
        return rows

    def _maybe_inject_bad_customer_data(self, row: Dict) -> Dict:
        if random.random() < 0.05:
            issue = random.choice(["null_email", "invalid_country"])
            if issue == "null_email":
                row["email"] = None
            elif issue == "invalid_country":
                row["country"] = random.choice(["", "UNKNOWN", None])

            self.logger.debug(f"injected bad customer data: {issue}")
        return row

    def generate_products(self, n: int) -> List[Dict]:
        rows = []

        for _ in range(n):
            row = {
                "product_id": self._uuid(),
                "name": self.fake.ecommerce_name(),
                "category": random.choice(self.CATEGORIES),
                "price": round(random.uniform(5, 500), 2),
                "stock_count": random.randint(0, 1000),
                "created_at": self.fake.date_time_between(start_date="-1y", end_date="now").isoformat(),
                "is_active": random.choice([True, False]),
            }
            row = self._maybe_inject_bad_product_data(row)
            rows.append(row)
        return rows

    def _maybe_inject_bad_product_data(self, row: Dict) -> Dict:
        if random.random() < 0.05:
            issue = random.choice(["bad_price", "bad_stock"])
            if issue == "bad_price":
                row["price"] = random.choice([None, -10, 0, "FREE"])
            elif issue == "bad_stock":
                row["stock_count"] = random.choice([None, -5, "??"])

            self.logger.debug(f"injected bad product data: {issue}")
        return row

    def generate_orders(self, n: int) -> List[Dict]:
        rows = []

        for _ in range(n):
            row = {
                "order_id": self._uuid(),
                "customer_id": self._maybe_pick_customer_id(),
                "order_date": self.fake.date_time_between(start_date="-2y", end_date="now").isoformat(),
                "status": random.choice(["new", "processing", "shipped", "cancelled", "returned"]),
                "total_amount": round(random.uniform(20, 800), 2),
                "shipping_cost": round(random.uniform(0, 30), 2),
                "shipping_country": random.choice(self.COUNTRY),
                "campaign": random.choice(self.CAMPAIGN),
            }

            row = self._maybe_inject_bad_order_data(row)
            rows.append(row)
        return rows

    def _maybe_pick_customer_id(self) -> str:
        """5% broken FK, 95% real customer"""
        if random.random() < 0.05 or not self.customer_ids:
            return self._uuid()  # fake ID
        return random.choice(self.customer_ids)

    def _maybe_inject_bad_order_data(self, row: Dict) -> Dict:
        """Inject bad totals, bad currency, bad shipping"""
        if random.random() < 0.05:
            issue = random.choice(["bad_totals", "bad_currency", "bad_shipping"])
            if issue == "bad_totals":
                row["total_amount"] = random.choice([None, -10, 0, -50])
            elif issue == "bad_shipping":
                row["shipping_country"] = random.choice([None, "unknown", "null"])

            self.logger.debug(f"injected bad product data: {issue}")
        return row

    def generate_order_items(self, n: int) -> List[Dict]:
        rows = []
        for _ in range(n):
            order_id = self._maybe_pick_order_id()
            product_id = self._maybe_pick_product_id()
            unit_price = self.product_price_map.get(product_id, round(random.uniform(5, 200), 2))
            quantity = random.randint(1, 5)
            price = self._safe_price(product_id)
            line_total = self._compute_line_total(price, quantity)

            row = {
                "order_item_id": self._uuid(),
                "order_id": order_id,
                "product_id": product_id,
                "quantity": quantity,
                "unit_price": unit_price,
                "line_total": line_total,
            }
            row = self._maybe_inject_bad_order_item_data(row)
            rows.append(row)
        return rows

    def _maybe_inject_bad_order_item_data(self, row: Dict) -> Dict:
        if random.random() < 0.10:
            issue = random.choice(["bad_quantity", "bad_line_total", "bad_product_fk"])
            if issue == "bad_quantity":
                row["quantity"] = random.choice([0, -3, None, "ten"])
            elif issue == "bad_line_total":
                row["line_total"] = random.choice([None, -50, 0, 9999, "NaN"])
            elif issue == "bad_product_fk":
                row["product_id"] = self._uuid()  # guaranteed invalid
            self.logger.debug(f"Injected bad order_item data: {issue}")
        return row

    def generate_payments(self, n: int) -> List[Dict]:
        rows = []
        for _ in range(n):
            order_id = self._maybe_pick_payment_order_id()
            amount = round(random.uniform(20, 800), 2)
            row = {
                "payment_id": self._uuid(),
                "order_id": order_id,
                "amount": amount,
                # "currency": random.choice(self.VALID_CURRENCIES),
                "payment_method": random.choice(["card", "paypal", "bank", "apple_pay"]),
                "paid_at": self.fake.date_time_between(start_date="-2y", end_date="now").isoformat(),
            }
            row = self._maybe_inject_bad_payment_data(row)
            rows.append(row)
        return rows

    def _maybe_pick_payment_order_id(self) -> str:
        if not self.order_ids or random.random() < 0.05:
            return self._uuid()
        return random.choice(self.order_ids)

    def _maybe_inject_bad_payment_data(self, row: Dict) -> Dict:
        if random.random() < 0.10:
            issue = random.choice(["bad_amount", "bad_method", "bad_currency"])
            if issue == "bad_amount":
                row["amount"] = random.choice([None, -20, "free", 0])
            elif issue == "bad_method":
                row["payment_method"] = random.choice(["crypto", None, "???"])
            # elif issue == "bad_currency":
            #    row["currency"] = random.choice(["BTC", "DOGE", None])
            self.logger.debug(f"Injected bad payment data: {issue}")
        return row

    def _day_folder(self, day) -> str:
        return os.path.join(self.output_path, str(day))

    def _ensure_day_folder(self, day) -> str:
        folder = self._day_folder(day)
        os.makedirs(folder, exist_ok=True)
        return folder

    def _csv_path(self, table: str, day) -> str:
        return os.path.join(self._day_folder(day), f"{table}_{day}.csv")

    def _write_csv(self, table: str, day, rows: List[Dict]) -> None:
        if not rows:
            self.logger.warning(f"{table}: no rows generated, skipping write")
            return
        self._ensure_day_folder(day)
        path = self._csv_path(table, day)
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
        self.logger.info(f"Wrote {len(rows):,} {table} rows → {path}")

    def _rows(self, table: str, initial: bool) -> int:
        """Return number of rows to generate for a given table."""
        return self.initial_rows.get(table, 0) if initial else self.daily_rows.get(table, 0)

    def _uuid(self) -> str:
        """Return a UUID4 string."""
        return str(uuid.uuid4())

    def _random_date(self, days_back: int = 730) -> str:
        """Return a random ISO date within the past N days (default 2 years)."""
        offset = random.randint(0, days_back)
        return (date.today() - timedelta(days=offset)).isoformat()

    def _maybe_pick_order_id(self) -> str:
        if not self.order_ids or random.random() < 0.05:
            return self._uuid()  # broken FK
        return random.choice(self.order_ids)

    def _maybe_pick_product_id(self) -> str:
        if not self.product_ids or random.random() < 0.05:
            return self._uuid()  # broken FK
        return random.choice(self.product_ids)

    def _compute_line_total(self, price: float, qty: int):
        """Return correct line total or deliberate bad value."""
        if price is not None:
            return round(price * qty, 2)
        return random.choice([None, -1, 0])

    def _safe_price(self, product_id: str):
        """Return valid price or None if unusable."""
        price = self.product_price_map.get(product_id)
        return price if isinstance(price, (int, float)) and price > 0 else None
