from faker import Faker
from faker_commerce import Provider
import random
from datetime import datetime
from typing import List, Dict


class EcommerceDataGenerator:

    def __init__(self, num_products=50, num_customers=1000):
        """
        Initialize the faker engine and store config.
        We set a random seed so every run produces different data.
        """
        self.fake = Faker()
        self.fake.add_provider(Provider)  # gives us ecommerce-specific methods

        # Seed randomness so each run generates different data
        seed = int(datetime.utcnow().timestamp())
        Faker.seed(seed)
        random.seed(seed)

        self.num_products = num_products
        self.num_customers = num_customers


    def generate_products(self) -> List[Dict]:
        """
        Generate product dimension table.
        Each product has a stable product_id (1..N).
        """
        products = []

        for i in range(1, self.num_products + 1):
            products.append({
                "product_id": i,
                "name": self.fake.ecommerce_name(),
                "category": self.fake.ecommerce_category(),
                "base_price": round(random.uniform(5.0, 500.0), 2),
                "stock_quantity": random.randint(0, 1000),
            })

        return products


    def generate_customers(self) -> List[Dict]:
        """
        Generate customer dimension table.
        """
        customers = []

        for i in range(1, self.num_customers + 1):
            customers.append({
                "customer_id": i,
                "customer_name": self.fake.name(),
                "email": self.fake.email(),
                "phone": self.fake.phone_number(),
                "city": self.fake.city(),
                "country": self.fake.country(),
                "signup_date": self.fake.date_between(
                    start_date="-2y", end_date="today"
                ).isoformat(),
                "customer_segment": random.choice(["Premium", "Regular", "New"]),
            })

        return customers
    
    def generate_orders(self, products: List[Dict], customers: List[Dict]) -> List[Dict]:
        """
        Generate order fact table.
        Each customer gets 0–5 random orders.
        """
        orders = []
        order_id = 1

        for customer in customers:
            num_orders = random.randint(0, 5)

            for _ in range(num_orders):
                product = random.choice(products)
                quantity = random.randint(1, 5)
                unit_price = product["base_price"]
                total_price = round(unit_price * quantity, 2)

                orders.append({
                    "order_id": order_id,
                    "customer_id": customer["customer_id"],
                    "product_id": product["product_id"],
                    "order_timestamp": self.fake.date_time_between(
                        start_date="-2y", end_date="now"
                    ).isoformat(),
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "total_price": total_price,
                })

                order_id += 1

        return orders

