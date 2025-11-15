from faker import Faker
from datetime import datetime
import uuid
import random

class CustomerGenerator:
    def __init__(self):
        self.fake = Faker()

    def _uuid(self):
        return str(uuid.uuid4())

    def _random_date(self):
        return self.fake.date_time_between(start_date="-2y", end_date="now")

    def _inject_bad_data(self, row: dict) -> dict:
        # OPTIONAL: copy logic from your big generator if you want
        return row

    def generate_customers(self, n: int):
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

            row = self._inject_bad_data(row)
            rows.append(row)

        return rows
