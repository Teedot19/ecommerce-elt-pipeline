from typing import Dict, List, Tuple
from ingestion.generators.generator import EcommerceDataGenerator
from validation.customer import Customer
from validation.helpers import validate_records



def process_customer_records(run_date: str, num_records: int = 5) -> Tuple[List[Dict], List[Dict]]:
    """
    1. Generate raw data (from your generator.py)
    2. Validate it using the Customer Pydantic schema
    3. Return clean + invalid records
    """
    generator = EcommerceDataGenerator(
        output_path="/tmp",
        initial_rows=0,
        daily_rows=0
    )

    raw_records = generator.generate_customers(num_records)
    valid, invalid = validate_records(raw_records, Customer)

    cleaned = [record.model_dump(mode="json") for record in valid]

    return cleaned, invalid