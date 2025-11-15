from ingestion.customer_generator import CustomerGenerator
from validation.validators import validate_records
from validation.schemas.customer import Customer



def process_customer_records(date: str):
    """
    1. Generate raw data (from your generator.py)
    2. Validate it using the Customer Pydantic schema
    3. Return clean + invalid records
    """

    # STEP 1 — initialize your generator class
    generator = CustomerGenerator()

    # STEP 2 — generate raw unvalidated records
    raw_records = generator.generate_customers(4)

    # STEP 3 — validate each record
    valid, invalid = validate_records(raw_records, Customer)

    # STEP 4 — convert Pydantic objects into clean dictionaries
    cleaned = [record.model_dump() for record in valid]

    return cleaned, invalid
