from pydantic import BaseModel, field_validator
from datetime import datetime

class Customer(BaseModel):
    customer_id: str
    first_name: str
    last_name: str
    email: str
    country: str
    signup_date: datetime

    @field_validator("first_name", "last_name", "country")
    @classmethod
    def clean_strings(cls, v: str) -> str:
        return v.strip()

    @field_validator("email")
    @classmethod
    def validate_email(cls, v: str) -> str:
        v = v.strip().lower()
        if "@" not in v:
            raise ValueError("Invalid email format")
        return v
