from pydantic import BaseModel, EmailStr, Field, field_validator
from typing import Optional
from datetime import date


class Customer(BaseModel):
    # Raw CSV fields come in as strings — Pydantic will cast types.

    customer_id: str = Field(..., min_length=1)

    first_name: str
    last_name: str

    # Email: CSV may contain None, "", or invalid
    email: Optional[EmailStr]


    # Generator outputs full country names → allow longer strings
    country: Optional[str]

    # CSV contains "YYYY-MM-DD" as a string → Pydantic will coerce to date
    signup_date: date

    # -----------------------------
    # VALIDATORS
    # -----------------------------

    @field_validator("first_name", "last_name", mode="before")
    def validate_name(cls, v):
        if v is None:
            raise ValueError("Name cannot be null")

        v = v.strip()
        if len(v) < 2:
            raise ValueError("Name must be at least 2 characters")

        # allow letters, spaces, hyphens, apostrophes, unicode letters
        if not all(c.isalpha() or c in " -'" for c in v):
            raise ValueError("Invalid characters in name")

        return v.title()

    @field_validator("email")
    def validate_email(cls, v: str) -> str:
        # allow None email (generator injects bad values)
        if v is None:
            return None

        v = v.strip()
        if v == "":
            return None

        # if invalid → leave for email validator to catch later
        return v

    @field_validator("email")
    def cast_email(cls, v):
        if v is None:
            return None
        # Let EmailStr validate format
        return v

    @field_validator("country", mode="before")
    def validate_country(cls, v):
        if v is None:
            return None

        v = v.strip()
        bad_values = {"", "UNKNOWN", "N/A", "NULL", "NONE", "--"}

        # treat these as missing
        if v.upper() in bad_values:
            return None

        # allow full country names
        return v

    @field_validator("signup_date", mode="before")
    def validate_signup_date(cls, v):
        """
        Input comes as string 'YYYY-MM-DD'.
        Pydantic will convert it into a datetime.date.
        """
        if isinstance(v, str):
            v = v.strip()
        return v

    # -----------------------------
    # MODEL CONFIG
    # -----------------------------
    model_config = {
        "str_strip_whitespace": True,
        "validate_assignment": True,
    }





