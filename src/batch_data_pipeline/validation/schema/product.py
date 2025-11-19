from pydantic import BaseModel, Field, field_validator
from typing import Optional
from datetime import datetime


class Product(BaseModel):
    # Raw CSV fields come in as strings — Pydantic will cast types.

    product_id: str = Field(..., min_length=1)

    name: str
    category: str

    price: Optional[float]       # may be None or invalid in generator
    stock_count: Optional[int]   # may be None or invalid
    created_at: datetime         # CSV gives string → datetime
    is_active: bool              # CSV gives "True"/"False" → bool

    # -----------------------------------
    # VALIDATORS
    # -----------------------------------

    @field_validator("name", mode="before")
    def validate_name(cls, v):
        if v is None:
            raise ValueError("Product name cannot be null")

        v = v.strip()
        if len(v) < 2:
            raise ValueError("Product name must be at least 2 characters")

        # allow regular characters, spaces, punctuation from Faker
        return v

    @field_validator("category", mode="before")
    def validate_category(cls, v):
        if v is None:
            raise ValueError("Category cannot be null")

        v = v.strip()
        if len(v) < 2:
            raise ValueError("Category must be at least 2 characters")

        return v

    @field_validator("price", mode="before")
    def clean_price(cls, v):
        """
        Generator bad values:
        - None
        - -10
        - 0
        - "FREE"
        - other invalid strings

        Strategy:
        - Convert valid numeric → float
        - Else → None
        """
        if v is None:
            return None

        if isinstance(v, (int, float)):
            return float(v) if v > 0 else None

        v = str(v).strip()

        try:
            f = float(v)
            return f if f > 0 else None
        except Exception:
            return None

    @field_validator("stock_count", mode="before")
    def clean_stock_count(cls, v):
        """
        Generator injects:
        - None
        - -5
        - "??"
        """
        if v is None:
            return None

        if isinstance(v, int):
            return v if v >= 0 else None

        v = str(v).strip()
        if not v.isdigit():
            return None

        return int(v)

    @field_validator("created_at", mode="before")
    def clean_created_at(cls, v):
        """
        CSV provides ISO strings.
        Pydantic will parse into datetime automatically.
        """
        if isinstance(v, str):
            v = v.strip()
        return v

    @field_validator("is_active", mode="before")
    def clean_is_active(cls, v):
        """
        Allow:
        - bool
        - "true"/"false"
        - "1"/"0"
        """
        if isinstance(v, bool):
            return v

        if v is None:
            return False

        v = str(v).strip().lower()

        if v in {"true", "1", "yes"}:
            return True
        if v in {"false", "0", "no"}:
            return False

        # Invalid → mark as False (safe fallback)
        return False

    # -----------------------------------
    # MODEL CONFIG
    # -----------------------------------

    model_config = {
        "str_strip_whitespace": True,
        "validate_assignment": True,
    }
