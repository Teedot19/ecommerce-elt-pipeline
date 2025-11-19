from pydantic import BaseModel, Field, field_validator
from typing import Optional
from datetime import datetime


class Order(BaseModel):
    # Raw CSV → strings → Pydantic coerces to correct types

    order_id: str = Field(..., min_length=1)
    customer_id: str = Field(..., min_length=1)

    order_date: datetime       # ISO string → datetime
    status: str                # fixed set of known statuses
    total_amount: Optional[float]
    shipping_cost: Optional[float]
    shipping_country: Optional[str]
    campaign: Optional[str]

    # -----------------------------------
    # VALIDATORS
    # -----------------------------------

    @field_validator("status", mode="before")
    def clean_status(cls, v):
        if v is None:
            raise ValueError("Order status cannot be null")

        v = v.strip().lower()

        allowed = {"new", "processing", "shipped", "cancelled", "returned"}

        if v not in allowed:
            raise ValueError(f"Invalid order status '{v}'")

        return v

    @field_validator("total_amount", mode="before")
    def clean_total_amount(cls, v):
        """
        Generator bad values:
        - None, -10, 0
        """
        if v is None:
            return None

        if isinstance(v, (int, float)):
            return float(v) if v > 0 else None

        try:
            f = float(str(v).strip())
            return f if f > 0 else None
        except Exception:
            return None

    @field_validator("shipping_cost", mode="before")
    def clean_shipping_cost(cls, v):
        if v is None:
            return None

        if isinstance(v, (int, float)):
            return float(v) if v >= 0 else None

        try:
            f = float(str(v).strip())
            return f if f >= 0 else None
        except Exception:
            return None

    @field_validator("shipping_country", mode="before")
    def clean_shipping_country(cls, v):
        """
        Generator bad values:
        - None
        - "unknown"
        - "null"
        """
        if v is None:
            return None

        v = str(v).strip()

        bad_values = {"", "unknown", "null", "none", "--"}

        if v.lower() in bad_values:
            return None

        return v

    @field_validator("campaign", mode="before")
    def clean_campaign(cls, v):
        if v is None:
            return None

        v = str(v).strip()
        return v if v else None

    @field_validator("order_date", mode="before")
    def clean_order_date(cls, v):
        if isinstance(v, str):
            v = v.strip()
        return v

    # -----------------------------------
    # MODEL CONFIG
    # -----------------------------------

    model_config = {
        "str_strip_whitespace": True,
        "validate_assignment": True,
    }
