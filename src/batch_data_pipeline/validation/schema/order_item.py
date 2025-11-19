from pydantic import BaseModel, Field, field_validator
from typing import Optional
from datetime import datetime


class OrderItem(BaseModel):
    # Raw CSV → strings → Pydantic coerces to correct types

    order_item_id: str = Field(..., min_length=1)
    order_id: str = Field(..., min_length=1)
    product_id: str = Field(..., min_length=1)

    quantity: Optional[int]
    unit_price: Optional[float]
    line_total: Optional[float]

    # -----------------------------------
    # VALIDATORS
    # -----------------------------------

    @field_validator("quantity", mode="before")
    def clean_quantity(cls, v):
        """
        Generator bad values possible:
        - None
        - 0
        - -3
        - "ten"
        """
        if v is None:
            return None

        # Valid int
        if isinstance(v, int):
            return v if v > 0 else None

        v = str(v).strip()

        if v.isdigit():  # positive integer
            return int(v) if int(v) > 0 else None

        # Non-numeric → bad
        return None

    @field_validator("unit_price", mode="before")
    def clean_unit_price(cls, v):
        """
        This comes from:
        - product price map (good)
        - OR random.uniform
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

    @field_validator("line_total", mode="before")
    def clean_line_total(cls, v):
        """
        Bad values possible:
        - None
        - -50
        - 0
        - 9999
        - "NaN"
        """
        if v is None:
            return None

        if isinstance(v, (int, float)):
            # must be > 0 and not unreasonably large
            return v if (v > 0 and v < 10000) else None

        v = str(v).strip().lower()

        # Reject "nan" or non-numeric
        if v in {"nan", "inf", "-inf"}:
            return None

        try:
            f = float(v)
            return f if (f > 0 and f < 10000) else None
        except Exception:
            return None

    # -----------------------------------
    # MODEL CONFIG
    # -----------------------------------

    model_config = {
        "str_strip_whitespace": True,
        "validate_assignment": True,
    }
