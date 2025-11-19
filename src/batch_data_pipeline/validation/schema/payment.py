from pydantic import BaseModel, Field, field_validator
from typing import Optional
from datetime import datetime


class Payment(BaseModel):
    # Raw CSV → strings → Pydantic coerces types

    payment_id: str = Field(..., min_length=1)
    order_id: str = Field(..., min_length=1)

    amount: Optional[float]
    payment_method: Optional[str]
    paid_at: datetime   # ISO → datetime

    # -----------------------------------
    # VALIDATORS
    # -----------------------------------

    @field_validator("amount", mode="before")
    def clean_amount(cls, v):
        """
        Generator may inject:
        - None
        - -20
        - "free"
        - 0
        """
        if v is None:
            return None

        if isinstance(v, (int, float)):
            return float(v) if v > 0 else None

        v = str(v).strip().lower()

        # Reject nonsense strings
        if v in {"free", "nan", "", "null"}:
            return None

        try:
            f = float(v)
            return f if f > 0 else None
        except Exception:
            return None

    @field_validator("payment_method", mode="before")
    def clean_payment_method(cls, v):
        """
        Generator injects:
        - "crypto"
        - None
        - "???"
        """
        if v is None:
            return None

        v = str(v).strip().lower()

        allowed = {"card", "paypal", "bank", "apple_pay"}

        if v not in allowed:
            return None

        return v

    @field_validator("paid_at", mode="before")
    def clean_paid_at(cls, v):
        """
        ISO string → datetime
        """
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
