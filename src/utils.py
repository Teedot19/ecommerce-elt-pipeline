from pydantic import BaseModel, field_validator
from datetime import datetime

class Ticker(BaseModel):
    symbol: str
    price: float

    @field_validator("symbol")
    @classmethod
    def uppercase(cls, v):
        return v.upper()

def ts_suffix(dt: datetime) -> str:
    return dt.strftime("%Y%m%d_%H%M%S")
