from pydantic import BaseModel, EmailStr, Field,field_validator
from datetime import datetime
from typing import Optional

class Customer(BaseModel):
 
        customer_id: str = Field(
            ..., 
            min_length=1,
            description="Unique customer identifier",
            examples=["cust_00001"]
        )
        
        first_name: str = Field(
            ..., 
            min_length=1, 
            max_length=100,
            description="Customer's first name"
        )
        
        last_name: str = Field(
            ..., 
            min_length=1, 
            max_length=100,
            description="Customer's last name"
        )
        
        email: EmailStr = Field(
            ...,
            description="Customer's email address"
        )
        
        country: Optional[str] = Field(
            None, 
            min_length=2, 
            max_length=2,
            description="ISO 3166-1 alpha-2 country code (e.g., 'US', 'CA')"
        )
        
        signup_date: datetime = Field(
            ...,
            description="Date when customer signed up"
        )
            # === VALIDATORS ===
        @field_validator("first_name", "last_name")
        @classmethod
        def validate_name(cls, v: str) -> str:
            """Validate and clean name fields."""
            v = v.strip()
            
            if not v:
                raise ValueError("Name cannot be empty or whitespace")
            
            if len(v) < 2:
                raise ValueError("Name must be at least 2 characters")
            
            allowed_chars = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ -'")
            if not all(c in allowed_chars for c in v):
                raise ValueError("Name can only contain letters, spaces, hyphens, and apostrophes")
            
            return v.title()
        
        @field_validator("country")
        @classmethod
        def validate_country(cls, v: Optional[str]) -> Optional[str]:
            """Validate country code."""
            if v is None:
                return None
            
            v = v.strip().upper()
            
            unknown_values = {"", "UNKNOWN", "N/A", "NA", "NULL", "NONE", "--"}
            if v in unknown_values:
                return None
            
            if len(v) != 2:
                raise ValueError("Country code must be exactly 2 characters (ISO 3166-1 alpha-2)")
            
            if not v.isalpha():
                raise ValueError("Country code must contain only letters")
            
            return v
        
        @field_validator("signup_date")
        @classmethod
        def validate_signup_date(cls, v: datetime) -> datetime:
            """Validate signup date."""
            now = datetime.now()
            
            if v > now:
                raise ValueError("Signup date cannot be in the future")
            
            if v.year < 1900:
                raise ValueError("Signup date seems unreasonably old")
            
            return v
        
        # === CONFIG ===
        model_config = {
            "str_strip_whitespace": True,
            "validate_assignment": True,
            "json_schema_extra": {
                "examples": [{
                    "customer_id": "cust_00001",
                    "first_name": "John",
                    "last_name": "Doe",
                    "email": "john.doe@example.com",
                    "country": "US",
                    "signup_date": "2025-01-01T00:00:00Z"
                }]
            }
        }
            
        
            