# src/validation/helpers.py
"""Validation helper functions."""

from typing import Any, Dict, List, Tuple, Type
from pydantic import BaseModel, ValidationError


def validate_records(
    raw_records: List[Dict[str, Any]],
    schema: Type[BaseModel],
) -> Tuple[List[BaseModel], List[Dict[str, Any]]]:
    """
    Validate records against a Pydantic schema.
    
    Args:
        raw_records: List of dictionaries to validate
        schema: Pydantic model class to validate against
        
    Returns:
        Tuple of (valid_records, invalid_records)
    """
    valid = []
    invalid = []

    for idx, record in enumerate(raw_records):
        try:
            obj = schema.model_validate(record)
            valid.append(obj)
        except ValidationError as e:
            error_details = []
            for error in e.errors():
                field = ".".join(str(loc) for loc in error['loc'])
                error_details.append({
                    "field": field,
                    "error": error['msg'],
                    "input": error.get('input')
                })
            
            invalid.append({
                "row_index": idx,
                "raw_data": record,
                "errors": error_details
            })

    return valid, invalid