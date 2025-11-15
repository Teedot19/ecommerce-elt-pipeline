from typing import Any, Dict, List, Tuple, Type
from pydantic import BaseModel, ValidationError

def validate_records(
    raw_records: List[Dict[str, Any]],
    schema: Type[BaseModel],) -> Tuple[List[BaseModel], List[Dict[str, Any]]]:

    valid = []
    invalid = []

    for record in raw_records:
        try:
            obj = schema.model_validate(record)
            valid.append(obj)
        except ValidationError as e:
            invalid.append({"error": str(e), "raw": record})

    return valid, invalid
