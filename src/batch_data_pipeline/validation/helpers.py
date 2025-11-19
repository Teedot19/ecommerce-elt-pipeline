def extract_error_details(e: ValidationError) -> List[Dict[str, Any]]:
    details = []
    for err in e.errors():
        field = ".".join(str(loc) for loc in err["loc"])
        details.append({
            "field": field,
            "error": err["msg"],
            "input": err.get("input"),
        })
    return details

def validate_records(
    raw_records: List[Dict[str, Any]],
    schema: Type[BaseModel],
) -> Tuple[List[BaseModel], List[Dict[str, Any]]]:
    """
    Validate records against a Pydantic schema.
    Returns: (valid_models, invalid_details)
    """
    valid = []
    invalid = []

    for idx, record in enumerate(raw_records):
        try:
            model = schema.model_validate(record)
            valid.append(model)
        except ValidationError as e:
            invalid.append({
                "row_index": idx,
                "raw_data": record,
                "errors": extract_error_details(e),
            })

    return valid, invalid
