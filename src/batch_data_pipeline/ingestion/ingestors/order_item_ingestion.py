import csv
from pathlib import Path
from datetime import date
from typing import Dict, Any, List, Tuple

from batch_data_pipeline.validation.schema.order_items import OrderItem
from batch_data_pipeline.validation.helpers import validate_records
from batch_data_pipeline.ingestion.loaders.quarantined_data_uploader import upload_quarantine_to_gcs
from batch_data_pipeline.ingestion.loaders.validated_data_uploader import upload_validated_to_gcs


def read_csv(path: Path) -> List[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def validate_order_item_rows(
    rows: List[Dict[str, Any]]
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:

    valid_models, invalid_models = validate_records(rows, OrderItem)

    cleaned = [m.model_dump(mode="json") for m in valid_models]
    invalid = [m.model_dump(mode="json") for m in invalid_models]

    return cleaned, invalid


def build_summary(
    entity: str,
    run_date: date,
    rows: List[Dict[str, Any]],
    cleaned: List[Dict[str, Any]],
    invalid: List[Dict[str, Any]],
    validated_path: str,
    quarantine_path: str,
) -> Dict[str, Any]:

    return {
        "entity": entity,
        "run_date": run_date.isoformat(),
        "total_rows": len(rows),
        "valid_rows": len(cleaned),
        "invalid_rows": len(invalid),
        "validated_path": validated_path,
        "quarantine_path": quarantine_path,
    }


def ingest_order_item(day_folder: Path, run_dt: date, bucket: str) -> Dict[str, Any]:
    entity = "order_items"
    file_path = day_folder / f"{entity}_{run_dt}.csv"

    if not file_path.exists():
        raise FileNotFoundError(f"Expected file not found: {file_path}")

    # 1. Extract
    rows = read_csv(file_path)

    # 2. Validate
    cleaned, invalid = validate_order_item_rows(rows)

    # 3. Load validated rows
    validated_path = upload_validated_to_gcs(
        bucket_name=bucket,
        entity=entity,
        run_date=run_dt.isoformat(),
        rows=cleaned,
    )

    # 4. Load invalid rows
    quarantine_path = upload_quarantine_to_gcs(
        bucket_name=bucket,
        entity=entity,
        run_date=run_dt.isoformat(),
        rows=invalid,
    )

    # 5. Summarize
    return build_summary(
        entity=entity,
        run_date=run_dt,
        rows=rows,
        cleaned=cleaned,
        invalid=invalid,
        validated_path=validated_path,
        quarantine_path=quarantine_path,
    )
