import csv
from pathlib import Path
from datetime import date
import pytest

from batch_data_pipeline.ingestion.customer_ingestion import ingest_customers

def test_customer_ingestion_integration(tmp_path):
    run_dt = date(2024, 1, 1)

    # Create folder: /tmp/.../2024-01-01/
    day_folder = tmp_path / run_dt.isoformat()
    day_folder.mkdir()

    # Create small test CSV file
    file_path = day_folder / f"customers_{run_dt}.csv"

    valid_row = {
        "customer_id": "1",
        "first_name": "John",
        "last_name": "Doe",
        "email": "john@example.com",
        "country": "US",
        "signup_date": "2024-01-01"
    }

    invalid_row = {
        "customer_id": "2",
        "first_name": "A",
        "last_name": "B",
        "email": "bad-email",   # invalid
        "country": "US",
        "signup_date": "2024-01-01"
    }

    # Write the CSV file
    with file_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=valid_row.keys())
        writer.writeheader()
        writer.writerow(valid_row)
        writer.writerow(invalid_row)

    # Run ingestion (real pipeline)
    result = ingest_customers(
        folder_path=day_folder,
        run_date=run_dt,
        bucket="dummy"
    )

    # --- Assertions (integration-level) ---

    # Validate returned summary
    assert result["valid_rows"] == 1
    assert result["invalid_rows"] == 1

    # Validate file outputs exist
    validated_path = Path(result["validated_path"])
    quarantine_path = Path(result["quarantine_path"])

    assert validated_path.exists()
    assert quarantine_path.exists()
    assert validated_path.read_text().strip() != ""
    assert quarantine_path.read_text().strip() != ""
