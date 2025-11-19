from datetime import date
from pathlib import Path
import os

from batch_data_pipeline.ingestion.raw_files import generate_raw_files
from batch_data_pipeline.ingestion.ingestors.customer_ingestion import ingest_customers


def test_customer_ingestion():
    run_dt = date.today()

    # Step 1 — generate the raw CSVs into the output folder
    day_folder = generate_raw_files(run_dt)

    print(f"Raw files generated at: {day_folder}")

    # Optional: list the files
    print("Files in folder:")
    for f in day_folder.iterdir():
        print(" -", f)

    # Step 2 — run customer ingestion
    BUCKET = "dummy-bucket-for-test"    # No real upload
    result = ingest_customers(day_folder, run_dt, BUCKET)

    # Step 3 — print the outputs
    print("\n=== CUSTOMER INGESTION RESULT ===")
    for k, v in result.items():
        if k in ("cleaned", "invalid"):
            print(f"{k}: {len(v)} rows")
        else:
            print(f"{k}: {v}")


if __name__ == "__main__":
    test_customer_ingestion()
