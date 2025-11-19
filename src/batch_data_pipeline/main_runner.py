from dotenv import load_dotenv
load_dotenv()
from datetime import date
from pathlib import Path
import os
from typing import List, Dict, Any, Tuple, Type

from google.cloud import storage


bucket_name = os.getenv("GCS_BUCKET")
if not bucket_name:
    raise RuntimeError("GCS_BUCKET must be set")

client = storage.Client()
bucket = client.bucket(bucket_name)


from batch_data_pipeline.ingestion.ingestors.customer_ingestion import ingest_customers
from batch_data_pipeline.ingestion.ingestors.product_ingestion import ingest_products
from batch_data_pipeline.ingestion.ingestors.order_ingestion import ingest_orders
from batch_data_pipeline.ingestion.ingestors.order_item_ingestion import ingest_order_items
from batch_data_pipeline.ingestion.ingestors.payment_ingestion import ingest_payments

from batch_data_pipeline.ingestion.loaders.upload_csvs_to_bucket import upload_raw_files
from batch_data_pipeline.generators.generator import EcommerceDataGenerator
from batch_data_pipeline.ingestion.utils.file_finder import list_csvs


def run_full_ingestion(run_dt: date, logger=None):
    base_output_dir = Path(os.getenv("ECOMMERCE_DATA_DIR", "/tmp/ecommerce_data"))
    base_output_dir.mkdir(parents=True, exist_ok=True)
    day_folder = base_output_dir / run_dt.isoformat()

    bucket_name = os.getenv("GCS_BUCKET")
    if not bucket_name:
        raise RuntimeError("GCS_BUCKET must be set")

    # Generate raw data
    gen = EcommerceDataGenerator(
        output_path=str(base_output_dir),
        daily_rows={"customers": 200, "products": 10, "orders": 2000},
        initial_rows={"customers": 50000, "products": 5000, "orders": 100000},
    )
    gen.run_incremental_batch(run_dt)

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # ------------------ RAW UPLOAD ------------------
    raw_files = list_csvs(day_folder)
    raw_uris = upload_raw_files(
        csv_files=raw_files,
        bucket=bucket,
        prefix=f"raw/ecommerce/{run_dt.isoformat()}",
        logger=logger,
    )

    # ------------------ VALIDATED / QUARANTINE ------------------
    results = {
        "customers": ingest_customers(day_folder, run_dt, bucket),
        "products": ingest_products(day_folder, run_dt, bucket),
        "orders": ingest_orders(day_folder, run_dt, bucket),
        "order_items": ingest_order_items(day_folder, run_dt, bucket),
        "payments": ingest_payments(day_folder, run_dt, bucket),
    }

    return {
        "run_date": run_dt.isoformat(),
        "raw_uploaded": raw_uris,
        "validated_and_quarantine": results,
    }


if __name__ == "__main__":
    import sys
    import json
    from datetime import date

    run_date = date.fromisoformat(sys.argv[1])
    summary = run_full_ingestion(run_date)
    print(json.dumps(summary, indent=4, default=str))