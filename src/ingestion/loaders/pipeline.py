import os
from datetime import date
from pathlib import Path

from ingestion.utils.file_finder import list_csvs
from ingestion.loaders.gcs_loader import upload_csvs_to_gcs
from ingestion.generators.generator import EcommerceDataGenerator


def run_full_ingestion(run_dt: date, logger=None) -> dict:
    # 1. Resolve paths
    base_output_dir = Path(os.getenv("ECOMMERCE_DATA_DIR", "/tmp/ecommerce_data"))
    base_output_dir.mkdir(parents=True, exist_ok=True)

    # 2. Bucket
    bucket_name = os.getenv("GCS_BUCKET")
    if not bucket_name:
        raise RuntimeError("GCS_BUCKET must be set")

    # 3. Generate data
    gen = EcommerceDataGenerator(
        output_path=str(base_output_dir),
        daily_rows={"customers": 200, "products": 10, "orders": 2000},
        initial_rows={"customers": 50000, "products": 5000, "orders": 100000},
    )
    gen.run_incremental_batch(run_dt)

    # 4. Locate files
    day_folder = base_output_dir / run_dt.isoformat()
    csv_files = list_csvs(day_folder)
    if not csv_files:
        raise FileNotFoundError(f"No CSVs found for {run_dt} in {day_folder}")

    # 5. Upload
    gcs_paths = upload_csvs_to_gcs(
        csv_files=csv_files,
        bucket_name=bucket_name,
        prefix=f"raw/ecommerce/{run_dt.isoformat()}",
        logger=logger,
    )

    return {
        "local_folder": str(day_folder),
        "file_count": len(csv_files),
        "gcs_paths": gcs_paths,
        "bucket": bucket_name,
    }
