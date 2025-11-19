import os
from datetime import date
from pathlib import Path

from batch_data_pipeline.generators.generator import EcommerceDataGenerator


def generate_raw_files(run_dt: date):
    """
    Runs ONLY the generator and returns the folder where CSVs are stored.
    """

    base_output_dir = Path(os.getenv("ECOMMERCE_DATA_DIR", "/tmp/ecommerce_data"))
    base_output_dir.mkdir(parents=True, exist_ok=True)

    gen = EcommerceDataGenerator(
        output_path=str(base_output_dir),
        daily_rows={"customers": 200, "products": 10, "orders": 2000},
        initial_rows={"customers": 50000, "products": 5000, "orders": 100000},
    )

    gen.run_incremental_batch(run_dt)

    day_folder = base_output_dir / run_dt.isoformat()

    return day_folder
