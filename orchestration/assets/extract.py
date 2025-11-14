
# assets/extract.py
from dagster import (
    asset,
    DailyPartitionsDefinition,
    AssetExecutionContext,
    AssetOut,
    Output,
)
from datetime import date
from pathlib import Path
from typing import List
import os

from ingestion.pipeline import run_full_ingestion

# One partition per day, starting 2025-11-07
daily_partitions = DailyPartitionsDefinition(start_date="2025-11-07")


from dagster import asset, Output
from datetime import date
from ingestion.pipeline import run_full_ingestion

@asset(partitions_def=daily_partitions)
def run_ecommerce_ingestion(context):
    run_dt = date.fromisoformat(context.partition_key)

    result = run_full_ingestion(
        run_dt=run_dt,
        logger=context.log,
    )

    # Extract values
    gcs_paths = result["gcs_paths"]
    file_count = result["file_count"]
    bucket = result["bucket"]
    local_folder = result["local_folder"]

    return Output(
        value=gcs_paths,
        metadata={
            "file_count": file_count,
            "uploaded_files": gcs_paths,       # shows full list in UI
            "bucket": bucket,
            "local_folder": local_folder,
            "date": run_dt.isoformat(),
        }
    )

