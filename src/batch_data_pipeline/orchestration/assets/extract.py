# assets/extract.py
import json
from datetime import date

from dagster import asset, Output, DailyPartitionsDefinition
from batch_data_pipeline.main_runner import run_full_ingestion



def parse_partition_date(partition_key: str) -> date:
    return date.fromisoformat(partition_key)


def build_ingestion_metadata(summary: dict) -> dict:
    validated = summary.get("validated_and_quarantine", {})

    return {
        "run_date": summary.get("run_date"),
        "raw_files": summary.get("raw_uploaded", []),
        "validated_entities": list(validated.keys()),
        "invalid_counts": {
            entity: validated[entity]["invalid_rows"]
            for entity in validated
        },
        "valid_counts": {
            entity: validated[entity]["valid_rows"]
            for entity in validated
        },
        # store full structured summary for Dagster UI
        "summary_json": json.dumps(summary, indent=2, default=str),
    }


def execute_ingestion(run_dt: date) -> dict:
    return run_full_ingestion(run_dt)


daily_partitions = DailyPartitionsDefinition(start_date="2025-11-07")


@asset(partitions_def=daily_partitions)
def run_ecommerce_ingestion(context):
 
    run_dt = parse_partition_date(context.partition_key)

    summary = execute_ingestion(run_dt)

    context.log.info(json.dumps(summary, indent=4, default=str))

    metadata = build_ingestion_metadata(summary)

    return Output(value=summary, metadata=metadata)
