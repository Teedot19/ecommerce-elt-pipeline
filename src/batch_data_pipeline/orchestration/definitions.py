# src/batch_data_pipeline/orchestration/definitions.py

import os
from dotenv import load_dotenv

load_dotenv()

from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules,
)
from dagster_dbt import DbtCliResource

import batch_data_pipeline.orchestration.assets.ecommerce_ingestion as ecommerce_ingestion
import batch_data_pipeline.orchestration.assets.dbt_run as dbt_build


all_assets = load_assets_from_modules([ecommerce_ingestion, dbt_build])

daily_job = define_asset_job(
    name="daily_materialize",
    selection=AssetSelection.assets(
        ecommerce_ingestion.ingestion_summary,  # ensures whole ingestion graph runs
      dbt_build.dbt_build,                    
    ),
    partitions_def=ecommerce_ingestion.daily_partitions,
)

daily_schedule = ScheduleDefinition(
    name="daily_schedule",
    job=daily_job,
    cron_schedule="0 2 * * *",
)

defs = Definitions(
    assets=all_assets,
    resources={
        "dbt": DbtCliResource(
            project_dir=os.getenv(
                "DBT_PROJECT_DIR",
                "/Users/teedotflims/projects/batch_data_pipeline/ecommerce_analytics",
            ),
            profiles_dir=os.getenv("DBT_PROFILES_DIR", "/Users/teedotflims/.dbt"),
        ),
    },
    schedules=[daily_schedule],
)
