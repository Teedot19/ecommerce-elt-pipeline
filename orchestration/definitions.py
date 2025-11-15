# orchestration/definitions.py
from dotenv import load_dotenv

load_dotenv()
import os
import sys

from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules,
)
from dagster_dbt import DbtCliResource

from assets import dbt_run, extract
from assets.extract import daily_partitions

# Ensure project root is importable (so 'assets/' can be imported)
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


# --- assets ---
all_assets = load_assets_from_modules([extract, dbt_run])

# --- jobs ---
daily_job = define_asset_job(
    name="daily_materialize",
    selection=AssetSelection.keys(
        "run_ecommerce_ingestion",
        "dbt_build",
    ),
    partitions_def=daily_partitions,
)

# --- schedule ---
daily_schedule = ScheduleDefinition(
    name="daily_schedule",
    job=daily_job,
    cron_schedule="0 2 * * *",  # runs daily at 2 AM
)

# --- final Dagster project definition ---
defs = Definitions(
    assets=all_assets,
    resources={
        "dbt": DbtCliResource(
            project_dir="/Users/teedotflims/projects/batch_data_pipeline/ecommerce_analytics",
            profiles_dir="/Users/teedotflims/.dbt",
        ),
    },
    schedules=[daily_schedule],
)
