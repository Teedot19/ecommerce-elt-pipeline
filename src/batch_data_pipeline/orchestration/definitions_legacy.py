# orchestration/definitions.py

import os
import sys
from dotenv import load_dotenv


# Ensure project root is importable (before importing assets)

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# Now safe to load environment variables
load_dotenv()



# Dagster imports

from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules,
)
from dagster_dbt import DbtCliResource


# Asset modules (import modules, not symbols)
import batch_data_pipeline.orchestration.assets.extract as extract
import batch_data_pipeline.orchestration.assets.dbt_run as dbt_run



# Load all assets from the modules

all_assets = load_assets_from_modules([extract, dbt_run])


# Asset job

daily_job = define_asset_job(
    name="daily_materialize",
    selection=AssetSelection.assets(
        extract.run_ecommerce_ingestion,
        dbt_run.dbt_build,  
    ),
    partitions_def=extract.daily_partitions,
)


# Schedule definition

daily_schedule = ScheduleDefinition(
    name="daily_schedule",
    job=daily_job,
    cron_schedule="0 2 * * *",  # 2 AM daily
)


# Dagster project definition

defs = Definitions(
    assets=all_assets,
    resources={
        "dbt": DbtCliResource(
            project_dir=os.getenv("DBT_PROJECT_DIR", "/Users/teedotflims/projects/batch_data_pipeline/ecommerce_analytics"),
            profiles_dir=os.getenv("DBT_PROFILES_DIR", "/Users/teedotflims/.dbt"),
        ),
    },
    schedules=[daily_schedule],
)
