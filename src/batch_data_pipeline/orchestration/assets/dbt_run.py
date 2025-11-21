# orchestration/assets/dbt_run.py
from dagster import asset
from dagster_dbt import DbtCliResource

from batch_data_pipeline.orchestration.assets.ecommerce_ingestion import daily_partitions



@asset(partitions_def=daily_partitions, deps=["ingestion_summary"])
def dbt_build(context, dbt: DbtCliResource):
    """Runs dbt build after ingestion completes for this partition."""
    invocation = dbt.cli(["build"])
    result = invocation.wait()

    if not result.is_successful():
        raise RuntimeError(f"dbt build failed with exit code {result.process.returncode}")

    context.log.info("dbt build completed successfully.")
