import json
import os
import csv
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Tuple

from dagster import asset, Output, DailyPartitionsDefinition
from google.cloud import storage

from batch_data_pipeline.generators.generator import EcommerceDataGenerator

from batch_data_pipeline.ingestion.ingestors.customer_ingestion import (
    validate_customer_rows,
)
from batch_data_pipeline.ingestion.ingestors.product_ingestion import (
    validate_product_rows,
)
from batch_data_pipeline.ingestion.ingestors.order_ingestion import (
    validate_order_rows,
)
from batch_data_pipeline.ingestion.ingestors.order_item_ingestion import (
    validate_order_item_rows,
)
from batch_data_pipeline.ingestion.ingestors.payment_ingestion import (
    validate_payment_rows,
)

from batch_data_pipeline.ingestion.loaders.upload_validated_to_bucket import (
    upload_validated_to_bucket,
)
from batch_data_pipeline.ingestion.loaders.upload_quarantined_to_bucket import (
    upload_quarantine_to_bucket,
)


# -------------------------------------------------------------------
# Shared config / helpers
# -------------------------------------------------------------------

daily_partitions = DailyPartitionsDefinition(start_date="2025-11-16")


def get_base_output_dir() -> Path:
    base = os.getenv("ECOMMERCE_DATA_DIR")
    if not base:
        raise RuntimeError("ECOMMERCE_DATA_DIR must be set")
    return Path(base)


def get_bucket() -> storage.Bucket:
    bucket_name = os.getenv("GCS_BUCKET")
    if not bucket_name:
        raise RuntimeError("GCS_BUCKET must be set")
    client = storage.Client()
    return client.bucket(bucket_name)


def partition_date(partition_key: str) -> date:
    return date.fromisoformat(partition_key)


def read_csv(path: Path) -> List[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def make_file_path(day_folder: Path, entity: str, run_dt: date) -> Path:
    return day_folder / f"{entity}_{run_dt}.csv"


# -------------------------------------------------------------------
# 1) Generator asset – creates all local raw CSVs for the day
# -------------------------------------------------------------------

@asset(partitions_def=daily_partitions)
def generate_raw_ecommerce_data(context) -> Path:
    """Generate all raw ecommerce CSVs for the given partition date."""
    run_dt = partition_date(context.partition_key)

    base_output_dir = get_base_output_dir()
    base_output_dir.mkdir(parents=True, exist_ok=True)

    day_folder = base_output_dir / run_dt.isoformat()
    day_folder.mkdir(parents=True, exist_ok=True)

    gen = EcommerceDataGenerator(
        output_path=str(base_output_dir),
        daily_rows={"customers": 200, "products": 10, "orders": 2000},
        initial_rows={"customers": 50000, "products": 5000, "orders": 100000},
    )
    gen.run_incremental_batch(run_dt)

    context.log.info(f"Generated raw CSVs in {day_folder}")
    return day_folder


# -------------------------------------------------------------------
# 2) RAW assets – read local CSV into in-memory rows per entity
# -------------------------------------------------------------------

@asset(partitions_def=daily_partitions)
def raw_customers(context, generate_raw_ecommerce_data: Path) -> List[Dict[str, Any]]:
    run_dt = partition_date(context.partition_key)
    path = make_file_path(generate_raw_ecommerce_data, "customers", run_dt)
    return read_csv(path)


@asset(partitions_def=daily_partitions)
def raw_products(context, generate_raw_ecommerce_data: Path) -> List[Dict[str, Any]]:
    run_dt = partition_date(context.partition_key)
    path = make_file_path(generate_raw_ecommerce_data, "products", run_dt)
    return read_csv(path)


@asset(partitions_def=daily_partitions)
def raw_orders(context, generate_raw_ecommerce_data: Path) -> List[Dict[str, Any]]:
    run_dt = partition_date(context.partition_key)
    path = make_file_path(generate_raw_ecommerce_data, "orders", run_dt)
    return read_csv(path)


@asset(partitions_def=daily_partitions)
def raw_order_items(context, generate_raw_ecommerce_data: Path) -> List[Dict[str, Any]]:
    run_dt = partition_date(context.partition_key)
    path = make_file_path(generate_raw_ecommerce_data, "order_items", run_dt)
    return read_csv(path)


@asset(partitions_def=daily_partitions)
def raw_payments(context, generate_raw_ecommerce_data: Path) -> List[Dict[str, Any]]:
    run_dt = partition_date(context.partition_key)
    path = make_file_path(generate_raw_ecommerce_data, "payments", run_dt)
    return read_csv(path)


# -------------------------------------------------------------------
# 3) VALIDATION assets – split into cleaned + invalid per entity
#    (to avoid double validation)
# -------------------------------------------------------------------

def _validate(
    rows: List[Dict[str, Any]],
    validator,
) -> Dict[str, List[Dict[str, Any]]]:
    cleaned, invalid = validator(rows)
    return {"cleaned": cleaned, "invalid": invalid}


@asset(partitions_def=daily_partitions)
def customers_validation(raw_customers: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    return _validate(raw_customers, validate_customer_rows)


@asset(partitions_def=daily_partitions)
def products_validation(raw_products: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    return _validate(raw_products, validate_product_rows)


@asset(partitions_def=daily_partitions)
def orders_validation(raw_orders: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    return _validate(raw_orders, validate_order_rows)


@asset(partitions_def=daily_partitions)
def order_items_validation(raw_order_items: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    return _validate(raw_order_items, validate_order_item_rows)


@asset(partitions_def=daily_partitions)
def payments_validation(raw_payments: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    return _validate(raw_payments, validate_payment_rows)


# -------------------------------------------------------------------
# 4) VALIDATED assets – upload cleaned rows to GCS
# -------------------------------------------------------------------

@asset(partitions_def=daily_partitions)
def validated_customers(context, customers_validation: Dict[str, List[Dict[str, Any]]]) -> str:
    run_dt = partition_date(context.partition_key)
    bucket = get_bucket()
    cleaned = customers_validation["cleaned"]

    uri = upload_validated_to_bucket(
        rows=cleaned,
        bucket=bucket,
        entity="customers",
        run_date=run_dt.isoformat(),
        logger=context.log,
    )
    context.log.info(f"Uploaded validated customers → {uri}")
    return uri


@asset(partitions_def=daily_partitions)
def validated_products(context, products_validation: Dict[str, List[Dict[str, Any]]]) -> str:
    run_dt = partition_date(context.partition_key)
    bucket = get_bucket()
    cleaned = products_validation["cleaned"]

    uri = upload_validated_to_bucket(
        rows=cleaned,
        bucket=bucket,
        entity="products",
        run_date=run_dt.isoformat(),
        logger=context.log,
    )
    context.log.info(f"Uploaded validated products → {uri}")
    return uri


@asset(partitions_def=daily_partitions)
def validated_orders(context, orders_validation: Dict[str, List[Dict[str, Any]]]) -> str:
    run_dt = partition_date(context.partition_key)
    bucket = get_bucket()
    cleaned = orders_validation["cleaned"]

    uri = upload_validated_to_bucket(
        rows=cleaned,
        bucket=bucket,
        entity="orders",
        run_date=run_dt.isoformat(),
        logger=context.log,
    )
    context.log.info(f"Uploaded validated orders → {uri}")
    return uri


@asset(partitions_def=daily_partitions)
def validated_order_items(context, order_items_validation: Dict[str, List[Dict[str, Any]]]) -> str:
    run_dt = partition_date(context.partition_key)
    bucket = get_bucket()
    cleaned = order_items_validation["cleaned"]

    uri = upload_validated_to_bucket(
        rows=cleaned,
        bucket=bucket,
        entity="order_items",
        run_date=run_dt.isoformat(),
        logger=context.log,
    )
    context.log.info(f"Uploaded validated order_items → {uri}")
    return uri


@asset(partitions_def=daily_partitions)
def validated_payments(context, payments_validation: Dict[str, List[Dict[str, Any]]]) -> str:
    run_dt = partition_date(context.partition_key)
    bucket = get_bucket()
    cleaned = payments_validation["cleaned"]

    uri = upload_validated_to_bucket(
        rows=cleaned,
        bucket=bucket,
        entity="payments",
        run_date=run_dt.isoformat(),
        logger=context.log,
    )
    context.log.info(f"Uploaded validated payments → {uri}")
    return uri


# -------------------------------------------------------------------
# 5) QUARANTINE assets – upload invalid rows to GCS
# -------------------------------------------------------------------

@asset(partitions_def=daily_partitions)
def quarantine_customers(context, customers_validation: Dict[str, List[Dict[str, Any]]]) -> str:
    run_dt = partition_date(context.partition_key)
    bucket = get_bucket()
    invalid = customers_validation["invalid"]

    uri = upload_quarantine_to_bucket(
        rows=invalid,
        bucket=bucket,
        entity="customers",
        run_date=run_dt.isoformat(),
        logger=context.log,
    )
    context.log.info(f"Uploaded quarantined customers → {uri}")
    return uri


@asset(partitions_def=daily_partitions)
def quarantine_products(context, products_validation: Dict[str, List[Dict[str, Any]]]) -> str:
    run_dt = partition_date(context.partition_key)
    bucket = get_bucket()
    invalid = products_validation["invalid"]

    uri = upload_quarantine_to_bucket(
        rows=invalid,
        bucket=bucket,
        entity="products",
        run_date=run_dt.isoformat(),
        logger=context.log,
    )
    context.log.info(f"Uploaded quarantined products → {uri}")
    return uri


@asset(partitions_def=daily_partitions)
def quarantine_orders(context, orders_validation: Dict[str, List[Dict[str, Any]]]) -> str:
    run_dt = partition_date(context.partition_key)
    bucket = get_bucket()
    invalid = orders_validation["invalid"]

    uri = upload_quarantine_to_bucket(
        rows=invalid,
        bucket=bucket,
        entity="orders",
        run_date=run_dt.isoformat(),
        logger=context.log,
    )
    context.log.info(f"Uploaded quarantined orders → {uri}")
    return uri


@asset(partitions_def=daily_partitions)
def quarantine_order_items(context, order_items_validation: Dict[str, List[Dict[str, Any]]]) -> str:
    run_dt = partition_date(context.partition_key)
    bucket = get_bucket()
    invalid = order_items_validation["invalid"]

    uri = upload_quarantine_to_bucket(
        rows=invalid,
        bucket=bucket,
        entity="order_items",
        run_date=run_dt.isoformat(),
        logger=context.log,
    )
    context.log.info(f"Uploaded quarantined order_items → {uri}")
    return uri


@asset(partitions_def=daily_partitions)
def quarantine_payments(context, payments_validation: Dict[str, List[Dict[str, Any]]]) -> str:
    run_dt = partition_date(context.partition_key)
    bucket = get_bucket()
    invalid = payments_validation["invalid"]

    uri = upload_quarantine_to_bucket(
        rows=invalid,
        bucket=bucket,
        entity="payments",
        run_date=run_dt.isoformat(),
        logger=context.log,
    )
    context.log.info(f"Uploaded quarantined payments → {uri}")
    return uri


# -------------------------------------------------------------------
# 6) Summary asset – one node that “represents” the ingestion run
# -------------------------------------------------------------------

@asset(partitions_def=daily_partitions)
def ingestion_summary(
    context,
    validated_customers: str,
    validated_products: str,
    validated_orders: str,
    validated_order_items: str,
    validated_payments: str,
    quarantine_customers: str,
    quarantine_products: str,
    quarantine_orders: str,
    quarantine_order_items: str,
    quarantine_payments: str,
) -> Dict[str, Any]:
    run_dt = partition_date(context.partition_key)

    summary: Dict[str, Any] = {
        "run_date": run_dt.isoformat(),
        "validated": {
            "customers": validated_customers,
            "products": validated_products,
            "orders": validated_orders,
            "order_items": validated_order_items,
            "payments": validated_payments,
        },
        "quarantine": {
            "customers": quarantine_customers,
            "products": quarantine_products,
            "orders": quarantine_orders,
            "order_items": quarantine_order_items,
            "payments": quarantine_payments,
        },
    }

    context.log.info(json.dumps(summary, indent=4, default=str))

    return Output(
        value=summary,
        metadata={
            "run_date": run_dt.isoformat(),
            "validated_customers": validated_customers,
            "validated_products": validated_products,
            "validated_orders": validated_orders,
            "validated_order_items": validated_order_items,
            "validated_payments": validated_payments,
            "quarantine_customers": quarantine_customers,
            "quarantine_products": quarantine_products,
            "quarantine_orders": quarantine_orders,
            "quarantine_order_items": quarantine_order_items,
            "quarantine_payments": quarantine_payments,
            "summary_json": json.dumps(summary, indent=2, default=str),
        },
    )
