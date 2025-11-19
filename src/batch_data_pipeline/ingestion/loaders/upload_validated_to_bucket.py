from typing import List, Dict, Any
from google.cloud import storage
from pathlib import Path

from .validated_helpers import (
    build_validated_temp_path,
    build_validated_blob_path,
    build_gcs_uri,
    write_validated_csv,
    blob_exists,
    upload_file,
)


def upload_validated_to_bucket(
    rows: List[Dict[str, Any]],
    bucket: storage.Bucket,
    entity: str,
    run_date: str,
    logger=None,
) -> str:
    temp_path = build_validated_temp_path(entity, run_date)
    blob_path = build_validated_blob_path(entity, run_date)
    uri = build_gcs_uri(bucket.name, blob_path)

    write_validated_csv(rows, temp_path)

    if blob_exists(bucket, blob_path):
        if logger:
            logger.info(f"Skipping {blob_path} — already exists")
        return uri

    upload_file(bucket, temp_path, blob_path)
    return uri







def upload_validated_to_bucket(
    rows: List[Dict[str, Any]],
    bucket: storage.Bucket,
    entity: str,
    run_date: str,
    logger=None,
) -> str:
    """
    Write validated rows to a temp CSV and upload to the provided bucket.
    Fully SRP, DI-friendly, no hidden mutation.
    """

    temp_path = build_validated_temp_path(entity, run_date)
    blob_path = build_validated_blob_path(entity, run_date)
    uri = build_gcs_uri(bucket.name, blob_path)

    write_validated_csv(rows, temp_path)

    if blob_exists(bucket, blob_path):
        if logger:
            logger.info(f"Skipping {blob_path} — already exists")
        return uri

    upload_file(bucket, temp_path, blob_path)
    return uri
