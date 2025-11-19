from .quanrantine_helpers import (
    build_quarantine_temp_path,
    build_quarantine_blob_path,
    build_gcs_uri,
    write_quarantine_csv,
    blob_exists,
    upload_file,
)
from typing import List, Dict, Any, Tuple, Type
from google.cloud import storage


def upload_quarantine_to_bucket(
    rows: List[Dict[str, Any]],
    bucket: storage.Bucket,
    entity: str,
    run_date: str,
    logger=None,
) -> str:
    """
    Writes invalid rows → temp CSV,
    uploads to injected bucket,
    returns the gs:// URI.
    """
    temp_path = build_quarantine_temp_path(entity, run_date)
    blob_path = build_quarantine_blob_path(entity, run_date)
    uri = build_gcs_uri(bucket.name, blob_path)

    write_quarantine_csv(rows, temp_path)

    if blob_exists(bucket, blob_path):
        if logger:
            logger.info(f"Skipping {blob_path} — already exists")
        return uri

    upload_file(bucket, temp_path, blob_path)
    return uri
