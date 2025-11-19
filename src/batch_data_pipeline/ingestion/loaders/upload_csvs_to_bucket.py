from pathlib import Path
from typing import List
from google.cloud import storage

def build_raw_blob_path(prefix: str, file: Path) -> str:
    return f"{prefix}/{file.name}"

def build_gcs_uri(bucket_name: str, blob_path: str) -> str:
    return f"gs://{bucket_name}/{blob_path}"

def blob_exists(bucket: storage.Bucket, blob_path: str) -> bool:
    return bucket.blob(blob_path).exists()

def upload_file(bucket: storage.Bucket, local_file: Path, blob_path: str) -> None:
    bucket.blob(blob_path).upload_from_filename(str(local_file))

def upload_raw_files(
    csv_files: List[Path],
    bucket: storage.Bucket,
    prefix: str,
    logger=None,
) -> List[str]:
    """
    Upload the original raw CSV files untouched.
    Perfect for Snowpipe.
    """
    uris = []

    for file in csv_files:
        blob_path = build_raw_blob_path(prefix, file)
        uri = build_gcs_uri(bucket.name, blob_path)

        if blob_exists(bucket, blob_path):
            if logger:
                logger.info(f"Skipping {blob_path} — already exists")
            uris.append(uri)
            continue

        upload_file(bucket, file, blob_path)
        uris.append(uri)

    return uris
