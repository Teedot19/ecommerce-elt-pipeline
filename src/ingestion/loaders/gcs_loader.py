from pathlib import Path
from typing import List

from google.cloud import storage


def upload_csvs_to_gcs(csv_files: List[Path], bucket_name: str, prefix: str, logger=None) -> List[str]:
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    gcs_paths = []

    for file in csv_files:
        blob_path = f"{prefix}/{file.name}"
        blob = bucket.blob(blob_path)

        if blob.exists():
            if logger:
                logger.info(f"Skipping {blob_path} — already exists")
            gcs_paths.append(f"gs://{bucket_name}/{blob_path}")
            continue

        blob.upload_from_filename(str(file))
        gcs_paths.append(f"gs://{bucket_name}/{blob_path}")

    if logger:
        logger.info(f"Uploaded {len(gcs_paths)} files to GCS")

    return gcs_paths
