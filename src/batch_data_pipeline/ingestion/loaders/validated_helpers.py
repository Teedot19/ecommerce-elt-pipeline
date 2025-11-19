import csv
from pathlib import Path
from typing import List, Dict, Any
from google.cloud import storage


def build_validated_blob_path(entity: str, run_date: str) -> str:
    return f"validated_raw/{entity}/{entity}_{run_date}.csv"


def build_validated_temp_path(entity: str, run_date: str) -> Path:
    return Path(f"/tmp/{entity}_{run_date}_validated.csv")


def build_gcs_uri(bucket_name: str, blob_path: str) -> str:
    return f"gs://{bucket_name}/{blob_path}"

def write_validated_csv(rows: List[Dict[str, Any]], path: Path) -> None:
    """Write validated rows to CSV."""
    if not rows:
        path.write_text("")
        return

    fieldnames = rows[0].keys()

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

def blob_exists(bucket: storage.Bucket, blob_path: str) -> bool:
    return bucket.blob(blob_path).exists()

def upload_file(bucket: storage.Bucket, local_file: Path, blob_path: str) -> None:
    bucket.blob(blob_path).upload_from_filename(str(local_file))