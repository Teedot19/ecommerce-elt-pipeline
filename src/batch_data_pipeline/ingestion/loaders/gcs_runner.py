from google.cloud import storage
from pathlib import Path
from quarantine_uploader import upload_quarantine_to_bucket
from csv_uploader import upload_csvs_to_bucket


# ---- dependency creation (DI) ----
client = storage.Client()
bucket = client.bucket("my_bucket")


# ---- quarantine upload example ----
uri = upload_quarantine_to_bucket(
    rows=invalid_rows,
    bucket=bucket,
    entity="orders",
    run_date="2025-11-19",
    logger=logger,
)

print(uri)


# ---- CSV batch upload example ----
upload_csvs_to_bucket(
    csv_files=[Path("/tmp/a.csv"), Path("/tmp/b.csv")],
    bucket=bucket,
    prefix="validated_raw/orders",
    logger=logger,
)
