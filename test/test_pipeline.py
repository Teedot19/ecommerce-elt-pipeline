from datetime import date

from ingestion.pipeline import os, run_full_ingestion


def test_pipeline_end_to_end(mocker, temp_dir):
    # inject temp_dir into env
    mocker.patch.dict(os.environ, {"ECOMMERCE_DATA_DIR": str(temp_dir), "GCS_BUCKET": "test-bucket"})

    # mock GCS
    mock_client = mocker.patch("ingestion.gcs_loader.storage.Client")
    blob = mock_client.return_value.bucket.return_value.blob.return_value
    blob.exists.return_value = False

    result = run_full_ingestion(date(2025, 11, 7))

    assert result["file_count"] == 5
    assert len(result["gcs_paths"]) == 5
