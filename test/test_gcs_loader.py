from ingestion.loaders.gcs_loader import upload_csvs_to_gcs


def test_upload_to_gcs_uploads_when_missing(mocker, temp_dir):
    f = temp_dir / "customers.csv"
    f.write_text("data")

    mock_client = mocker.patch("ingestion.loaders.gcs_loader.storage.Client")
    bucket = mock_client.return_value.bucket.return_value
    blob = bucket.blob.return_value

    blob.exists.return_value = False

    paths = upload_csvs_to_gcs(
        csv_files=[f],
        bucket_name="test-bucket",
        prefix="raw/ecommerce/test",
        logger=None,
    )

    blob.upload_from_filename.assert_called_once_with(str(f))
    assert paths == ["gs://test-bucket/raw/ecommerce/test/customers.csv"]
