import os
import requests
from google.cloud import storage

# services = ['fhv','green','yellow']
gcs_bucket_name = os.environ.get("GOOGLE_GCS_BUCKET")
download_base_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data'


def upload_to_gcs(bucket_name: str, blob_name: str, content) -> None:
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()

    bucket = client.bucket(bucket_name)

    blob = bucket.blob(blob_name)

    blob.upload_from_file(content, content_type='application/octet-stream')


def web_to_gcs(bucket_name: str, year: str, service: str, count=12) -> None:
    for i in range(count):

        # sets the month part of the file_name string
        month = '0'+str(i+1)
        month = month[-2:]

        blob_name = f"{service}_tripdata_{year}-{month}.parquet"
        download_url = f"{download_base_url}/{blob_name}"

        # Download the file and upload it to GCS
        print(f"Downloading {download_url}")
        res = requests.get(download_url, stream=True)

        if res.status_code == 200:
            print(f"Uploading {download_url} to GCS")

            upload_to_gcs(bucket_name=bucket_name,
                          blob_name=blob_name,
                          content=res.raw)

            print(f"Uploaded {download_url} to GCS\n")


if __name__ == "__main__":
    # services = ['fhv','green','yellow']
    web_to_gcs(bucket_name=gcs_bucket_name, year='2021', service='green')
