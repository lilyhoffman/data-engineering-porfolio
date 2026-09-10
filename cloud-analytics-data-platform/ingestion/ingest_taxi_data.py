import os
from pathlib import Path

import boto3
import requests


S3_BUCKET = os.environ["S3_BUCKET"]
DATA_URL = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    "yellow_tripdata_2025-01.parquet"
)

LOCAL_DIR = Path("data")
LOCAL_FILE = LOCAL_DIR / "yellow_tripdata_2025-01.parquet"

S3_KEY = "bronze/yellow_taxi/year=2025/month=01/yellow_tripdata_2025-01.parquet"


def download_file():
    LOCAL_DIR.mkdir(exist_ok=True)

    print(f"Downloading data from:\n{DATA_URL}")

    response = requests.get(DATA_URL, stream=True, timeout=120)
    response.raise_for_status()

    with open(LOCAL_FILE, "wb") as file:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                file.write(chunk)

    print(f"Downloaded to {LOCAL_FILE}")


def upload_to_s3():
    s3 = boto3.client("s3")

    print(f"Uploading to s3://{S3_BUCKET}/{S3_KEY}")

    s3.upload_file(
        str(LOCAL_FILE),
        S3_BUCKET,
        S3_KEY,
    )

    print("Upload complete.")


if __name__ == "__main__":
    download_file()
    upload_to_s3()