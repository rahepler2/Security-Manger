import os
import boto3
from botocore.client import Config
from typing import Optional

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "trivy-reports")

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1",
)

def ensure_bucket():
    try:
        s3.head_bucket(Bucket=MINIO_BUCKET)
    except Exception:
        s3.create_bucket(Bucket=MINIO_BUCKET)

def upload_file(local_path: str, key: str) -> str:
    ensure_bucket()
    s3.upload_file(local_path, MINIO_BUCKET, key)
    # return a URL for retrieval inside cluster (minio path)
    return f"{MINIO_ENDPOINT.rstrip('/')}/{MINIO_BUCKET}/{key}"
