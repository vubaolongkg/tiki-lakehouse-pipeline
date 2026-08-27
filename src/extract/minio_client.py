import io
import json
import logging
import os
import boto3
from botocore.client import Config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Module-level environment variable resolution
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000").strip().strip("'").strip('"')
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", os.getenv("MINIO_ACCESS_KEY", "admin")).strip().strip("'").strip('"')
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", os.getenv("MINIO_SECRET_KEY", "password123")).strip().strip("'").strip('"')
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "lakehouse").strip()

def get_s3_client(endpoint_url=None, access_key=None, secret_key=None):
    endpoint = endpoint_url or MINIO_ENDPOINT
    if not endpoint.startswith("http://") and not endpoint.startswith("https://"):
        endpoint = f"http://{endpoint}"

    key = access_key or MINIO_ACCESS_KEY
    secret = secret_key or MINIO_SECRET_KEY

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=key,
        aws_secret_access_key=secret,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        region_name="us-east-1"
    )

def ensure_bucket_exists(s3_client, bucket_name=MINIO_BUCKET):
    """Kiểm tra và tạo bucket nếu chưa tồn tại trên MinIO."""
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        logging.info(f"✅ Bucket '{bucket_name}' đã tồn tại.")
    except Exception:
        logging.info(f"⚡ Bucket '{bucket_name}' chưa có, đang tiến hành tạo mới...")
        s3_client.create_bucket(Bucket=bucket_name)
        logging.info(f"✅ Đã tạo thành công bucket '{bucket_name}'.")

def upload_json_to_minio(data, s3_key, s3_client=None, bucket_name=MINIO_BUCKET):
    """Upload dữ liệu Python dict/list dưới dạng JSON vào MinIO S3 bucket."""
    client = s3_client or get_s3_client()
    ensure_bucket_exists(client, bucket_name)

    json_bytes = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
    client.upload_fileobj(io.BytesIO(json_bytes), bucket_name, s3_key)
    logging.info(f"📤 Đã tải lên MinIO: s3://{bucket_name}/{s3_key} ({len(json_bytes)} bytes)")