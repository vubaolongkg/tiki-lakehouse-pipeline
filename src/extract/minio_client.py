import os
import io
import json
import boto3
from botocore.client import Config

# Lấy cấu hình từ biến môi trường, loại bỏ khoảng trắng và xuống dòng thừa
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://lakehouse_minio:9000").replace("\r", "").replace("\n", "").strip()
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", os.getenv("MINIO_ACCESS_KEY", "admin")).replace("\r", "").replace("\n", "").strip()
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", os.getenv("MINIO_SECRET_KEY", "password123")).replace("\r", "").replace("\n", "").strip()
BUCKET_NAME = os.getenv("BUCKET_NAME", "lakehouse").replace("\r", "").replace("\n", "").strip()

def get_s3_client(endpoint_url=None):
    target_endpoint = endpoint_url or MINIO_ENDPOINT
    if not target_endpoint.startswith("http://") and not target_endpoint.startswith("https://"):
        target_endpoint = f"http://{target_endpoint}"
        
    return boto3.client(
        "s3",
        endpoint_url=target_endpoint,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1"
    )

def ensure_bucket_exists(s3_client, bucket_name=BUCKET_NAME):
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except Exception:
        try:
            s3_client.create_bucket(Bucket=bucket_name)
            print(f"Bucket '{bucket_name}' created successfully.")
        except Exception as e:
            print(f"Bucket notice: {e}")

def upload_json_to_minio(data, s3_key, s3_client=None, bucket_name=BUCKET_NAME):
    if s3_client is None:
        s3_client = get_s3_client()
    
    ensure_bucket_exists(s3_client, bucket_name)
    
    json_bytes = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
    s3_client.put_object(
        Bucket=bucket_name,
        Key=s3_key,
        Body=io.BytesIO(json_bytes),
        ContentType="application/json"
    )
    print(f"Uploaded: s3://{bucket_name}/{s3_key}")