import os
import io
import json
import boto3
from botocore.client import Config

# Nếu chạy trong Docker container, endpoint là http://minio:9000
# Nếu chạy ngoài máy Windows host, fallback về http://localhost:9000
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password123"
BUCKET_NAME = "lakehouse"

def get_s3_client(endpoint_url=None):
    if endpoint_url is None:
        endpoint_url = MINIO_ENDPOINT
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1"
    )

def ensure_bucket_exists(s3_client, bucket_name=BUCKET_NAME):
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except Exception:
        s3_client.create_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' created successfully.")

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