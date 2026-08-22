import os
from pyspark.sql import SparkSession

def get_spark_session(app_name="Tiki_Lakehouse_Gold"):
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    minio_access_key = os.getenv("MINIO_ACCESS_KEY", "admin")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY", "password123")

    packages = [
        "io.delta:delta-spark_2.12:3.1.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        "org.postgresql:postgresql:42.6.0"
    ]

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "1g")
    )
    
    return builder.getOrCreate()