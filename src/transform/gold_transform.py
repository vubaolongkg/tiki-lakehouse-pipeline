import os
import sys
import datetime
from pyspark.sql import functions as F
from spark_session_helper import get_spark_session

def get_postgres_config():
    db_host = os.getenv("POSTGRES_HOST", "lakehouse_postgres")
    db_port = os.getenv("POSTGRES_PORT", "5432")
    db_name = os.getenv("POSTGRES_DB", "tiki_mart")
    db_user = os.getenv("POSTGRES_USER", "postgres")
    db_password = os.getenv("POSTGRES_PASSWORD", "password123")
    
    db_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"
    
    properties = {
        "user": db_user,
        "password": db_password,
        "driver": "org.postgresql.Driver"
    }
    return db_url, properties

def build_gold_dimensions(spark, db_url, properties):
    print("\n--- [1/2] Building Gold Dimension Tables ---")
    
    # 1. Dim Categories
    df_cat = spark.read.format("delta").load("s3a://lakehouse/silver/categories")
    dim_categories = df_cat.select(
        "category_id",
        "category_name",
        "url"
    ).dropDuplicates(["category_id"])
    
    dim_categories.write.jdbc(
        url=db_url,
        table="dim_categories",
        mode="overwrite",
        properties=properties
    )
    print("✅ Populated: dim_categories")

    # 2. Dim Products with Audit Metadata (created_at, updated_at)
    df_prod = spark.read.format("delta").load("s3a://lakehouse/silver/products")
    curr_ts = F.current_timestamp()
    
    dim_products = df_prod.select(
        "product_id",
        F.col("name").alias("product_name"),
        F.coalesce(F.col("brand"), F.lit("Không có thương hiệu")).alias("brand"),
        "short_description",
        F.col("thumbnail_url").alias("image_url"),
        curr_ts.alias("created_at"),
        curr_ts.alias("updated_at")
    ).dropDuplicates(["product_id"])

    dim_products.write.jdbc(
        url=db_url,
        table="dim_products",
        mode="overwrite",
        properties=properties
    )
    print("✅ Populated: dim_products (with audit timestamps)")

def build_gold_facts(spark, run_date, db_url, properties):
    print(f"\n--- [2/2] Building Gold Fact Tables for Date: {run_date} ---")

    # 1. Fact Daily Product Snapshot
    df_prod = spark.read.format("delta").load("s3a://lakehouse/silver/products")
    
    fact_snapshot = (
        df_prod
        .withColumn("snapshot_date", F.to_date(F.lit(run_date)))
        .withColumn(
            "discount_rate",
            F.when(
                (F.col("original_price") > 0) & (F.col("discount") > 0), 
                F.round(F.col("discount") / F.col("original_price"), 4)
            ).otherwise(0.0)
        )
        .withColumn(
            "price_segment",
            F.when(F.col("price") < 100000, "0 - 100K")
             .when((F.col("price") >= 100000) & (F.col("price") < 500000), "100K - 500K")
             .when((F.col("price") >= 500000) & (F.col("price") < 1000000), "500K - 1M")
             .when((F.col("price") >= 1000000) & (F.col("price") < 5000000), "1M - 5M")
             .otherwise("Trên 5M")
        )
        .select(
            "snapshot_date",
            "product_id",
            "price",
            "original_price",
            "discount",
            "discount_rate",
            "rating_average",
            "review_count",
            "inventory_status",
            "price_segment"
        )
    )

    fact_snapshot.write.jdbc(
        url=db_url,
        table="fact_daily_product_snapshot",
        mode="append",
        properties=properties
    )
    print("✅ Populated: fact_daily_product_snapshot")

    # 2. Fact Reviews
    df_rev = spark.read.format("delta").load("s3a://lakehouse/silver/reviews")
    fact_reviews = df_rev.select(
        "review_id",
        "product_id",
        "rating",
        "title",
        "content",
        "thank_count",
        "comment_count",
        "created_at"
    ).dropDuplicates(["review_id"])

    fact_reviews.write.jdbc(
        url=db_url,
        table="fact_reviews",
        mode="overwrite",
        properties=properties
    )
    print("✅ Populated: fact_reviews")

def main():
    run_date = sys.argv[1] if len(sys.argv) > 1 else datetime.date.today().isoformat()
    print(f"Starting Gold Layer Modeling for Date: {run_date}")
    
    spark = get_spark_session(app_name="Tiki_Lakehouse_Gold")
    db_url, properties = get_postgres_config()

    try:
        build_gold_dimensions(spark, db_url, properties)
        build_gold_facts(spark, run_date, db_url, properties)
        print("\n🎉 Gold Layer modeling and PostgreSQL serving finished successfully!")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()