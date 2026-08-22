import sys
import datetime
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from spark_session_helper import get_spark_session
from data_quality import DataQualityChecker


def transform_categories(spark, crawl_date):
    print("\n--- Transforming Silver: categories ---")
    bronze_path = f"s3a://lakehouse/bronze/categories/crawl_date={crawl_date}/categories.json"
    silver_path = "s3a://lakehouse/silver/categories"

    df_raw = spark.read.option("multiline", True).json(bronze_path)
    
    df_clean = (
        df_raw.select(
            F.col("category_id").cast("bigint"),
            F.col("category_name").cast("string"),
            F.col("url").cast("string"),
            F.lit(crawl_date).alias("updated_at")
        )
        .filter(F.col("category_id").isNotNull())
        .dropDuplicates(["category_id"])
    )

    # Upsert vào Delta Table
    if DeltaTable.isDeltaTable(spark, silver_path):
        delta_table = DeltaTable.forPath(spark, silver_path)
        (
            delta_table.alias("target")
            .merge(
                df_clean.alias("source"),
                "target.category_id = source.category_id"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        print("Upserted categories into existing Delta table.")
    else:
        df_clean.write.format("delta").mode("overwrite").save(silver_path)
        print("Initialized categories Delta table.")

def transform_products(spark, crawl_date):
    print("\n--- Transforming Silver: products ---")
    bronze_path = f"s3a://lakehouse/bronze/products/crawl_date={crawl_date}/products.json"
    silver_path = "s3a://lakehouse/silver/products"

    df_raw = spark.read.option("multiline", True).json(bronze_path)

    # Logic làm sạch dữ liệu (df_clean)
    df_clean = df_raw.select(
        F.col("id").cast("bigint"),
        F.col("name").cast("string").alias("product_name"),
        F.col("price").cast("double"),
        F.col("original_price").cast("double"),
        F.col("discount").cast("double"),
        F.col("rating_average").cast("double"),
        F.col("review_count").cast("int"),
        F.col("brand").cast("string"),
        F.col("short_description").cast("string"),
        F.col("description").cast("string")
    ).dropDuplicates(["id"])

    # 1. Chạy Data Quality Check tại đây:
    df_clean = DataQualityChecker.validate_products_silver(df_clean)

    # 2. Tiến hành ghi/MERGE vào Delta Lake Silver:
    if not DeltaTable.isDeltaTable(spark, silver_path):
        df_clean.write.format("delta").mode("overwrite").save(silver_path)
    else:
        delta_table = DeltaTable.forPath(spark, silver_path)
        delta_table.alias("target").merge(
            df_clean.alias("source"),
            "target.id = source.id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    print(f"Products transformed and merged successfully.")

def transform_reviews(spark, crawl_date):
    print("\n--- Transforming Silver: reviews ---")
    bronze_path = f"s3a://lakehouse/bronze/reviews/crawl_date={crawl_date}/reviews.json"
    silver_path = "s3a://lakehouse/silver/reviews"

    df_raw = spark.read.option("multiline", True).json(bronze_path)

    df_clean = df_raw.select(
        F.col("id").cast("bigint"),
        F.col("product_id").cast("bigint"),
        F.col("rating").cast("double"),
        F.col("title").cast("string"),
        F.col("content").cast("string"),
        F.col("thank_count").cast("int"),
        F.col("comment_count").cast("int"),
        F.to_timestamp(F.from_unixtime(F.col("created_at"))).alias("created_at")
    ).dropDuplicates(["id"])

    # 1. Chạy Data Quality Check cho Reviews:
    df_clean = DataQualityChecker.validate_reviews_silver(df_clean)

    # 2. Ghi/MERGE vào Delta Lake Silver:
    if not DeltaTable.isDeltaTable(spark, silver_path):
        df_clean.write.format("delta").mode("overwrite").save(silver_path)
    else:
        delta_table = DeltaTable.forPath(spark, silver_path)
        delta_table.alias("target").merge(
            df_clean.alias("source"),
            "target.id = source.id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    print(f"Reviews transformed and merged successfully.")

def main():
    crawl_date = sys.argv[1] if len(sys.argv) > 1 else datetime.date.today().isoformat()
    print(f"Starting Silver Transformation for Date: {crawl_date}")
    
    spark = get_spark_session()
    
    try:
        transform_categories(spark, crawl_date)
        transform_products(spark, crawl_date)
        transform_reviews(spark, crawl_date)
        print("\n✅ Silver transformation completed successfully!")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()