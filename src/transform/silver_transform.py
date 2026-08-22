import sys
import datetime
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from spark_session_helper import get_spark_session

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

    df_clean = (
        df_raw.select(
            F.col("id").cast("bigint").alias("product_id"),
            F.col("name").cast("string"),
            F.coalesce(F.col("price").cast("double"), F.lit(0.0)).alias("price"),
            F.coalesce(F.col("original_price").cast("double"), F.lit(0.0)).alias("original_price"),
            F.coalesce(F.col("discount").cast("double"), F.lit(0.0)).alias("discount"),
            F.coalesce(F.col("rating_average").cast("double"), F.lit(0.0)).alias("rating_average"),
            F.coalesce(F.col("review_count").cast("int"), F.lit(0)).alias("review_count"),
            F.coalesce(F.col("brand"), F.lit("Không có thương hiệu")).alias("brand"),
            F.coalesce(F.col("inventory_status"), F.lit("Không rõ")).alias("inventory_status"),
            F.col("short_description").cast("string"),
            F.col("thumbnail_url").cast("string"),
            F.lit(crawl_date).alias("updated_at")
        )
        .filter(F.col("product_id").isNotNull())
        .dropDuplicates(["product_id"])
    )

    # Upsert vào Delta Table
    if DeltaTable.isDeltaTable(spark, silver_path):
        delta_table = DeltaTable.forPath(spark, silver_path)
        (
            delta_table.alias("target")
            .merge(
                df_clean.alias("source"),
                "target.product_id = source.product_id"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        print("Upserted products into existing Delta table.")
    else:
        df_clean.write.format("delta").mode("overwrite").save(silver_path)
        print("Initialized products Delta table.")

def transform_reviews(spark, crawl_date):
    print("\n--- Transforming Silver: reviews ---")
    bronze_path = f"s3a://lakehouse/bronze/reviews/crawl_date={crawl_date}/reviews.json"
    silver_path = "s3a://lakehouse/silver/reviews"

    df_raw = spark.read.option("multiline", True).json(bronze_path)

    df_clean = (
        df_raw.select(
            F.col("id").cast("bigint").alias("review_id"),
            F.col("product_id").cast("bigint"),
            F.coalesce(F.col("rating").cast("int"), F.lit(5)).alias("rating"),
            F.col("title").cast("string"),
            F.col("content").cast("string"),
            F.coalesce(F.col("thank_count").cast("int"), F.lit(0)).alias("thank_count"),
            F.coalesce(F.col("comment_count").cast("int"), F.lit(0)).alias("comment_count"),
            F.when(F.col("created_at").isNotNull(),
                   F.from_unixtime(F.col("created_at")).cast("timestamp")
            ).alias("created_at"),
            F.lit(crawl_date).alias("ingest_date")
        )
        .filter(F.col("review_id").isNotNull())
        .dropDuplicates(["review_id"])
    )

    # Upsert vào Delta Table
    if DeltaTable.isDeltaTable(spark, silver_path):
        delta_table = DeltaTable.forPath(spark, silver_path)
        (
            delta_table.alias("target")
            .merge(
                df_clean.alias("source"),
                "target.review_id = source.review_id"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        print("Upserted reviews into existing Delta table.")
    else:
        df_clean.write.format("delta").mode("overwrite").save(silver_path)
        print("Initialized reviews Delta table.")

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