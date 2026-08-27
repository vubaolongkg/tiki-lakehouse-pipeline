import sys
import logging
from datetime import datetime
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from spark_session_helper import get_spark_session
from data_quality import DataQualityChecker

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def transform_categories(spark, crawl_date):
    print(f"\n--- Transforming Silver: categories ({crawl_date}) ---")
    bronze_path = f"s3a://lakehouse/bronze/categories/crawl_date={crawl_date}/categories.json"
    silver_path = "s3a://lakehouse/silver/categories"

    try:
        df_raw = spark.read.option("multiline", True).json(bronze_path)
    except Exception as e:
        logging.warning(f"⚠️ Không thể đọc file bronze categories cho ngày {crawl_date}: {e}")
        return

    # Guard check dataset rỗng
    if df_raw.rdd.isEmpty() or len(df_raw.columns) == 0:
        logging.warning(f"⚠️ Bronze categories rỗng cho ngày {crawl_date}. Bỏ qua transform.")
        return

    id_col = F.col("category_id") if "category_id" in df_raw.columns else F.col("id")
    name_col = F.col("category_name") if "category_name" in df_raw.columns else F.col("name")

    df_mapped = df_raw.select(
        id_col.cast("bigint").alias("category_id"),
        name_col.cast("string").alias("category_name"),
        F.col("url").cast("string")
    )
    
    DataQualityChecker.validate_categories_silver(df_mapped)
    df_clean = df_mapped.dropDuplicates(["category_id"])

    if not DeltaTable.isDeltaTable(spark, silver_path):
        df_clean.write.format("delta").mode("overwrite").save(silver_path)
    else:
        delta_table = DeltaTable.forPath(spark, silver_path)
        delta_table.alias("target").merge(
            df_clean.alias("source"),
            "target.category_id = source.category_id"
        ).whenMatchedUpdate(set={
            "category_name": "source.category_name",
            "url": "source.url"
        }).whenNotMatchedInsertAll().execute()

def transform_products(spark, crawl_date):
    print(f"\n--- Transforming Silver: products ({crawl_date}) ---")
    bronze_path = f"s3a://lakehouse/bronze/products/crawl_date={crawl_date}/products.json"
    silver_path = "s3a://lakehouse/silver/products"

    try:
        df_raw = spark.read.option("multiline", True).json(bronze_path)
    except Exception as e:
        logging.warning(f"⚠️ Không thể đọc file bronze products cho ngày {crawl_date}: {e}")
        return

    if df_raw.rdd.isEmpty() or len(df_raw.columns) == 0:
        logging.warning(f"⚠️ Bronze products rỗng cho ngày {crawl_date}. Bỏ qua transform.")
        return

    has_inventory = "inventory_status" in df_raw.columns
    inv_col = F.coalesce(F.col("inventory_status").cast("string"), F.lit("available")) if has_inventory else F.lit("available")

    # Xử lý làm sạch Price: Nếu price < 0 hoặc NULL thì thay bằng 0.0
    raw_price = F.coalesce(F.col("price").cast("double"), F.lit(0.0))
    clean_price = F.when(raw_price < 0.0, F.lit(0.0)).otherwise(raw_price)

    discount_col = F.coalesce(F.col("discount").cast("double"), F.lit(0.0))
    clean_discount = F.when(discount_col < 0.0, F.lit(0.0)).otherwise(discount_col)

    orig_price_col = F.coalesce(F.col("original_price").cast("double"), clean_price + clean_discount)
    clean_orig_price = F.when(orig_price_col < 0.0, clean_price).otherwise(orig_price_col)

    df_mapped = df_raw.select(
        F.col("id").cast("bigint").alias("product_id"),
        F.coalesce(F.col("name").cast("string"), F.lit("Sản phẩm chưa cập nhật tên")).alias("product_name"),
        clean_price.alias("price"),
        clean_orig_price.alias("original_price"),
        clean_discount.alias("discount"),
        F.coalesce(F.col("rating_average").cast("double"), F.lit(0.0)).alias("rating_average"),
        F.coalesce(F.col("review_count").cast("int"), F.lit(0)).alias("review_count"),
        F.coalesce(F.col("brand"), F.lit("Không có thương hiệu")).alias("brand"),
        F.coalesce(F.col("short_description").cast("string"), F.lit("")).alias("short_description"),
        F.coalesce(F.col("thumbnail_url").cast("string"), F.lit("")).alias("image_url"),
        inv_col.alias("inventory_status")
    ).filter(F.col("product_id").isNotNull())

    # Data Quality Validation
    DataQualityChecker.validate_products_silver(df_mapped)
    df_clean = df_mapped.dropDuplicates(["product_id"])

    if not DeltaTable.isDeltaTable(spark, silver_path):
        df_clean.write.format("delta").mode("overwrite").save(silver_path)
    else:
        delta_table = DeltaTable.forPath(spark, silver_path)
        delta_table.alias("target").merge(
            df_clean.alias("source"),
            "target.product_id = source.product_id"
        ).whenMatchedUpdate(
            condition="source.product_name IS NOT NULL",
            set={
                "product_name": "source.product_name",
                "price": "source.price",
                "original_price": "source.original_price",
                "discount": "source.discount",
                "rating_average": "source.rating_average",
                "review_count": "source.review_count",
                "brand": "source.brand",
                "short_description": "source.short_description",
                "image_url": "source.image_url",
                "inventory_status": "source.inventory_status"
            }
        ).whenNotMatchedInsertAll().execute()

def transform_reviews(spark, crawl_date):
    print(f"\n--- Transforming Silver: reviews ({crawl_date}) ---")
    bronze_path = f"s3a://lakehouse/bronze/reviews/crawl_date={crawl_date}/reviews.json"
    silver_path = "s3a://lakehouse/silver/reviews"

    try:
        df_raw = spark.read.option("multiline", True).json(bronze_path)
    except Exception as e:
        logging.warning(f"⚠️ Không thể đọc file bronze reviews cho ngày {crawl_date}: {e}")
        return

    if df_raw.rdd.isEmpty() or len(df_raw.columns) == 0:
        logging.warning(f"⚠️ Bronze reviews rỗng cho ngày {crawl_date}. Bỏ qua transform.")
        return

    df_mapped = df_raw.select(
        F.col("id").cast("bigint").alias("review_id"),
        F.col("product_id").cast("bigint"),
        F.col("rating").cast("double"),
        F.col("title").cast("string"),
        F.col("content").cast("string"),
        F.coalesce(F.col("thank_count").cast("int"), F.lit(0)).alias("thank_count"),
        F.coalesce(F.col("comment_count").cast("int"), F.lit(0)).alias("comment_count"),
        # Set explicitly via spark.sql.session.timeZone = Asia/Ho_Chi_Minh
        F.to_timestamp(F.from_unixtime(F.col("created_at"))).alias("created_at")
    )

    DataQualityChecker.validate_reviews_silver(df_mapped)
    df_clean = df_mapped.dropDuplicates(["review_id"])

    if not DeltaTable.isDeltaTable(spark, silver_path):
        df_clean.write.format("delta").mode("overwrite").save(silver_path)
    else:
        delta_table = DeltaTable.forPath(spark, silver_path)
        delta_table.alias("target").merge(
            df_clean.alias("source"),
            "target.review_id = source.review_id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

def main():
    crawl_date = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")
    spark = get_spark_session(app_name="Tiki_Lakehouse_Silver")
    try:
        transform_categories(spark, crawl_date)
        transform_products(spark, crawl_date)
        transform_reviews(spark, crawl_date)
        print("\n🎉 Silver Layer Transformations & DQ Checks completed successfully!")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()