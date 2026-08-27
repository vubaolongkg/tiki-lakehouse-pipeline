import os
import sys
import datetime
import psycopg2
from pyspark.sql import functions as F
from spark_session_helper import get_spark_session

def get_postgres_connection_params():
    host = os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT", "5432")
    db_name = os.getenv("POSTGRES_DB")
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")

    if not all([host, db_name, user, password]):
        raise EnvironmentError("Missing PostgreSQL environment variables.")

    db_url = f"jdbc:postgresql://{host}:{port}/{db_name}"
    properties = {
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver"
    }
    return db_url, properties, host, port, db_name, user, password

def init_gold_tables(conn):
    """Đảm bảo các bảng và Primary Key Constraints tồn tại."""
    ddl_schema = """
    CREATE TABLE IF NOT EXISTS public.dim_categories (
        category_id BIGINT PRIMARY KEY,
        category_name TEXT,
        url TEXT
    );

    CREATE TABLE IF NOT EXISTS public.dim_products (
        product_id BIGINT PRIMARY KEY,
        product_name TEXT,
        brand TEXT,
        short_description TEXT,
        image_url TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS public.fact_daily_product_snapshot (
        snapshot_date DATE,
        product_id BIGINT,
        price DOUBLE PRECISION,
        original_price DOUBLE PRECISION,
        discount DOUBLE PRECISION,
        discount_rate DOUBLE PRECISION,
        rating_average DOUBLE PRECISION,
        review_count INT,
        inventory_status TEXT,
        price_segment TEXT
    );

    CREATE TABLE IF NOT EXISTS public.fact_reviews (
        review_id BIGINT PRIMARY KEY,
        product_id BIGINT,
        rating DOUBLE PRECISION,
        title TEXT,
        content TEXT,
        thank_count INT,
        comment_count INT,
        created_at TIMESTAMP
    );
    """
    with conn.cursor() as cursor:
        cursor.execute(ddl_schema)
        conn.commit()

def build_gold_dimensions(spark, conn, db_url, properties):
    print("\n--- [1/2] Building Gold Dimension Tables ---")
    
    # 1. Dim Categories (UPSERT bảo toàn Primary Key)
    df_cat = spark.read.format("delta").load("s3a://lakehouse/silver/categories")
    dim_categories = df_cat.select("category_id", "category_name", "url").dropDuplicates(["category_id"])
    
    dim_categories.write.jdbc(
        url=db_url, 
        table="staging_dim_categories", 
        mode="overwrite", 
        properties=properties
    )

    upsert_categories_sql = """
    INSERT INTO public.dim_categories (category_id, category_name, url)
    SELECT category_id, category_name, url FROM public.staging_dim_categories
    ON CONFLICT (category_id) DO UPDATE SET
        category_name = EXCLUDED.category_name,
        url = EXCLUDED.url;
    
    DROP TABLE IF EXISTS public.staging_dim_categories;
    """
    with conn.cursor() as cursor:
        cursor.execute(upsert_categories_sql)
        conn.commit()
    print("✅ Successfully UPSERTED: dim_categories (PK Preserved)")

    # 2. Dim Products (SCD Type 1 with Audit Timestamps)
    df_prod = spark.read.format("delta").load("s3a://lakehouse/silver/products")
    dim_products_staging = df_prod.select(
        "product_id",
        "product_name",
        "brand",
        "short_description",
        "image_url"
    ).dropDuplicates(["product_id"])

    dim_products_staging.write.jdbc(
        url=db_url,
        table="staging_dim_products",
        mode="overwrite",
        properties=properties
    )

    upsert_products_sql = """
    INSERT INTO public.dim_products (product_id, product_name, brand, short_description, image_url, created_at, updated_at)
    SELECT product_id, product_name, brand, short_description, image_url, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
    FROM public.staging_dim_products
    ON CONFLICT (product_id) DO UPDATE SET
        product_name = EXCLUDED.product_name,
        brand = EXCLUDED.brand,
        short_description = EXCLUDED.short_description,
        image_url = EXCLUDED.image_url,
        updated_at = CURRENT_TIMESTAMP;

    DROP TABLE IF EXISTS public.staging_dim_products;
    """
    with conn.cursor() as cursor:
        cursor.execute(upsert_products_sql)
        conn.commit()
    print("✅ Successfully UPSERTED: dim_products (Audit timestamps preserved)")

def build_gold_facts(spark, conn, run_date, db_url, properties):
    print(f"\n--- [2/2] Building Gold Fact Tables for Date: {run_date} ---")

    # 1. Fact Daily Product Snapshot (Idempotent Partition Handling)
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

    delete_partition_sql = "DELETE FROM public.fact_daily_product_snapshot WHERE snapshot_date = %s;"
    with conn.cursor() as cursor:
        cursor.execute(delete_partition_sql, (run_date,))
        conn.commit()

    fact_snapshot.write.jdbc(
        url=db_url,
        table="fact_daily_product_snapshot",
        mode="append",
        properties=properties
    )
    print(f"✅ Populated (Idempotent): fact_daily_product_snapshot for date {run_date}")

    # 2. Fact Reviews (Incremental UPSERT)
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
        table="staging_fact_reviews",
        mode="overwrite",
        properties=properties
    )

    upsert_reviews_sql = """
    INSERT INTO public.fact_reviews (review_id, product_id, rating, title, content, thank_count, comment_count, created_at)
    SELECT review_id, product_id, rating, title, content, thank_count, comment_count, created_at
    FROM public.staging_fact_reviews
    ON CONFLICT (review_id) DO UPDATE SET
        rating = EXCLUDED.rating,
        title = EXCLUDED.title,
        content = EXCLUDED.content,
        thank_count = EXCLUDED.thank_count,
        comment_count = EXCLUDED.comment_count;

    DROP TABLE IF EXISTS public.staging_fact_reviews;
    """
    with conn.cursor() as cursor:
        cursor.execute(upsert_reviews_sql)
        conn.commit()
    print("✅ Populated (Incremental Upsert): fact_reviews")

def main():
    run_date = sys.argv[1] if len(sys.argv) > 1 else datetime.date.today().isoformat()
    spark = get_spark_session(app_name="Tiki_Lakehouse_Gold")
    db_url, properties, host, port, db_name, user, password = get_postgres_connection_params()

    conn = psycopg2.connect(host=host, port=port, dbname=db_name, user=user, password=password)
    try:
        init_gold_tables(conn)
        build_gold_dimensions(spark, conn, db_url, properties)
        build_gold_facts(spark, conn, run_date, db_url, properties)
        print("\n🎉 Gold Layer modeling completed with uniform staging upserts & preserved constraints!")
    finally:
        conn.close()
        spark.stop()

if __name__ == "__main__":
    main()