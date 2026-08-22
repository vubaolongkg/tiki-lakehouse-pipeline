from spark_session_helper import get_spark_session

spark = get_spark_session(app_name="Peek_Silver")

print("\n" + "="*50)
print("1. BẢNG CATEGORIES (SILVER)")
print("="*50)
df_cat = spark.read.format("delta").load("s3a://lakehouse/silver/categories")
df_cat.printSchema()
df_cat.show(5, truncate=False)

print("\n" + "="*50)
print("2. BẢNG PRODUCTS (SILVER)")
print("="*50)
df_prod = spark.read.format("delta").load("s3a://lakehouse/silver/products")
df_prod.printSchema()
df_prod.show(5, truncate=False)

print("\n" + "="*50)
print("3. BẢNG REVIEWS (SILVER)")
print("="*50)
df_rev = spark.read.format("delta").load("s3a://lakehouse/silver/reviews")
df_rev.printSchema()
df_rev.show(5, truncate=False)

spark.stop()