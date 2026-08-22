import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("Tiki_Unit_Tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

def test_discount_rate_calculation(spark):
    schema = StructType([
        StructField("price", DoubleType(), True),
        StructField("original_price", DoubleType(), True),
        StructField("discount", DoubleType(), True)
    ])
    data = [
        (80000.0, 100000.0, 20000.0),
        (50000.0, 0.0, 0.0),
        (100000.0, 100000.0, 0.0)
    ]
    df = spark.createDataFrame(data, schema)
    
    df_transformed = df.withColumn(
        "discount_rate",
        F.when(
            (F.col("original_price") > 0) & (F.col("discount") > 0),
            F.round(F.col("discount") / F.col("original_price"), 4)
        ).otherwise(0.0)
    )
    
    results = [row.discount_rate for row in df_transformed.collect()]
    assert results == [0.2, 0.0, 0.0]

def test_price_segmentation_logic(spark):
    schema = StructType([
        StructField("price", DoubleType(), True)
    ])
    data = [(50000.0,), (250000.0,), (750000.0,), (2000000.0,), (10000000.0,)]
    df = spark.createDataFrame(data, schema)
    
    df_segmented = df.withColumn(
        "price_segment",
        F.when(F.col("price") < 100000, "0 - 100K")
         .when((F.col("price") >= 100000) & (F.col("price") < 500000), "100K - 500K")
         .when((F.col("price") >= 500000) & (F.col("price") < 1000000), "500K - 1M")
         .when((F.col("price") >= 1000000) & (F.col("price") < 5000000), "1M - 5M")
         .otherwise("Trên 5M")
    )
    
    segments = [row.price_segment for row in df_segmented.collect()]
    assert segments == ["0 - 100K", "100K - 500K", "500K - 1M", "1M - 5M", "Trên 5M"]