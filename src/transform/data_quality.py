import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

class DataQualityChecker:
    @staticmethod
    def check_not_null(df: DataFrame, column_name: str, table_name: str) -> bool:
        """Kiểm tra cột không được chứa giá trị NULL."""
        null_count = df.filter(F.col(column_name).isNull()).count()
        if null_count > 0:
            logging.error(f"❌ [DQ FAIL] Table '{table_name}': Found {null_count} NULL values in '{column_name}'!")
            return False
        logging.info(f"✅ [DQ PASS] Table '{table_name}': Column '{column_name}' has 0 NULLs.")
        return True

    @staticmethod
    def check_unique_primary_key(df: DataFrame, key_columns: list, table_name: str) -> bool:
        """Kiểm tra tính duy nhất của tập khóa chính."""
        total_rows = df.count()
        distinct_rows = df.select(key_columns).distinct().count()
        if total_rows != distinct_rows:
            diff = total_rows - distinct_rows
            logging.warning(f"⚠️ [DQ WARNING] Table '{table_name}': Found {diff} duplicate rows on keys {key_columns}. Will be deduped.")
            return False
        logging.info(f"✅ [DQ PASS] Table '{table_name}': 100% unique primary keys ({total_rows} rows).")
        return True

    @staticmethod
    def check_numeric_range(df: DataFrame, column_name: str, min_val: float, max_val: float, table_name: str, allow_null: bool = False) -> bool:
        """
        Kiểm tra dải giá trị số. 
        Bắt cả trường hợp NULL (do cast fail hoặc raw null) nếu allow_null=False.
        """
        condition = (F.col(column_name) < min_val) | (F.col(column_name) > max_val)
        if not allow_null:
            condition = condition | F.col(column_name).isNull()

        invalid_count = df.filter(condition).count()
        if invalid_count > 0:
            logging.warning(f"⚠️ [DQ WARNING] Table '{table_name}': Found {invalid_count} records where '{column_name}' is out of range [{min_val}, {max_val}] or NULL.")
            return False
        logging.info(f"✅ [DQ PASS] Table '{table_name}': Column '{column_name}' valid in range [{min_val}, {max_val}].")
        return True

    @classmethod
    def validate_categories_silver(cls, df: DataFrame) -> DataFrame:
        logging.info("🔍 Running DQ Validations on Categories Silver...")
        cls.check_unique_primary_key(df, ["category_id"], "silver_categories")
        if not cls.check_not_null(df, "category_id", "silver_categories"):
            raise ValueError("DQ Hard Failure: category_id contains NULL values.")
        if not cls.check_not_null(df, "category_name", "silver_categories"):
            raise ValueError("DQ Hard Failure: category_name contains NULL values.")
        return df


    @classmethod
    def validate_reviews_silver(cls, df: DataFrame) -> DataFrame:
        logging.info("🔍 Running DQ Validations on Reviews Silver (df_mapped)...")
        cls.check_unique_primary_key(df, ["review_id"], "silver_reviews")
        if not cls.check_not_null(df, "review_id", "silver_reviews"):
            raise ValueError("DQ Hard Failure: review_id contains NULL values.")
        if not cls.check_not_null(df, "product_id", "silver_reviews"):
            raise ValueError("DQ Hard Failure: product_id in reviews contains NULL values.")
        
        # Rating bắt buộc từ 1.0 đến 5.0
        cls.check_numeric_range(df, "rating", 1.0, 5.0, "silver_reviews", allow_null=False)
        return df

    @staticmethod
    def validate_products_silver(df):
        logging.info("🔍 Running DQ Validations on Products Silver...")
        total_rows = df.count()
        if total_rows == 0:
            return

        # 1. Primary Key Uniqueness
        distinct_pks = df.select("product_id").distinct().count()
        if distinct_pks == total_rows:
            logging.info(f"✅ [DQ PASS] Table 'silver_products': 100% unique primary keys ({total_rows} rows).")
        else:
            logging.warning(f"⚠️ [DQ WARNING] Found {total_rows - distinct_pks} duplicate PKs in products.")

        # 2. Non-null constraints
        for col_name in ["product_id", "product_name", "price"]:
            null_count = df.filter(F.col(col_name).isNull()).count()
            if null_count == 0:
                logging.info(f"✅ [DQ PASS] Table 'silver_products': Column '{col_name}' has 0 NULLs.")
            else:
                logging.warning(f"⚠️ [DQ WARNING] Column '{col_name}' has {null_count} NULL values.")

        # 3. Range check on price
        out_of_range = df.filter((F.col("price") < 0.0) | (F.col("price") > 1_000_000_000.0)).count()
        if out_of_range == 0:
            logging.info("✅ [DQ PASS] Table 'silver_products': All prices within valid range [0, 1B VND].")
        else:
            logging.warning(f"⚠️ [DQ WARNING] Found {out_of_range} records with out-of-range price.")