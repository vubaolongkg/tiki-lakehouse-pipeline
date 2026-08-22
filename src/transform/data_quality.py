import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

class DataQualityChecker:
    @staticmethod
    def check_not_null(df: DataFrame, column_name: str, table_name: str) -> bool:
        """Kiểm tra cột khóa không được chứa giá trị NULL"""
        null_count = df.filter(F.col(column_name).isNull()).count()
        if null_count > 0:
            logging.error(f"❌ [DQ FAIL] Table '{table_name}': Found {null_count} NULL values in primary column '{column_name}'!")
            return False
        logging.info(f"✅ [DQ PASS] Table '{table_name}': Column '{column_name}' has 0 NULLs.")
        return True

    @staticmethod
    def check_unique_primary_key(df: DataFrame, key_columns: list, table_name: str) -> bool:
        """Kiểm tra tính duy nhất của Primary Key (không có duplicate)"""
        total_rows = df.count()
        distinct_rows = df.select(key_columns).distinct().count()
        if total_rows != distinct_rows:
            diff = total_rows - distinct_rows
            logging.error(f"❌ [DQ FAIL] Table '{table_name}': Found {diff} duplicate rows on keys {key_columns}!")
            return False
        logging.info(f"✅ [DQ PASS] Table '{table_name}': Unique PK check passed ({total_rows} distinct rows).")
        return True

    @staticmethod
    def check_numeric_range(df: DataFrame, column_name: str, min_val: float, max_val: float, table_name: str) -> bool:
        """Kiểm tra giá trị số nằm trong khoảng hợp lệ"""
        out_of_bounds = df.filter((F.col(column_name) < min_val) | (F.col(column_name) > max_val)).count()
        if out_of_bounds > 0:
            logging.warning(f"⚠️ [DQ WARNING] Table '{table_name}': Found {out_of_bounds} records with '{column_name}' out of range [{min_val}, {max_val}].")
            return False
        logging.info(f"✅ [DQ PASS] Table '{table_name}': Column '{column_name}' within range [{min_val}, {max_val}].")
        return True

    @classmethod
    def validate_products_silver(cls, df: DataFrame) -> DataFrame:
        """Bộ kiểm định chất lượng cho bảng Products Silver"""
        logging.info("🔍 Running Data Quality Validations on Silver Products...")
        
        # 1. Primary key not null
        assert cls.check_not_null(df, "id", "silver_products"), "DQ Error: Product ID contains NULL"
        
        # 2. Price >= 0
        assert cls.check_numeric_range(df, "price", 0, 1_000_000_000, "silver_products"), "DQ Error: Invalid product price"
        
        # 3. Rating average in [0, 5]
        cls.check_numeric_range(df, "rating_average", 0.0, 5.0, "silver_products")
        
        return df

    @classmethod
    def validate_reviews_silver(cls, df: DataFrame) -> DataFrame:
        """Bộ kiểm định chất lượng cho bảng Reviews Silver"""
        logging.info("🔍 Running Data Quality Validations on Silver Reviews...")
        
        # 1. Review ID & Product ID not null
        assert cls.check_not_null(df, "id", "silver_reviews"), "DQ Error: Review ID contains NULL"
        assert cls.check_not_null(df, "product_id", "silver_reviews"), "DQ Error: Review Product ID contains NULL"
        
        # 2. Rating in [1, 5]
        cls.check_numeric_range(df, "rating", 1.0, 5.0, "silver_reviews")
        
        return df