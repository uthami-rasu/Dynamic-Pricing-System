
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType , IntegerType ,DateType 
    )

PRODUCT_SCHEMA = StructType(
    [
        StructField("product_id", StringType()),
        StructField("base_price", FloatType()),
        StructField("competitor_price", FloatType()),
        StructField("current_price", FloatType()),
        StructField("max_stock", IntegerType()),
        StructField("sales_rate", FloatType()),
        StructField("stock_level", IntegerType()),
        StructField("date", DateType()),
    ]
)