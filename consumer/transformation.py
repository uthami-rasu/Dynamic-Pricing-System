from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, dayofweek, to_date, when, lit, round
from utils.utility_functions import isHoliday
from utils.custom_logging import Logger,createOrGetLogger

logger = createOrGetLogger("Stream-Application")

def transform_v1(stream_df: DataFrame) -> DataFrame:

    # check the product_id has records in mongodb
    df = (
        stream_df#.withColumn("hasRecords", hasDocumentUdf(col("product_id")))
        .withColumn("isHoliday", isHoliday(col("date")))
        .withColumn(
            "isWeekend",
            when(dayofweek(to_date(col("date"))) == 1, True)
            .when(dayofweek(to_date(col("date"))) == 7, True)
            .otherwise(False),
        )
    )
    logger.debug("Function 'transform_v2' running and processing data from topic 'demo_test'.")


    return df

# @Logger.log
def transform_v2(stream_df: DataFrame) -> DataFrame:

    # set base values
    alpha = 0.6
    beta = 0.4
    gamma = 0.3
    _delta = 1.2

    stream_df = stream_df.withColumn("new_price", (
        when(col('isHoliday')=='true', (
            col("base_price")
            * (lit(1) + alpha * ((col("competitor_price") / col("current_price")) - lit(1))
            + beta * (lit(1) - (col("stock_level") / col("max_stock"))))
            + (gamma * col("sales_rate") * _delta * lit(10))
        ))
        .when(col('isWeekend')=='true', (
            col("base_price")
            * (lit(1) + alpha * ((col("competitor_price") / col("current_price")) - lit(1))
            + beta * (lit(1) - (col("stock_level") / col("max_stock"))))
            + (gamma * col("sales_rate") * _delta * lit(5))
        ))
        .otherwise(
            col("base_price")
            * (lit(1) + alpha * ((col("competitor_price") / col("current_price")) - lit(1))
            + beta * (lit(1) - (col("stock_level") / col("max_stock"))))
            + (gamma * col("sales_rate") * _delta * lit(3))
        )
    ))

    logger.debug("Function 'transform_v2' running and processing data from topic 'demo_test'.")
   
    return stream_df #.select('base_price','competitor_price','current_price','new_price')





# @Logger.log
def price_refactoring(df,margin=1.3):

    df = df.withColumn("refactor_price",when(
        col('new_price') < col('base_price'), 

        round(col('base_price') * margin ,2))
        .otherwise(round(col('new_price'),2))) 
    logger.debug("Function 'price_refactoring' running. Calculating refactored price for original price")

    return df 
