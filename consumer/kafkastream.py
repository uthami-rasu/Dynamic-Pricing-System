import os 
from pyspark.sql.functions import col,from_json
from utils.connections import createOrGetSparkSession 
from consumer.schema import PRODUCT_SCHEMA 
from utils.custom_logging import Logger,createOrGetLogger
from pyspark.sql import DataFrame 
from dotenv import load_dotenv

load_dotenv() 

spark = createOrGetSparkSession() 
logger = createOrGetLogger("Stream-Application") 
kafka_server = os.getenv("KAFKA_SERVERS")
def read_kafka_topic():
     
    global spark 
    kafka_topic = "demo_test"
    # Kafka stream
    product_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_server)
        .option("subscribe", kafka_topic)
        .load()
    )

    # Apply schema and transformation
    product_df = (
        product_df.selectExpr(
            "CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp",
        )
        .select(
            col("timestamp"), from_json(col("value"), PRODUCT_SCHEMA).alias("parsed_data")
        )
        .select(
            col("parsed_data.product_id"),
            col("parsed_data.base_price"),
            col("parsed_data.competitor_price"),
            col("parsed_data.current_price"),
            col("parsed_data.max_stock"),
            col("parsed_data.sales_rate"),
            col("parsed_data.stock_level"),
            col("parsed_data.date"),
            col("timestamp").alias("arrival_time"),
        )
        
    )
    logger.debug("Executing 'read_kafka_topic' function for topic '%s'.", kafka_topic)


    return product_df

