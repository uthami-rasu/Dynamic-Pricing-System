import os 
from pyspark.sql.functions import col,from_json
from utils.connections import createOrGetSparkSession 
from consumer.schema import PRODUCT_SCHEMA 
from utils.custom_logging import Logger,createOrGetLogger
from pyspark.sql import DataFrame 
from dotenv import load_dotenv

# Load environment variables from a .env file to configure the application
load_dotenv() 

# Create or get an existing Spark session to interact with Spark
spark = createOrGetSparkSession() 

# Set up logging for tracking the application's streaming operations
logger = createOrGetLogger("Stream-Application") 

# Retrieve the Kafka server address from the environment variables
kafka_server = os.getenv("KAFKA_SERVERS")

def read_kafka_topic():
    """
    Reads data from a Kafka topic, processes it, and returns a transformed DataFrame.

    Steps:
        1. Connect to Kafka server and subscribe to the specified topic.
        2. Deserialize the Kafka message value using a pre-defined schema.
        3. Select relevant fields like product ID, price, stock, sales rate, etc.
        4. Return the transformed DataFrame for further processing.

    Returns:
        DataFrame: A transformed DataFrame containing the extracted product data.
    """
    global spark 
    kafka_topic = "demo_test"
    # Kafka stream
    product_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_server)
        .option("subscribe", kafka_topic)
        .load()
    )

    # Apply schema and perform transformations on the raw Kafka data
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

