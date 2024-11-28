"""
Utility for Creating or Retrieving a Spark Session

This module provides a function to create or retrieve a global Spark session instance. 
It ensures that the Spark session is initialized only once, avoiding redundant creations.

Features:
    - Supports the MongoDB Spark Connector for integration with MongoDB.
    - Includes Kafka support for streaming data processing.
"""

from pyspark.sql import SparkSession
from utils.custom_logging import Logger

# Global variable to hold the Spark session instance
SPARK_SESSION = None

def createOrGetSparkSession() -> SparkSession:
    """
    Creates or retrieves a Spark session with pre-configured dependencies.

    Dependencies:
        - MongoDB Spark Connector (org.mongodb.spark:mongo-spark-connector_2.12:3.0.1)
        - Kafka Connector for Spark Streaming (org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0)

    Returns:
        SparkSession: The Spark session instance with required configurations.
    """
    global SPARK_SESSION
    dependencies = (
        "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,"
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0"
    )

    if not SPARK_SESSION:
        # Create a new Spark session if it doesn't already exist
        SPARK_SESSION = SparkSession.builder \
            .appName("DynamicPricing") \
            .config("spark.jars.packages", dependencies) \
            .getOrCreate()

    return SPARK_SESSION
