
from pyspark.sql import SparkSession 
from utils.custom_logging import Logger

SPARK_SESSION = None 



# @Logger.log
def createOrGetSparkSession():
    global SPARK_SESSION 
    dependencies = 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0'
    if not SPARK_SESSION:
        SPARK_SESSION = SparkSession.builder.appName('DynamicPricing')\
                        .config('spark.jars.packages',dependencies)\
                        .getOrCreate()
    return SPARK_SESSION   

