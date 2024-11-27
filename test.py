# from pyspark.sql import SparkSession 


# spark = (SparkSession.builder.appName("Test")
#          .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1")
#          .getOrCreate()
# )

# df = spark.read.format('com.mongodb.spark.sql.DefaultSource')\
#         .option('uri',mongodb_uri)\
#         .option('database','Test')\
#         .option('collection','Users')\
#         .load() 

# df.show() 


# from utils.custom_logging import log



# @log 
# def add(a):
#     print(a) 



# @log 
# def sub(b):
#     print(b) 


# add(2) 
# sub(3)

# print(log.logs)

# import os 

# from dotenv import load_dotenv

# load_dotenv() 


# data = [(1,2)]

# from utils.connections import createOrGetSparkSession 


# spark = createOrGetSparkSession() 

# import os 
# df = spark.createDataFrame(data,['cd','b'])

# df.write.format('mongo').option('uri',os.getenv('MONGO_URI')).option('database','Test')\
#     .option('collection','Users').mode('append').save() 

# from pymongo.mongo_client import MongoClient
# from pymongo.server_api import ServerApi

# uri = "mongodb+srv://razz-programmer:somepass@mongodb-cluster.zfhc0.mongodb.net/?retryWrites=true&w=majority&appName=mongodb-cluster"

# # Create a new client and connect to the server
# client = MongoClient(uri, server_api=ServerApi('1'))

# # Send a ping to confirm a successful connection
# try:
#     client.admin.command('ping')
#     print("Pinged your deployment. You successfully connected to MongoDB!")
# except Exception as e:
#     print(e)

# from utils.custom_logging import Logger ,createOrGetLogger 
# import time 
# import json 
# logger = createOrGetLogger("TestingLogger") 

# @Logger.log 
# def add():
#     print("hi")
#     time.sleep(5)
# add() 
# add() 
# logger.debug("Hello World!")
# print(Logger.logs)
# logger.debug(json.dumps(Logger.logs))



from kafka import KafkaProducer
import json
import time
from datetime import datetime 
from utils.custom_logging import createOrGetLogger
import random

start, end = 0, 5


def get_product_id(pid):
    return f"P{pid:04d}"


def get_random_price(base_price):
    base_prices = [base_price * ((i + 7.7) / 10) for i in range(1, 11)]
    return round(random.choice(base_prices), 2)


def get_random_stock_level(max_stock):

    stock_level = [(max_stock * i) / 100 for i in range(10, 71, 10)]
    return int(random.choice(stock_level))

def get_random_sales_rate(max_stock,stock_left):
    unit_sold = max_stock - stock_left 
    time_period = random.uniform(0.5,5)
    return round(unit_sold / time_period,2)

def start_produce():
    producer = KafkaProducer(
        bootstrap_servers=["54.224.3.177:9092"],
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )
    product = {}
    for count in range(start, end):
        product["product_id"] = get_product_id(count)
        product["base_price"] = random.randrange(400, 5000, 450) * 1.3
        product["competitor_price"] = get_random_price(product.get("base_price", 0))
        product["current_price"] = get_random_price(product.get("base_price", 0))
        product["max_stock"] = random.randrange(100, 1000, 50)
        product["stock_level"] = get_random_stock_level(product.get("max_stock", 100))
        product["sales_rate"] = get_random_sales_rate(
            product.get("max_stock", 100), product.get("stock_level", 50)
        )
        product["date"] = datetime.now().strftime("%Y-%m-%d")

        print(count, "\n", product)
        producer.send("demo_test", product)
        time.sleep(7)
    producer.close()

logger = createOrGetLogger("KafkaProducer")

try:
    logger.debug("Kafka Producers Starts Execution")
    start_produce()
except Exception as ex:
    logger.debug(str(ex))
finally:
    logger.debug("Kafka Producer has been stopped..")
    
     