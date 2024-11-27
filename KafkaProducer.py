
from kafka import KafkaProducer
import json
import time
from datetime import datetime 
from utils.custom_logging import createOrGetLogger,Logger
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


@Logger.log
def StartKafkaProducer():
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
    StartKafkaProducer()
except Exception as ex:
    logger.debug(str(ex))
finally:
    logger.debug("Kafka Producer has been stopped..")
    logger.debug(Logger.logs) 
    Logger.clear_logs() 
    
     