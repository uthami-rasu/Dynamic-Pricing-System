
from utils.connections import createOrGetSparkSession
from consumer.kafkastream import read_kafka_topic 
from consumer.transformation import transform_v1 , transform_v2 , price_refactoring 
from consumer.loading import write_into_console ,write_into_mongodb
from utils.custom_logging import Logger , createOrGetLogger
import json 



logger = createOrGetLogger("Stream-Application")

@Logger.log
def main():

    logger.info("Spark Streaming has been Started..")
    try:
        logger.debug("Spark Session initiated.")

        spark = createOrGetSparkSession() 

        logger.debug("Reading data from Kafka-Topic")

        df = read_kafka_topic() 

        logger.debug("Started executing transformations functions ")

        df_v1 = transform_v1(df)

        df_v2 = transform_v2(df_v1)

        df_v3 = price_refactoring(df_v2)

        #write_into_console(df_v3)

        logger.debug("Started write data to MongoDB")

        query = write_into_mongodb(df_v3)

        try:
            query.awaitTermination(30*60)
        finally:
            query.stop() 
            logger.debug("Stream Processing has been stopped..")
    except Exception as ex:
        print(ex)   
        logger.error(str(ex))
        raise 

try:
    main() 
except Exception as ex:
    logger.error(str(ex))
finally:
    logger.debug(json.dumps(Logger.logs))
    Logger.clear_logs()

