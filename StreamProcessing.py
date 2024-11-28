
"""
Stream Processing Application for Kafka Data

This script implements a Spark Streaming application that:
- Reads data from a Kafka topic.
- Performs multiple transformations on the data.
- Writes the transformed data into MongoDB.

Workflow:
    1. Create a Spark session.
    2. Read data from the specified Kafka topic.
    3. Apply transformations (`transform_v1`, `transform_v2`, `price_refactoring`).
    4. Write the transformed data into MongoDB.
"""


from utils.connections import createOrGetSparkSession
from consumer.kafkastream import read_kafka_topic 
from consumer.transformation import transform_v1 , transform_v2 , price_refactoring 
from consumer.loading import write_into_console ,write_into_mongodb
from utils.custom_logging import Logger , createOrGetLogger
import json 


# Logger for the application
logger = createOrGetLogger("Stream-Application")

@Logger.log
def main():
    """
    Main function to orchestrate the Spark Streaming application.

    Steps:
        1. Create a Spark session using `createOrGetSparkSession`.
        2. Read streaming data from the Kafka topic using `read_kafka_topic`.
        3. Apply data transformations using `transform_v1`, `transform_v2`, and `price_refactoring`.
        4. Write the final transformed data into MongoDB using `write_into_mongodb`.

    Logs:
        - Logs progress and execution at each step, including errors.
    """

    logger.info("Spark Streaming Application has been started.")
    try:
        #Initialize Spark Session
        logger.debug("Spark Session initiated.")
        spark = createOrGetSparkSession() 

        # Read data from Kafka topic
        logger.debug("Read data from Kafka topic.")
        df = read_kafka_topic() 

        # Apply transformations
        logger.debug("Started executing transformations functions ")
        df_v1 = transform_v1(df)
        df_v2 = transform_v2(df_v1)
        df_v3 = price_refactoring(df_v2)

        #write_into_console(df_v3)

        # Write data to MongoDB
        logger.debug("Started write data to MongoDB")
        query = write_into_mongodb(df_v3)

        # Await termination of the query
        try:
            query.awaitTermination(30*60) #wait fpr 30 minutes
        finally:
            query.stop() 
            logger.debug("Stream Processing has been stopped.")
    except Exception as ex:
        print(ex)   
        logger.error(f"Error occurred during stream processing: {str(ex)}")
        raise 

if __name__ == "__main__":
    try:
        # Execute the main function
        main()
    except Exception as ex:
        logger.error(f"Unhandled exception: {str(ex)}")
    finally:
        # Dump logs for debugging and clear them
        logger.debug(json.dumps(Logger.logs, indent=4))
        Logger.clear_logs()