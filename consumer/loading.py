

import os 
from dotenv import load_dotenv
from utils.connections import createOrGetSparkSession 
from pyspark.sql.window import Window 
from pyspark.sql.functions import * 
from utils.custom_logging import Logger ,createOrGetLogger


load_dotenv() 
spark = createOrGetSparkSession() 
logger = createOrGetLogger("Stream-Application")

def update_versioning(batch_df):
    windowSpec = Window.partitionBy('product_id').orderBy('arrival_time')
    existing_df = (spark.read.format('mongo')
                   .option('uri',os.getenv('MONGO_URI'))
                   .option('database','Products')
                   .option('collection','PricingHistory')
                   .load()) 
    
    if 'version' not in existing_df.columns:
        existing_df = existing_df.withColumn('version',lit(1))

    version_df = existing_df.select('product_id','version')\
                    .groupBy('product_id')\
                    .agg(
                        max('version').alias('max_version')
                    ).drop('_id')
    version_df.orderBy('product_id').join(batch_df,'product_id','left_semi').show(truncate=False)
    
    batch_df = batch_df.join(
        version_df, 
        'product_id',
        'left_outer'
    ).withColumn('version',
                 coalesce(col('max_version'),lit(0)) + row_number().over(windowSpec)).drop('max_version')
    # batch_df.show(truncate=False)
    return batch_df

def write_documents(batch_df,batch_id):
    print("Writing batch:",batch_id)

    batch_df = update_versioning(batch_df)
    batch_df.select(
        'product_id','version','competitor_price','current_price','new_price','refactor_price','date'
        ).show(truncate=False)
    logger.debug("Function 'write_into_mongodb' running. Writes data into mongodb")

    batch_df.write.format("mongo")\
        .option('uri',os.getenv('MONGO_URI'))\
        .option('database','Products')\
        .option('collection','PricingHistory')\
        .mode('append')\
        .save() 
    

def write_into_mongodb(df):
     
    return df.writeStream.foreachBatch(write_documents).start()




def printBatchData(batch_df, batch_id):
    print("Processing batch:", batch_id)
    #batch_df.printSchema() 
    batch_df.show(truncate=False)


def write_into_console(stream_df):

    query = stream_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .foreachBatch(printBatchData) \
    .start()

    print("Stream started...")
    query.awaitTermination() 


# print(os.getenv('MONGO_URI'))