from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from time import sleep
from pymongo import MongoClient


import logging

def write_to_mongo(batch_df, batch_id):
    """
    Write batch DataFrame to MongoDB using pymongo.
    """
    mongo_options = {
        "host": "mongodb://localhost",
        "port": 27017,
        "database": "mydatabase",
        "collection": "messages"
    }

    try:
        # Extract the data from the DataFrame and decode bytearray to string
        records = batch_df.select("value").rdd.map(lambda r: r[0].decode('utf-8')).collect()

        # Create a MongoDB connection
        client = MongoClient(mongo_options["host"], mongo_options["port"])
        db = client[mongo_options["database"]]
        collection = db[mongo_options["collection"]]

        # Insert records into MongoDB
        for record in records:
            collection.insert_one({"message_content": record})

        # Close the MongoDB connection
        client.close()

    except Exception as e:
        logging.error(f"Failed to write batch {batch_id} to MongoDB: {e}")

def main():
    # Set the logging level and format
    logging.basicConfig(level=logging.ERROR, format="%(asctime)s %(levelname)s %(message)s")

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("KafkaMessageHandler") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:10.2.1") \
        .config("spark.mongodb.input.uri", "mongodb://localhost:27017/mydatabase.messages") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    # Read from Kafka
    kafka_bootstrap_servers = "localhost:9092"
    kafka_topic = "test-topic-python1"
    kafka_group_id = "my_consumer_group_id"
    
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic) \
        .option("kafka_group_id",kafka_group_id) \
        .load()
    print('df is done')
    # Define processing logic
    processed_df = df.withColumn("message_content", col("value").cast("string"))

    # Check if the message content contains "help"
    help_df = processed_df.filter("message_content like '%help%'")

    # Check if the message content does not contain "help"
    non_help_df = processed_df.filter("message_content not like '%help%'")

    # Define output sink for "help" messages
    kafka_output_topic_help = "test-topic-python2"
    checkpoint_location_help = "checkpoint_help"

    help_query = help_df.writeStream \
        .outputMode("append") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("topic", kafka_output_topic_help) \
        .option("checkpointLocation", checkpoint_location_help) \
        .option("failOnDataLoss", "false") \
        .start()
    print('help_query is done')
    # Define output sink for non-"help" messages
    checkpoint_location_non_help = "checkpoint_non_help"

    non_help_query = non_help_df.writeStream \
         .outputMode("append") \
         .foreachBatch(write_to_mongo) \
         .option("checkpointLocation", checkpoint_location_non_help) \
         .start()
    print('non_help_query is done')
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        logging.warning("Streaming terminated by user.")

    finally:
        try:
            spark.stop()
        except Exception as e:
            logging.error(f"Error during Spark shutdown: {str(e)}")

if __name__ == "__main__":
    sleep(30)
    main()
