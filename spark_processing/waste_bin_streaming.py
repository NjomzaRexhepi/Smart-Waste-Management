from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

# Create Spark session with Windows-specific configurations
spark = SparkSession.builder \
    .appName("SmartWasteManagement") \
    .config("spark.sql.streaming.checkpointLocation", "C:/tmp/spark_checkpoints/my_stream") \
    .config("spark.hadoop.io.native.lib.available", "false") \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

# Set log level to reduce noise
spark.sparkContext.setLogLevel("WARN")

# Define schema for the incoming JSON data
schema = StructType([
    StructField("bin_id", StringType(), True),
    StructField("measurement", StringType(), True)
])

try:
    # Read from Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "waste-sensor-data") \
        .option("startingOffsets", "latest") \
        .load()

    # Parse JSON data
    parsed_df = df.select(
        col("value").cast("string").alias("json_str")
    ).filter(
        col("json_str").isNotNull()
    ).select(
        from_json(col("json_str"), schema).alias("data")
    ).select(
        col("data.bin_id").alias("bin_id"),
        col("data.measurement").alias("measurement")
    )

    # Create checkpoint directory if it doesn't exist
    checkpoint_dir = "C:/tmp/spark_checkpoints/my_stream"
    os.makedirs(checkpoint_dir, exist_ok=True)

    # Write to console with proper error handling
    query = parsed_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .option("checkpointLocation", checkpoint_dir) \
        .trigger(processingTime='10 seconds') \
        .start()

    print("Streaming query started successfully!")
    query.awaitTermination()

except Exception as e:
    print(f"Error occurred: {str(e)}")
    print("Stopping Spark session...")
    spark.stop()
    raise e