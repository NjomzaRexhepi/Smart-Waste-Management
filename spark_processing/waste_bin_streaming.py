import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, BooleanType, TimestampType

os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] += ";C:\\hadoop\\bin"
# 1. Create SparkSession with Kafka support
spark = SparkSession.builder \
    .appName("WasteBinStreamingConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Define schema for JSON messages
schema = StructType() \
    .add("timestamp", StringType()) \
    .add("bin_id", StringType()) \
    .add("sensor_type", StringType()) \
    .add("measurement", StringType())  # use StringType initially for flexibility

# 3. Read from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "waste-sensor-data") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Convert Kafka value (binary) to string
json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")

# 5. Parse the JSON
parsed_df = json_df.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

# 6. Simple transformation or filter (e.g., only ultrasonic data)
filtered_df = parsed_df.filter(col("sensor_type") == "ultrasonic")

# 7. Output to console (or HDFS, DB, etc.)
query = filtered_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
