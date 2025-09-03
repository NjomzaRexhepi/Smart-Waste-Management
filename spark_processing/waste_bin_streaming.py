import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import substring, from_json, col, to_timestamp
from pyspark.sql.types import StructType, StringType

# --- 1. Hadoop setup (Windows specific) ---
os.environ["HADOOP_HOME"] = "C:\\Program Files\\hadoop\\hadoop-3.3.6"
os.environ["PATH"] += ";C:\\Program Files\\hadoop\\hadoop-3.3.6\\bin"

# --- 2. Create SparkSession with Kafka + Cassandra support ---
spark = SparkSession.builder \
    .appName("WasteBinStreamingConsumer") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,"
            "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1") \
    .config("spark.cassandra.connection.host", "127.0.0.1") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# --- 3. Define schema for JSON messages ---
schema = StructType() \
    .add("timestamp", StringType()) \
    .add("bin_id", StringType()) \
    .add("sensor_type", StringType()) \
    .add("measurement", StringType())

# --- 4. Read from Kafka ---
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "waste-sensor-data") \
    .option("startingOffsets", "latest") \
    .load()

json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")

parsed_df = json_df.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

# --- 7. Transform fields ---
# Convert timestamp safely -> Spark timestamp type
# FIXED: Use to_timestamp with proper format string literal
transformed_df = parsed_df \
    .withColumn("ts_trimmed", substring(col("timestamp"), 1, 23)) \
    .withColumn("event_time", to_timestamp(col("ts_trimmed"), "yyyy-MM-dd'T'HH:mm:ss.SSS")) \
    .drop("timestamp", "ts_trimmed")

# Optional: filter out invalid timestamps
clean_df = transformed_df.filter(col("event_time").isNotNull())

# Filter example: only ultrasonic data
filtered_df = clean_df.filter(col("sensor_type") == "ultrasonic")

# --- 8. Define function to write to Cassandra ---
def write_to_cassandra(batch_df, batch_id):
    batch_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(keyspace="wastebin", table="sensor_data") \
        .save()

# --- 9. Start streaming query ---
query = filtered_df.writeStream \
    .foreachBatch(write_to_cassandra) \
    .outputMode("append") \
    .start()

query.awaitTermination()