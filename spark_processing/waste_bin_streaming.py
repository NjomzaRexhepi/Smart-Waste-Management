import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType

os.environ["HADOOP_HOME"] = "C:\\hadoop-3.3.6"
os.environ["PATH"] += ";C:\\hadoop-3.3.6\\bin"

spark = SparkSession.builder \
    .appName("WasteBinStreamingConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType() \
    .add("timestamp", StringType()) \
    .add("bin_id", StringType()) \
    .add("sensor_type", StringType()) \
    .add("measurement", StringType())

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "waste-sensor-data") \
    .option("startingOffsets", "latest") \
    .load()

json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")

parsed_df = json_df.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

filtered_df = parsed_df.filter(col("sensor_type") == "ultrasonic")

query = filtered_df.writeStream \
    .foreachBatch(lambda df, epoch_id: df.write
                  .format("org.apache.spark.sql.cassandra")
                  .options(keyspace="wastebin", table="sensor_data")
                  .mode("append")
                  .save()) \
    .outputMode("append") \
    .start()

query.awaitTermination()
