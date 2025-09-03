import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, substring, to_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType, DateType

# --- Hadoop setup ---
os.environ["HADOOP_HOME"] = "C:\\Program Files\\hadoop\\hadoop-3.3.6"
os.environ["PATH"] += ";C:\\Program Files\\hadoop\\hadoop-3.3.6\\bin"

# --- SparkSession ---
spark = SparkSession.builder \
    .appName("SmartWasteStreaming") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,"
            "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1") \
    .config("spark.cassandra.connection.host", "127.0.0.1") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Spark session created successfully")

# --- Schemas for different topics ---
schema_bins = StructType() \
    .add("bin_id", StringType()) \
    .add("type", StringType()) \
    .add("capacity_kg", IntegerType()) \
    .add("district", StringType()) \
    .add("street", StringType()) \
    .add("latitude", DoubleType()) \
    .add("longitude", DoubleType()) \
    .add("installation_date", StringType()) \
    .add("last_maintenance", StringType()) \
    .add("status", StringType())

schema_sensor = StructType() \
    .add("bin_id", StringType()) \
    .add("sensor_type", StringType()) \
    .add("timestamp", StringType()) \
    .add("value", StringType())

schema_reports = StructType() \
    .add("report_id", StringType()) \
    .add("bin_id", StringType()) \
    .add("issue_type", StringType()) \
    .add("description", StringType()) \
    .add("reporter_id", StringType()) \
    .add("timestamp", StringType()) \
    .add("status", StringType())

schema_maintenance = StructType() \
    .add("event_id", StringType()) \
    .add("bin_id", StringType()) \
    .add("action", StringType()) \
    .add("technician", StringType()) \
    .add("timestamp", StringType())

# --- Function to create Kafka topics if they don't exist ---
def create_kafka_topics():
    """
    Create Kafka topics programmatically using kafka-python
    Make sure to install: pip install kafka-python
    """
    try:
        from kafka.admin import KafkaAdminClient, NewTopic
        from kafka.errors import TopicAlreadyExistsError
        
        admin_client = KafkaAdminClient(
            bootstrap_servers="localhost:9092",
            client_id="topic_creator"
        )
        
        topics = [
            NewTopic(name="bin-metadata", num_partitions=3, replication_factor=1),
            NewTopic(name="waste-sensor-data", num_partitions=3, replication_factor=1),
            NewTopic(name="citizen-reports", num_partitions=3, replication_factor=1),
            NewTopic(name="maintenance-events", num_partitions=3, replication_factor=1)
        ]
        
        try:
            admin_client.create_topics(new_topics=topics, validate_only=False)
            print("Topics created successfully")
        except TopicAlreadyExistsError:
            print("Topics already exist")
        except Exception as e:
            print(f"Error creating topics: {e}")
        
        admin_client.close()
        
    except ImportError:
        print("kafka-python not installed. Please create topics manually using:")
        print("kafka-topics.sh --create --topic bin-metadata --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1")
        print("kafka-topics.sh --create --topic waste-sensor-data --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1")
        print("kafka-topics.sh --create --topic citizen-reports --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1")
        print("kafka-topics.sh --create --topic maintenance-events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1")

# --- Create topics before starting streams ---
create_kafka_topics()

# --- Utility function to read from Kafka ---
def read_kafka(topic):
    try:
        kafka_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .option("kafka.session.timeout.ms", "30000") \
            .option("kafka.request.timeout.ms", "40000") \
            .load()
        print(f"Successfully subscribed to topic: {topic}")
        return kafka_df.selectExpr("CAST(value AS STRING) as json_str")
    except Exception as e:
        print(f"Error reading from Kafka topic {topic}: {e}")
        raise

# --- Enhanced writer with error handling ---
def write_to_cassandra(df, keyspace, table):
    try:
        if df.count() > 0:  # Only write if there's data
            df.write \
                .format("org.apache.spark.sql.cassandra") \
                .mode("append") \
                .options(keyspace=keyspace, table=table) \
                .save()
            print(f"Successfully wrote {df.count()} records to {keyspace}.{table}")
        else:
            print(f"No data to write to {keyspace}.{table}")
    except Exception as e:
        print(f"Error writing to Cassandra table {keyspace}.{table}: {e}")
        # Don't re-raise the exception to avoid stopping the stream

# --- Enhanced batch processing function ---
def process_batch(df, epoch_id, keyspace, table, stream_name):
    try:
        print(f"Processing batch {epoch_id} for {stream_name}")
        if df.count() > 0:
            df.show(5, truncate=False)  # Show sample data
            write_to_cassandra(df, keyspace, table)
        else:
            print(f"Empty batch {epoch_id} for {stream_name}")
    except Exception as e:
        print(f"Error processing batch {epoch_id} for {stream_name}: {e}")

# --- 1. Bins stream ---
try:
    bins_df = read_kafka("bin-metadata") \
        .select(from_json(col("json_str"), schema_bins).alias("data")) \
        .select("data.*") \
        .filter(col("bin_id").isNotNull())  # Filter out null records

    bins_query = bins_df.writeStream \
        .foreachBatch(lambda df, epoch_id: process_batch(df, epoch_id, "wastebin", "bins", "bins")) \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/checkpoint/bins") \
        .trigger(processingTime='30 seconds') \
        .start()
    
    print("Bins stream started successfully")
    
except Exception as e:
    print(f"Error starting bins stream: {e}")

# --- 2. Sensor readings stream ---
try:
    sensor_df = read_kafka("waste-sensor-data") \
        .select(from_json(col("json_str"), schema_sensor).alias("data")) \
        .select("data.*") \
        .filter(col("bin_id").isNotNull()) \
        .withColumn("ts_trimmed", substring(col("timestamp"), 1, 23)) \
        .withColumn("timestamp", to_timestamp(col("ts_trimmed"), "yyyy-MM-dd'T'HH:mm:ss.SSS")) \
        .drop("ts_trimmed") \
        .filter(col("timestamp").isNotNull())  # Filter out invalid timestamps

    sensor_query = sensor_df.writeStream \
        .foreachBatch(lambda df, epoch_id: process_batch(df, epoch_id, "wastebin", "sensor_readings", "sensor")) \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/checkpoint/sensor") \
        .trigger(processingTime='10 seconds') \
        .start()
    
    print("Sensor stream started successfully")
    
except Exception as e:
    print(f"Error starting sensor stream: {e}")

# --- 3. Citizen reports stream ---
try:
    reports_df = read_kafka("citizen-reports") \
        .select(from_json(col("json_str"), schema_reports).alias("data")) \
        .select("data.*") \
        .filter(col("report_id").isNotNull()) \
        .withColumn("ts_trimmed", substring(col("timestamp"), 1, 23)) \
        .withColumn("timestamp", to_timestamp(col("ts_trimmed"), "yyyy-MM-dd'T'HH:mm:ss.SSS")) \
        .drop("ts_trimmed") \
        .filter(col("timestamp").isNotNull())

    reports_query = reports_df.writeStream \
        .foreachBatch(lambda df, epoch_id: process_batch(df, epoch_id, "wastebin", "citizen_reports", "reports")) \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/checkpoint/reports") \
        .trigger(processingTime='30 seconds') \
        .start()
    
    print("Reports stream started successfully")
    
except Exception as e:
    print(f"Error starting reports stream: {e}")

# --- 4. Maintenance events stream ---
try:
    maintenance_df = read_kafka("maintenance-events") \
        .select(from_json(col("json_str"), schema_maintenance).alias("data")) \
        .select("data.*") \
        .filter(col("event_id").isNotNull()) \
        .withColumn("ts_trimmed", substring(col("timestamp"), 1, 23)) \
        .withColumn("timestamp", to_timestamp(col("ts_trimmed"), "yyyy-MM-dd'T'HH:mm:ss.SSS")) \
        .drop("ts_trimmed") \
        .filter(col("timestamp").isNotNull())

    maintenance_query = maintenance_df.writeStream \
        .foreachBatch(lambda df, epoch_id: process_batch(df, epoch_id, "wastebin", "maintenance_events", "maintenance")) \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/checkpoint/maintenance") \
        .trigger(processingTime='30 seconds') \
        .start()
    
    print("Maintenance stream started successfully")
    
except Exception as e:
    print(f"Error starting maintenance stream: {e}")

# --- Keep all queries running with better error handling ---
try:
    print("All streams started. Waiting for termination...")
    print("Press Ctrl+C to stop the application")
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("\nStopping all streams...")
    for stream in spark.streams.active:
        stream.stop()
    print("All streams stopped")
except Exception as e:
    print(f"Error in stream processing: {e}")
finally:
    spark.stop()
    print("Spark session closed")