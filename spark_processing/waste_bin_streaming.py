import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, substring, to_timestamp, when, lit
)
from pyspark.sql.types import (
    StructType, StringType, DoubleType, IntegerType
)

# -----------------------------
# Configurable thresholds
# -----------------------------
FILL_LEVEL_ALARM = 90.0      # %
TEMPERATURE_ALARM = 60.0     # °C
CO2_ALARM = 2000.0           # ppm (example)
WEIGHT_MIN_WRITE = 1.0       # kg (example: skip near-zero noise)
# -----------------------------

# --- Hadoop setup (Windows) ---
os.environ["HADOOP_HOME"] = os.getenv("HADOOP_HOME", r"C:\Program Files\hadoop\hadoop-3.3.6")
os.environ["PATH"] += f";{os.environ['HADOOP_HOME']}\\bin"

# --- SparkSession ---
spark = SparkSession.builder \
    .appName("SmartWasteStreaming") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,"
            "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1") \
    .config("spark.cassandra.connection.host", os.getenv("CASSANDRA_HOST", "127.0.0.1")) \
    .config("spark.cassandra.connection.port", os.getenv("CASSANDRA_PORT", "9042")) \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Spark session created successfully")

# --- Schemas ---
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

# --- Kafka topic helper ---
def create_kafka_topics():
    try:
        from kafka.admin import KafkaAdminClient, NewTopic
        from kafka.errors import TopicAlreadyExistsError

        admin_client = KafkaAdminClient(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
            client_id="topic_creator"
        )

        topics = [
            NewTopic(name="bin-metadata", num_partitions=3, replication_factor=1),
            NewTopic(name="waste-sensor-data", num_partitions=3, replication_factor=1),
            NewTopic(name="citizen-reports", num_partitions=3, replication_factor=1),
            NewTopic(name="maintenance-events", num_partitions=3, replication_factor=1),
            NewTopic(name="alarms", num_partitions=3, replication_factor=1),
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
        print("kafka-python not installed. Create topics manually if needed.")

create_kafka_topics()

# --- Kafka reader ---
def read_kafka(topic):
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    try:
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .option("kafka.session.timeout.ms", "30000") \
            .option("kafka.request.timeout.ms", "40000") \
            .load()
        print(f"Subscribed to topic: {topic}")
        return df.selectExpr("CAST(value AS STRING) as json_str")
    except Exception as e:
        print(f"Error reading from Kafka topic {topic}: {e}")
        raise

# --- batch writer(s) ---
def write_to_cassandra(df, keyspace, table):
    df.write \
      .format("org.apache.spark.sql.cassandra") \
      .mode("append") \
      .options(keyspace=keyspace, table=table) \
      .save()

def process_batch(df, epoch_id, keyspace, table, label):
    try:
        cnt = df.count()
        if cnt > 0:
            print(f"[{label}] batch {epoch_id} -> {cnt} rows")
            df.show(5, truncate=False)
            write_to_cassandra(df, keyspace, table)
        else:
            print(f"[{label}] batch {epoch_id} empty")
    except Exception as e:
        print(f"Error in [{label}] batch {epoch_id}: {e}")

# -----------------------------
# 1) Bins stream (no special filters)
# -----------------------------
bins_df = read_kafka("bin-metadata") \
    .select(from_json(col("json_str"), schema_bins).alias("data")) \
    .select("data.*") \
    .filter(col("bin_id").isNotNull())

bins_query = bins_df.writeStream \
    .foreachBatch(lambda df, epoch_id: process_batch(df, epoch_id, "wastebin", "bins", "bins")) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoint/bins") \
    .trigger(processingTime='30 seconds') \
    .start()

print("Bins stream started")

# -----------------------------
# 2) Sensor readings stream with FILTERS
# -----------------------------
sensor_df_raw = read_kafka("waste-sensor-data") \
    .select(from_json(col("json_str"), schema_sensor).alias("data")) \
    .select("data.*") \
    .filter(col("bin_id").isNotNull()) \
    .withColumn("ts_trimmed", substring(col("timestamp"), 1, 23)) \
    .withColumn("timestamp", to_timestamp(col("ts_trimmed"), "yyyy-MM-dd'T'HH:mm:ss.SSS")) \
    .drop("ts_trimmed") \
    .filter(col("timestamp").isNotNull())

# Normalize numeric value for filters and alarms (leave original 'value' as text for raw writes if needed)
sensor_df = sensor_df_raw.withColumn(
    "value_num",
    when(col("value").rlike("^[0-9]+(\\.[0-9]+)?$"), col("value").cast("double")).otherwise(lit(None).cast("double"))
)

# Example data filters BEFORE saving (adjust as you like):
#  - ultrasonic: keep only > 30% or any reading that triggers alarm (> 90%)
#  - temperature: keep only > 5°C or any alarm (> 60°C)
#  - weight: keep only > 1kg
#  - co2_ppm: keep only > 400ppm baseline or any alarm
sensor_df_filtered = sensor_df.filter(
    ((col("sensor_type") == "ultrasonic") & ((col("value_num") > 30) | (col("value_num") > FILL_LEVEL_ALARM))) |
    ((col("sensor_type") == "temperature") & ((col("value_num") > 5)  | (col("value_num") > TEMPERATURE_ALARM))) |
    ((col("sensor_type") == "weight") & (col("value_num") > WEIGHT_MIN_WRITE)) |
    ((col("sensor_type") == "co2_ppm") & ((col("value_num") > 400) | (col("value_num") > CO2_ALARM))) |
    # keep GPS/camera if present in sensor topic (often they are separate)
    (col("sensor_type").isin("gps", "camera"))
)

sensor_query = sensor_df_filtered \
    .select("bin_id", "timestamp", "sensor_type", "value") \
    .writeStream \
    .foreachBatch(lambda df, epoch_id: process_batch(df, epoch_id, "wastebin", "sensor_readings", "sensor")) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoint/sensor") \
    .trigger(processingTime='10 seconds') \
    .start()

print("Sensor stream started with filters")

# -----------------------------
# 3) Alarm detection stream
# -----------------------------
alarms_df = sensor_df.filter(
    ((col("sensor_type") == "ultrasonic") & (col("value_num") > FILL_LEVEL_ALARM)) |
    ((col("sensor_type") == "temperature") & (col("value_num") > TEMPERATURE_ALARM)) |
    ((col("sensor_type") == "co2_ppm") & (col("value_num") > CO2_ALARM))
).withColumn(
    "alarm_type",
    when((col("sensor_type") == "ultrasonic") & (col("value_num") > FILL_LEVEL_ALARM), lit("High Fill Level")) \
    .when((col("sensor_type") == "temperature") & (col("value_num") > TEMPERATURE_ALARM), lit("High Temperature")) \
    .when((col("sensor_type") == "co2_ppm") & (col("value_num") > CO2_ALARM), lit("High CO2")) \
)

# Write alarms to Cassandra
alarms_query_cassandra = alarms_df \
    .select(
        col("bin_id"),
        col("timestamp"),
        col("sensor_type"),
        col("value_num").alias("value"),
        col("alarm_type")
    ) \
    .writeStream \
    .foreachBatch(lambda df, epoch_id: process_batch(df, epoch_id, "wastebin", "alarms", "alarms-cassandra")) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoint/alarms_cass") \
    .trigger(processingTime='10 seconds') \
    .start()

# Publish alarms to Kafka topic "alarms" for real-time consumers
kafka_servers = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
alarms_query_kafka = alarms_df \
    .selectExpr(
        "to_json(named_struct('bin_id', bin_id, 'timestamp', CAST(timestamp AS STRING), 'sensor_type', sensor_type, 'value', CAST(value_num AS STRING), 'alarm_type', alarm_type)) AS value"
    ) \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_servers) \
    .option("topic", "alarms") \
    .option("checkpointLocation", "/tmp/checkpoint/alarms_kafka") \
    .trigger(processingTime='10 seconds') \
    .start()

print("Alarm streams started")

# -----------------------------
# 4) Citizen reports
# -----------------------------
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

print("Reports stream started")

# -----------------------------
# 5) Maintenance events
# -----------------------------
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

print("Maintenance stream started")

# -----------------------------
# Await termination
# -----------------------------
try:
    print("All streams started. Waiting for termination... (Ctrl+C to stop)")
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("\nStopping all streams...")
    for q in spark.streams.active:
        q.stop()
    print("All streams stopped")
finally:
    spark.stop()
    print("Spark session closed")
