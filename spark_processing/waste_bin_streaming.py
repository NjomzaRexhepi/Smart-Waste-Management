import os
import uuid
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, when, lit, udf, coalesce
)
from pyspark.sql.types import (
    StructType, StringType, DoubleType, IntegerType, BooleanType, DateType, TimestampType
)
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
from pyspark.sql.functions import try_to_timestamp

# ----------------------------------------------------
# Configurable thresholds
# ----------------------------------------------------
FILL_LEVEL_ALARM = 90.0
TEMPERATURE_ALARM = 60.0

# ----------------------------------------------------
# Environment setup
# ----------------------------------------------------
os.environ["HADOOP_HOME"] = os.getenv("HADOOP_HOME", r"C:\hadoop-3.3.6")
os.environ["PATH"] += f";{os.environ['HADOOP_HOME']}\\bin"

EMAIL_CONFIG = {
    'smtp_server': 'smtp.gmail.com',
    'smtp_port': 587,
    'username': 'test17@gmail.com',  
    'password': '',
    'recipient': 'test@gmail.com'
}

# ----------------------------------------------------
# Email function (fixed)
# ----------------------------------------------------
import time
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import KafkaError

def ensure_topics_exist():
    """Ensure all required topics exist in Kafka"""
    topics = ["bins", "sensor_data", "citizen_reports", "maintenance_events", 
              "alarms", "performance_analytics", "route_history"]
    
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers="localhost:9092",
            client_id='spark_admin'
        )
        
        existing_topics = admin_client.list_topics()
        topics_to_create = []
        
        for topic in topics:
            if topic not in existing_topics:
                print(f"Topic {topic} doesn't exist, creating...")
                topics_to_create.append(NewTopic(name=topic, num_partitions=3, replication_factor=1))
        
        if topics_to_create:
            admin_client.create_topics(new_topics=topics_to_create, validate_only=False)
            print(f"Created {len(topics_to_create)} topics")
            time.sleep(5)  # Wait for topics to be ready
        
        admin_client.close()
        return True
    except Exception as e:
        print(f"Error ensuring topics: {e}")
        return False

# Call this before creating Spark session
print("Checking Kafka topics...")
if not ensure_topics_exist():
    print("Warning: Could not verify/create topics. Proceeding anyway...")
    time.sleep(5)
    
def send_email_alert(alert_data):
    try:
        msg = MIMEMultipart()
        msg['From'] = EMAIL_CONFIG['username']
        msg['To'] = EMAIL_CONFIG['recipient']
        msg['Subject'] = f"URGENT: {alert_data['alarm_type']} - {alert_data['bin_id']}"

        body = f"""
Alert Details:
- Bin ID: {alert_data['bin_id']}
- Alert Type: {alert_data['alarm_type']}
- Severity: {alert_data.get('severity', 'high')}
- Timestamp: {alert_data['triggered_at']}
- Location: https://maps.google.com/?q={alert_data.get('lat', '')},{alert_data.get('lon', '')}

Please take immediate action.
"""
        msg.attach(MIMEText(body, 'plain'))

        server = smtplib.SMTP(EMAIL_CONFIG['smtp_server'], EMAIL_CONFIG['smtp_port'])
        server.starttls()
        server.login(EMAIL_CONFIG['username'], EMAIL_CONFIG['password'])
        server.sendmail(EMAIL_CONFIG['username'], EMAIL_CONFIG['recipient'], msg.as_string())
        server.quit()

        logger.info(f"Email alert sent for {alert_data['bin_id']}")
        return True
    except Exception as e:
        logger.error(f"Failed to send email: {e}")
        return False

# --------------------------------------------------------
# Spark session with corrected packages
# --------------------------------------------------------
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

# ------------------------------------------------------
# UUID generator
# ------------------------------------------------------
def generate_uuid():
    return str(uuid.uuid4())
uuid_udf = udf(generate_uuid, StringType())

# ------------------------------------------------------
# Kafka reader helper
# ------------------------------------------------------
def read_kafka(topic):
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_servers) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .option("maxOffsetsPerTrigger", 1000) \
        .option("kafka.consumer.request.timeout.ms", "20000") \
        .option("kafka.consumer.session.timeout.ms", "10000") \
        .load() \
        .selectExpr("CAST(value AS STRING) as json_str")

# ------------------------------------------------------
# Enhanced batch processing with error handling
# ------------------------------------------------------
def process_batch_safe(df, epoch_id, keyspace, table, label):
    try:
        cnt = df.count()
        if cnt > 0:
            print(f"[{label}] Processing batch {epoch_id} with {cnt} rows")
            
            print(f"[{label}] Sample data:")
            df.show(3, truncate=False)
            
            df.write \
                .format("org.apache.spark.sql.cassandra") \
                .mode("append") \
                .option("keyspace", keyspace) \
                .option("table", table) \
                .option("spark.cassandra.output.ignoreNulls", "true") \
                .option("spark.cassandra.output.consistency.level", "ONE") \
                .option("spark.cassandra.output.batch.size.rows", "100") \
                .option("spark.cassandra.output.concurrent.writes", "1") \
                .save()
                
            if table.lower() == "alarms" or table.lower() == "sensor_alarms":
                alerts = df.collect()
                for row in alerts:
                    alert_data = row.asDict()
                    send_email_alert(alert_data)
                    
            print(f"[{label}] Successfully wrote {cnt} rows to {keyspace}.{table}")
        else:
            print(f"[{label}] Batch {epoch_id} is empty")
            
    except Exception as e:
        print(f"[{label}] Error in batch {epoch_id}: {str(e)}")
        import traceback
        traceback.print_exc()

# ------------------------------------------------------
# Schemas matching simulator output
# -----------------------------------------------------
schema_bins = StructType() \
    .add("bin_id", StringType()) \
    .add("location_lat", DoubleType()) \
    .add("location_lng", DoubleType()) \
    .add("type", StringType()) \
    .add("capacity", DoubleType()) \
    .add("current_fill_level", DoubleType()) \
    .add("status", StringType()) \
    .add("last_maintenance_date", TimestampType()) \
    .add("installation_date", TimestampType())

schema_sensor = StructType() \
    .add("sensor_id", StringType()) \
    .add("bin_id", StringType()) \
    .add("timestamp", StringType()) \
    .add("fill_level", DoubleType()) \
    .add("temperature", DoubleType()) \
    .add("weight", DoubleType()) \
    .add("humidity", DoubleType()) \
    .add("status", StringType())

schema_reports = StructType() \
    .add("report_id", StringType()) \
    .add("bin_id", StringType()) \
    .add("report_timestamp", StringType()) \
    .add("report_type", StringType()) \
    .add("description", StringType()) \
    .add("resolved", BooleanType()) \
    .add("resolved_timestamp", StringType())

schema_maintenance = StructType() \
    .add("event_id", StringType()) \
    .add("bin_id", StringType()) \
    .add("maintenance_type", StringType()) \
    .add("event_timestamp", StringType()) \
    .add("performed_by", StringType()) \
    .add("duration", DoubleType()) \
    .add("cost", DoubleType())

schema_alarms = StructType() \
    .add("alarm_id", StringType()) \
    .add("bin_id", StringType()) \
    .add("alarm_type", StringType()) \
    .add("triggered_at", StringType()) \
    .add("resolved_at", StringType()) \
    .add("severity", StringType()) \
    .add("status", StringType())

schema_performance = StructType() \
    .add("id", StringType()) \
    .add("date", StringType()) \
    .add("total_bins", IntegerType()) \
    .add("avg_fill_level", DoubleType()) \
    .add("total_reports", IntegerType()) \
    .add("resolved_reports", IntegerType()) \
    .add("maintenance_events_count", IntegerType()) \
    .add("avg_response_time", DoubleType()) \
    .add("alarms_count", IntegerType()) \
    .add("routes_completed", IntegerType())

schema_routes = StructType() \
    .add("route_id", StringType()) \
    .add("bin_id", StringType()) \
    .add("service_date", StringType()) \
    .add("collection_status", StringType())

# ------------------------------------------------------
# Helper function for timestamp parsing
# ------------------------------------------------------
def parse_timestamp_safe(timestamp_col):
    """Safely parse timestamps with multiple format attempts"""
    return coalesce(
        to_timestamp(timestamp_col, "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"),
        to_timestamp(timestamp_col, "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
        to_timestamp(timestamp_col, "yyyy-MM-dd'T'HH:mm:ssXXX"),
        to_timestamp(timestamp_col, "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"),
        to_timestamp(timestamp_col, "yyyy-MM-dd'T'HH:mm:ss.SSS"),
        to_timestamp(timestamp_col, "yyyy-MM-dd'T'HH:mm:ss"),
        to_timestamp(timestamp_col, "yyyy-MM-dd"),
        to_timestamp(timestamp_col)
    )

# ------------------------------------------------------
# 1) Bins stream
# ------------------------------------------------------
print("Setting up bins stream...")
bins_df = read_kafka("bins") \
    .select(from_json(col("json_str"), schema_bins).alias("data")) \
    .select("data.*") \
    .filter(col("bin_id").isNotNull()) \
    .withColumn(
        "last_maintenance_date",
        try_to_timestamp("last_maintenance_date")
    ) \
    .withColumn(
        "installation_date",
        try_to_timestamp("installation_date")
    )

bins_query = bins_df.writeStream \
    .foreachBatch(lambda df, epoch_id: process_batch_safe(df, epoch_id, "wastebin", "bins", "BINS")) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/spark_checkpoints/bins") \
    .trigger(processingTime='30 seconds') \
    .start()

# ------------------------------------------------------
# 2) Sensor stream
# ------------------------------------------------------
print("Setting up sensor stream...")
sensor_df = read_kafka("sensor_data") \
    .select(from_json(col("json_str"), schema_sensor).alias("data")) \
    .select("data.*") \
    .filter(col("bin_id").isNotNull()) \
    .withColumn("timestamp", parse_timestamp_safe(col("timestamp"))) \
    .filter(col("timestamp").isNotNull())

sensor_query = sensor_df.writeStream \
    .foreachBatch(lambda df, epoch_id: process_batch_safe(df, epoch_id, "wastebin", "sensor_data", "SENSOR")) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/spark_checkpoints/sensor") \
    .trigger(processingTime='15 seconds') \
    .start()

# ------------------------------------------------------
# 3) Citizen reports
# ------------------------------------------------------
print("Setting up citizen reports stream...")
reports_df = read_kafka("citizen_reports") \
    .select(from_json(col("json_str"), schema_reports).alias("data")) \
    .select("data.*") \
    .filter(col("bin_id").isNotNull()) \
    .withColumn("report_timestamp", parse_timestamp_safe(col("report_timestamp"))) \
    .withColumn("resolved_timestamp", 
                when(col("resolved_timestamp").isNotNull() & (col("resolved_timestamp") != ""), 
                     parse_timestamp_safe(col("resolved_timestamp")))
                .otherwise(lit(None).cast(TimestampType())))

reports_query = reports_df.writeStream \
    .foreachBatch(lambda df, epoch_id: process_batch_safe(df, epoch_id, "wastebin", "citizen_reports", "REPORTS")) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/spark_checkpoints/reports") \
    .trigger(processingTime='30 seconds') \
    .start()

# ------------------------------------------------------
# 4) Maintenance events
# ------------------------------------------------------
print("Setting up maintenance stream...")
maintenance_df = read_kafka("maintenance_events") \
    .select(from_json(col("json_str"), schema_maintenance).alias("data")) \
    .select("data.*") \
    .filter(col("bin_id").isNotNull()) \
    .withColumn("event_timestamp", parse_timestamp_safe(col("event_timestamp")))

maintenance_query = maintenance_df.writeStream \
    .foreachBatch(lambda df, epoch_id: process_batch_safe(df, epoch_id, "wastebin", "maintenance_events", "MAINTENANCE")) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/spark_checkpoints/maintenance") \
    .trigger(processingTime='30 seconds') \
    .start()

# ------------------------------------------------------
# 5) Alarms stream
# ------------------------------------------------------
print("Setting up alarms stream...")
alarms_df = read_kafka("alarms") \
    .select(from_json(col("json_str"), schema_alarms).alias("data")) \
    .select("data.*") \
    .filter(col("bin_id").isNotNull()) \
    .withColumn("triggered_at", parse_timestamp_safe(col("triggered_at"))) \
    .withColumn("resolved_at", 
                when(col("resolved_at").isNotNull() & (col("resolved_at") != ""), 
                     parse_timestamp_safe(col("resolved_at")))
                .otherwise(lit(None).cast(TimestampType())))

alarms_query = alarms_df.writeStream \
    .foreachBatch(lambda df, epoch_id: process_batch_safe(df, epoch_id, "wastebin", "alarms", "ALARMS")) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/spark_checkpoints/alarms") \
    .trigger(processingTime='20 seconds') \
    .start()

# ------------------------------------------------------
# 6) Performance analytics
# ------------------------------------------------------
print("Setting up performance analytics stream...")
performance_df = read_kafka("performance_analytics") \
    .select(from_json(col("json_str"), schema_performance).alias("data")) \
    .select("data.*") \
    .filter(col("id").isNotNull()) \
    .withColumn("date", 
                when(col("date").isNotNull(), col("date").cast(DateType()))
                .otherwise(lit(None).cast(DateType())))

performance_query = performance_df.writeStream \
    .foreachBatch(lambda df, epoch_id: process_batch_safe(df, epoch_id, "wastebin", "performance_analytics", "PERFORMANCE")) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/spark_checkpoints/performance") \
    .trigger(processingTime='60 seconds') \
    .start()

# ------------------------------------------------------
# 7) Route history
# ------------------------------------------------------
print("Setting up routes stream...")
routes_df = read_kafka("route_history") \
    .select(from_json(col("json_str"), schema_routes).alias("data")) \
    .select("data.*") \
    .filter(col("route_id").isNotNull() & col("bin_id").isNotNull()) \
    .withColumn("service_date", parse_timestamp_safe(col("service_date")))

routes_query = routes_df.writeStream \
    .foreachBatch(lambda df, epoch_id: process_batch_safe(df, epoch_id, "wastebin", "route_history", "ROUTES")) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/spark_checkpoints/routes") \
    .trigger(processingTime='60 seconds') \
    .start()

# ------------------------------------------------------
# 8) Real-time alarm detection from sensor data
# ------------------------------------------------------
print("Setting up real-time alarm detection...")
sensor_alarms_df = sensor_df.filter(
    (col("fill_level") >= FILL_LEVEL_ALARM) | 
    (col("temperature") >= TEMPERATURE_ALARM)
).withColumn(
    "alarm_type_derived",
    when(col("fill_level") >= FILL_LEVEL_ALARM, lit("overflow"))
    .when(col("temperature") >= TEMPERATURE_ALARM, lit("temperature"))
    .otherwise(lit("system"))
).select(
    uuid_udf().alias("alarm_id"),
    col("bin_id"),
    col("alarm_type_derived").alias("alarm_type"),
    col("timestamp").alias("triggered_at"),
    lit(None).cast(TimestampType()).alias("resolved_at"),
    when(col("fill_level") >= 95, lit("critical"))
    .when(col("temperature") >= 70, lit("critical"))
    .otherwise(lit("high")).alias("severity"),
    lit("active").alias("status")
)

sensor_alarms_query = sensor_alarms_df.writeStream \
    .foreachBatch(lambda df, epoch_id: process_batch_safe(df, epoch_id, "wastebin", "alarms", "SENSOR_ALARMS")) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/spark_checkpoints/sensor_alarms") \
    .trigger(processingTime='20 seconds') \
    .start()

# ------------------------------------------------------
# Monitor and manage streams
# ------------------------------------------------------
def print_stream_status():
    active_streams = spark.streams.active
    print(f"\n=== Active Streams: {len(active_streams)} ===")
    for i, stream in enumerate(active_streams):
        print(f"{i+1}. {stream.name} - Status: {stream.status}")

try:
    print("\n" + "="*60)
    print("ALL STREAMS STARTED SUCCESSFULLY!")
    print("="*60)
    print("Processing Topics:")
    print("  • bins -> wastebin.bins")
    print("  • sensor_data -> wastebin.sensor_data") 
    print("  • citizen_reports -> wastebin.citizen_reports")
    print("  • maintenance_events -> wastebin.maintenance_events")
    print("  • alarms -> wastebin.alarms")
    print("  • performance_analytics -> wastebin.performance_analytics")
    print("  • route_history -> wastebin.route_history")
    print("  • sensor-based alarms -> wastebin.alarms")
    print("="*60)
    print("Press Ctrl+C to stop all streams")
    print("="*60)
    
    print_stream_status()
    
    spark.streams.awaitAnyTermination()
    
except KeyboardInterrupt:
    print("\nStopping all streams...")
    for stream in spark.streams.active:
        print(f"Stopping {stream.name}...")
        stream.stop()
    print("All streams stopped")
    
except Exception as e:
    print(f"Error occurred: {str(e)}")
    import traceback
    traceback.print_exc()
    
finally:
    spark.stop()
    print("Spark session closed")