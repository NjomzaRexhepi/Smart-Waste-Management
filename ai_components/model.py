from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

def train_waste_prediction_model():
    historical_data = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(keyspace="wastebin", table="sensor_readings") \
        .load()
    
    training_data = historical_data.filter(col("sensor_type") == "ultrasonic") \
        .groupBy("bin_id", date_trunc("hour", "timestamp").alias("hour")) \
        .agg(avg("value").alias("avg_fill_level")) \
        .withColumn("needs_cleaning", when(col("avg_fill_level") > 85, 1).otherwise(0))
    
    assembler = VectorAssembler(inputCols=["avg_fill_level"], outputCol="features")
    lr = LogisticRegression(featuresCol="features", labelCol="needs_cleaning")
    pipeline = Pipeline(stages=[assembler, lr])
    
    model = pipeline.fit(training_data)
    model.write().overwrite().save("/models/waste_prediction_model")
    
    return model

def predict_cleaning_needs():
    model = PipelineModel.load("/models/waste_prediction_model")
    
    current_hour_data = sensor_df.filter(col("sensor_type") == "ultrasonic") \
        .withWatermark("timestamp", "1 hour") \
        .groupBy("bin_id", window(col("timestamp"), "1 hour")) \
        .agg(avg("value_num").alias("avg_fill_level"))
    
    predictions = model.transform(current_hour_data)
    return predictions.filter(col("prediction") == 1.0)