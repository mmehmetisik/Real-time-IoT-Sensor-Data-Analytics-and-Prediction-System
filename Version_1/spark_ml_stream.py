import sys
import os
import warnings
import traceback
import logging
import findspark
import time
from datetime import datetime
from colorama import Fore, Back, Style, init
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml.feature import VectorAssembler

init()
findspark.init("/opt/spark")

logging.getLogger("org.apache.spark").setLevel(logging.ERROR)
logging.getLogger("org.apache.kafka").setLevel(logging.ERROR)

def print_banner():
   """Print colored banner"""
   banner = f"""
{Fore.CYAN}
╔═══════════════════════════════════════════════════════╗
║             ML STREAM PROCESSING                      ║
║                 Version 1.0                           ║
║ --------------------------------------------------- ║
║  📥 office-input → 🤖 ML → 📤 activity/no-activity     ║
╚═══════════════════════════════════════════════════════╝
{Style.RESET_ALL}"""
   print(banner)

def print_batch_stats(batch_id, movement_count, no_movement_count, duration):
   """Print batch processing statistics"""
   print(f"\n{Fore.CYAN}{'='*60}")
   stats = f"""
   📊 BATCH {batch_id} SUMMARY
   ├── 🏃 Movement Detected : {movement_count}
   ├── 🚫 No Movement      : {no_movement_count}
   └── ⏱️  Duration        : {duration:.2f} seconds
   """
   print(stats)
   print(f"{'='*60}{Style.RESET_ALL}\n")

def log_message(message, level="info", indent=0):
   """Colored log messages"""
   timestamp = datetime.now().strftime("%H:%M:%S")
   colors = {
       "info": Fore.GREEN,
       "warning": Fore.YELLOW,
       "error": Fore.RED + Back.WHITE,
       "highlight": Fore.BLUE,
       "success": Fore.CYAN
   }
   icons = {
       "info": "ℹ️",
       "warning": "⚠️",
       "error": "❌",
       "highlight": "🔍",
       "success": "✅"
   }
   indent_str = "  " * indent
   print(f"{colors.get(level, Fore.WHITE)}[{timestamp}] {icons.get(level, '')} {indent_str}{message}{Style.RESET_ALL}")

print_banner()
log_message("🚀 Starting ML Stream process...", "highlight")

# Initialize Spark Session
try:
   spark = (SparkSession.builder
            .appName("ML Stream Processing")
            .master("local[2]")
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "4g")
            .config("spark.sql.shuffle.partitions", "10")
            .config("spark.default.parallelism", "10")
            .config("spark.network.timeout", "800s")
            .config("spark.executor.heartbeatInterval", "60s")
            .config("spark.storage.blockManagerSlaveTimeoutMs", "800s")
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1")
            .getOrCreate())
   
   spark.sparkContext.setLogLevel("ERROR")
   log_message("✅ Spark Session created successfully!", "success")
except Exception as e:
   log_message(f"❌ Spark Session error: {str(e)}", "error")
   sys.exit(1)

# Read from Kafka
try:
   df = spark \
       .readStream \
       .format("kafka") \
       .option("kafka.bootstrap.servers", "kafka:9092") \
       .option("subscribe", "office-input") \
       .option("startingOffsets", "latest") \
       .load()
   log_message("✅ Kafka connection successful!", "success")
except Exception as e:
   log_message(f"❌ Kafka connection error: {str(e)}", "error")
   sys.exit(1)

# Data transformations
log_message("🔄 Starting data transformations...")

df2 = df.selectExpr("CAST(value AS STRING)")
df3 = df2.withColumn("timestamp", F.split(F.col("value"), ",")[0]) \
   .withColumn("ts_min_bignt", F.split(F.col("value"), ",")[1].cast(IntegerType())) \
   .withColumn("room", F.split(F.col("value"), ",")[2]) \
   .withColumn("co2", F.split(F.col("value"), ",")[3].cast(FloatType())) \
   .withColumn("light", F.split(F.col("value"), ",")[4].cast(FloatType())) \
   .withColumn("temp", F.split(F.col("value"), ",")[5].cast(FloatType())) \
   .withColumn("humidity", F.split(F.col("value"), ",")[6].cast(FloatType())) \
   .withColumn("pir", F.split(F.col("value"), ",")[7].cast(FloatType()))

# Create feature vector
assembler = VectorAssembler(
   inputCols=["co2", "light", "temp", "humidity"],
   outputCol="features"
)
vectorized_df = assembler.transform(df3)

# Load ML model
try:
   model = LogisticRegressionModel.load("/opt/spark/ml_model")
   log_message("✅ Model loaded successfully!", "success")
except Exception as e:
   log_message(f"❌ Model loading error: {str(e)}", "error")
   sys.exit(1)

# Make predictions
predictions = model.transform(vectorized_df)

def process_batch(batch_df, batch_id):
   """Process each batch and write to Kafka"""
   start_time = time.time()
   try:
       # Adjust timestamp
       batch_df = batch_df.withColumn(
           "event_ts_min", 
           F.from_unixtime(F.col("ts_min_bignt")).cast("timestamp")
       )
       
       # Get probability value from vector
       batch_df = batch_df.withColumn(
           "confidence",
           F.udf(lambda x: float(x[1]), FloatType())(F.col("probability"))
       )
       
       # Remove unnecessary columns
       columns_to_drop = ["probability", "features", "rawPrediction", "value"]
       batch_df = batch_df.drop(*columns_to_drop)
       
       # Split movement and no-movement data
       movement_df = batch_df.filter(F.col("prediction") == 1.0)
       no_movement_df = batch_df.filter(F.col("prediction") == 0.0)
       
       # Columns to send
       COLUMNS_TO_SEND = [
           "event_ts_min", 
           "room",
           "co2", 
           "light", 
           "temp", 
           "humidity",
           "prediction",
           "confidence"
       ]
       
       # Send movement data
       movement_count = 0
       if movement_df.count() > 0:
           movement_count = movement_df.count()
           (movement_df
            .select(COLUMNS_TO_SEND)
            .select(F.to_json(F.struct(*COLUMNS_TO_SEND)).alias("value"))
            .write
            .format("kafka")
            .option("kafka.bootstrap.servers", "kafka:9092")
            .option("topic", "office-activity")
            .save())
       
       # Send no-movement data
       no_movement_count = 0
       if no_movement_df.count() > 0:
           no_movement_count = no_movement_df.count()
           (no_movement_df
            .select(COLUMNS_TO_SEND)
            .select(F.to_json(F.struct(*COLUMNS_TO_SEND)).alias("value"))
            .write
            .format("kafka")
            .option("kafka.bootstrap.servers", "kafka:9092")
            .option("topic", "office-no-activity")
            .save())
       
       # Show batch statistics
       duration = time.time() - start_time
       print_batch_stats(batch_id, movement_count, no_movement_count, duration)
       
   except Exception as e:
       log_message(f"❌ Batch processing error: {str(e)}", "error")
       raise e

# Start streaming process
try:
   query = (predictions.writeStream
            .foreachBatch(process_batch)
            .option("checkpointLocation", "/tmp/kafka_ml_checkpoint")
            .trigger(processingTime='10 seconds')
            .start())
   
   log_message("""
   ✨ Stream started successfully!
   ├── 📥 Input: Kafka (office-input)
   ├── 🤖 Model: Logistic Regression
   ├── 📤 Output 1: Kafka (office-activity)
   └── 📤 Output 2: Kafka (office-no-activity)
   """, "success")
   
   query.awaitTermination()
   
except Exception as e:
   log_message(f"❌ Stream startup error: {str(e)}", "error")
   sys.exit(1)