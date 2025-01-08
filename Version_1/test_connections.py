from pyspark.sql import SparkSession
import time
from elasticsearch import Elasticsearch

def log_message(message):
   print(f"{time.strftime('%H:%M:%S')} - {message}")

log_message("Starting connection tests...")

# 1. Create Spark Session
try:
   log_message("Creating Spark Session...")
   spark = SparkSession.builder \
       .appName("Connection Test") \
       .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
       .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
       .getOrCreate()
   log_message("Spark Session created successfully!")
except Exception as e:
   log_message(f"Spark Session creation error: {e}")

# 2. Test Kafka Connection
try:
   log_message("Testing Kafka connection...")
   log_message("Attempting to connect to test-topic...")
   df = spark \
       .readStream \
       .format("kafka") \
       .option("kafka.bootstrap.servers", "kafka:9092") \
       .option("subscribe", "test-topic") \
       .load()
   log_message("Kafka connection successful! Topic is accessible.")
   
   # Try starting a simple stream
   query = df.writeStream \
       .format("console") \
       .outputMode("append") \
       .start()
   
   log_message("Kafka stream started successfully! Waiting 5 seconds...")
   time.sleep(5)  # Monitor stream for 5 seconds
   query.stop()
   log_message("Kafka stream stopped.")
   
except Exception as e:
   log_message(f"Kafka connection error: {e}")

# 3. Test Elasticsearch Connection
try:
   log_message("Testing Elasticsearch connection...")
   es = Elasticsearch(["http://es:9200"])
   if es.ping():
       log_message("Elasticsearch connection successful! Service is responding.")
       cluster_info = es.info()
       log_message(f"Elasticsearch version: {cluster_info['version']['number']}")
       log_message("\n")
       log_message("=" * 50)
       log_message("🎉 ALL SYSTEMS GO CAPTAIN! 🎉")
       log_message("=" * 50)
       log_message("🍫 TIME FOR A CHOCOLATE WAFER! 🍫")
       log_message("COME ON TUGBA, GET US SOME CHOCOLATE WAFERS! 🏃‍♀️ 🛍️")
       log_message("=" * 50)
   else:
       log_message("Elasticsearch is not responding!")
except Exception as e:
   log_message(f"Elasticsearch connection error: {e}")

log_message("All connection tests completed.")