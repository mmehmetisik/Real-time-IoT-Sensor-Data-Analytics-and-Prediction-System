import os
import sys
import warnings
import logging
import findspark
import time
import pandas as pd
from datetime import datetime
from colorama import Fore, Back, Style, init
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

init()
findspark.init("/opt/spark")

logging.getLogger("org.apache.spark").setLevel(logging.ERROR)
logging.getLogger("org.apache.kafka").setLevel(logging.ERROR)

def print_banner():
   banner = f"""
{Fore.CYAN}
╔══════════════════════════════════════════════════════════════╗
║             SENSOR DATA STREAMING SYSTEM                      ║
║                      Version 3.0                             ║
║----------------------------------------------------------  ║
║    🔄 Automatic Processing  |  📊 Real-time Data             ║
║    💾 Training Data        |  🤖 ML Pipeline                 ║
╚══════════════════════════════════════════════════════════════╝
{Style.RESET_ALL}"""
   print(banner)

def log_message(message, level="info"):
   timestamp = datetime.now().strftime("%H:%M:%S")
   colors = {
       "info": Fore.GREEN, "warning": Fore.YELLOW, "error": Fore.RED,
       "success": Fore.CYAN, "highlight": Fore.BLUE,
       "stats": Fore.MAGENTA, "ml": Fore.MAGENTA + Style.BRIGHT
   }
   icons = {
       "info": "ℹ️", "warning": "⚠️", "error": "❌", "success": "✅",
       "highlight": "🔍", "stats": "📊", "ml": "🤖"
   }
   print(f"{colors.get(level, Fore.WHITE)}[{timestamp}] {icons.get(level, '')} {message}{Style.RESET_ALL}")

def print_progress(room_name, step, details=""):
   colors = {
       "read": Fore.BLUE, "process": Fore.GREEN,
       "write": Fore.CYAN, "ml": Fore.MAGENTA
   }
   step_icons = {
       "read": "📂", "process": "🔄",
       "write": "💾", "ml": "🤖"
   }
   message = f"[Room {room_name}] {step_icons.get(step, '•')} {details}"
   print(f"{colors.get(step, Fore.WHITE)}{message}{Style.RESET_ALL}")

def save_to_csv(df, is_first_room=False):
   try:
       # Convert DataFrame to Pandas
       pandas_df = df.toPandas()
       
       # Save to CSV
       mode = 'w' if is_first_room else 'a'
       header = is_first_room
       pandas_df.to_csv('/opt/data-generator/input/sensors.csv', 
                       mode=mode, header=header, index=False)
       return True
   except Exception as e:
       log_message(f"CSV save error: {str(e)}", "error")
       return False
def process_room_data(spark, room_name, directory_path, save_training=False, is_first_room=False):
   try:
       log_message(f"\n{'='*50}", "highlight")
       log_message(f"Starting process for Room {room_name}...", "info")
       
       sensor_files = {
           'co2': 'co2.csv',
           'humidity': 'humidity.csv',
           'light': 'light.csv',
           'pir': 'pir.csv',
           'temperature': 'temperature.csv'
       }
       
       schema = StructType([
           StructField("ts_min_bignt", StringType(), True),
           StructField("sensor_value", StringType(), True)
       ])

       dataframes = {}
       for sensor_type, filename in sensor_files.items():
           file_path = os.path.join(directory_path, room_name, filename)
           
           if not os.path.exists(file_path):
               print_progress(room_name, "read", f"{filename} not found!")
               continue
           
           print_progress(room_name, "read", f"Reading {filename}...")
           
           df = spark.read.option("delimiter", ",").schema(schema).csv(file_path)
           df = df.withColumn("ts_min_bignt", F.trim(F.col("ts_min_bignt"))) \
                  .withColumn("sensor_value", F.trim(F.col("sensor_value"))) \
                  .withColumnRenamed("sensor_value", sensor_type)
           
           df.createOrReplaceTempView(f"df_{sensor_type}")
           dataframes[sensor_type] = df
           
           print_progress(room_name, "process", f"{sensor_type} sensor prepared")

       print_progress(room_name, "process", "Merging data...")
       
       query = f"""
       SELECT
           FROM_UNIXTIME(CAST(df_co2.ts_min_bignt AS BIGINT)) as event_ts_min,
           df_co2.ts_min_bignt,
           '{room_name}' as room,
           CAST(df_co2.co2 AS DOUBLE) as co2,
           CAST(df_light.light AS DOUBLE) as light,
           CAST(df_temperature.temperature AS DOUBLE) as temp,
           CAST(df_humidity.humidity AS DOUBLE) as humidity,
           CAST(df_pir.pir AS DOUBLE) as pir
       FROM df_co2
           INNER JOIN df_humidity USING(ts_min_bignt)
           INNER JOIN df_light USING(ts_min_bignt)
           INNER JOIN df_pir USING(ts_min_bignt)
           INNER JOIN df_temperature USING(ts_min_bignt)
       ORDER BY ts_min_bignt
       """
       
       df = spark.sql(query)
       df = df.withColumn("ts_min_bignt", F.col("ts_min_bignt").cast(LongType()))

       COLUMNS_TO_SEND = [
           "event_ts_min", "ts_min_bignt", "room", 
           "co2", "light", "temp", "humidity", "pir"
       ]

       if save_training:
           try:
               log_message("Processing training data...", "ml")
               processed_df = df.select(COLUMNS_TO_SEND)
               
               if is_first_room:
                   log_message("Saving first room data to CSV...", "ml")
               else:
                   log_message("Appending data to existing CSV...", "ml")
               
               if save_to_csv(processed_df, is_first_room):
                   print_progress(room_name, "ml", "Training data added to CSV")
               
           except Exception as e:
               log_message(f"CSV processing error: {str(e)}", "error")

       kafka_df = df.select(COLUMNS_TO_SEND)
       kafka_df = kafka_df.select(
           F.to_json(F.struct([F.col(c).alias(c) for c in COLUMNS_TO_SEND])).alias("value")
       )

       print_progress(room_name, "write", "Sending data to Kafka...")
       
       retry_count = 3
       while retry_count > 0:
           try:
               kafka_df.write \
                   .format("kafka") \
                   .option("kafka.bootstrap.servers", "kafka:9092") \
                   .option("topic", "office-input") \
                   .save()
               
               log_message(f"Room {room_name} data successfully written to Kafka!", "success")
               break
               
           except Exception as e:
               retry_count -= 1
               if retry_count == 0:
                   raise e
               print_progress(room_name, "warning", f"Retrying... (Remaining: {retry_count})")
               time.sleep(2)

       for df in dataframes.values():
           df.unpersist()
       
       log_message(f"{'='*50}\n", "highlight")
       return True

   except Exception as e:
       log_message(f"Error processing Room {room_name}: {str(e)}", "error")
       return False
def main():
   print_banner()
   log_message("Initializing system...", "highlight")

   try:
       spark = (SparkSession.builder
               .appName("Sensor Data Reader")
               .master("local[2]")
               .config("spark.driver.memory", "2g")
               .config("spark.executor.memory", "2g")
               .config("spark.sql.shuffle.partitions", "10")
               .config("spark.default.parallelism", "10")
               .config("spark.memory.offHeap.enabled", "true")
               .config("spark.memory.offHeap.size", "1g")
               .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1")
               .getOrCreate())
       
       spark.sparkContext.setLogLevel("ERROR")
       log_message("Spark Session created successfully!", "success")

       directory_path = '/opt/final_project/KETI'
       first_iteration = True
       iteration = 1

       # Clean old CSV
       csv_path = "/opt/data-generator/input/sensors.csv"
       try:
           if os.path.exists(csv_path):
               os.remove(csv_path)
               log_message("Old CSV file cleaned", "info")
       except Exception as e:
           log_message(f"CSV cleanup error: {str(e)}", "warning")

       while True:
           try:
               log_message(f"\n{'#'*60}", "highlight")
               if first_iteration:
                   log_message("🤖 FIRST ITERATION - Creating Training Data...", "ml")
               log_message(f"Starting iteration {iteration}...", "highlight")
               log_message(f"{'#'*60}\n", "highlight")
               
               sorted_rooms = sorted(os.listdir(directory_path))
               for idx, room_name in enumerate(sorted_rooms):
                   if not os.path.isdir(os.path.join(directory_path, room_name)):
                       continue
                   
                   is_first_room = (idx == 0)
                   success = process_room_data(spark, room_name, directory_path,
                                            save_training=first_iteration,
                                            is_first_room=is_first_room)
                   
                   if success:
                       time.sleep(1)
                   else:
                       log_message(f"Error processing Room {room_name}, moving to next room...", "warning")
                       time.sleep(3)
               
               if first_iteration:
                   first_iteration = False
                   log_message("""
╔════════════════════════════════════════════════════════════╗
║                   🎯 IMPORTANT NOTICE                      ║
║--------------------------------------------------------- ║
║ ✅ Training data created successfully!                     ║
║ 📍 Location: /opt/data-generator/input/sensors.csv         ║
║ 💪 All room data combined                                 ║
║ ⚡ Model training can now begin!                          ║
╚════════════════════════════════════════════════════════════╝
                   """, "ml")
               
               log_message(f"\n{'='*60}", "highlight")
               log_message(f"Iteration {iteration} completed!", "success")
               log_message("Waiting 30 seconds...", "info")
               log_message(f"{'='*60}\n", "highlight")
               
               iteration += 1
               time.sleep(30)

           except Exception as e:
               log_message(f"Error during iteration {iteration}: {str(e)}", "error")
               log_message("Retrying in 5 seconds...", "warning")
               time.sleep(5)

   except KeyboardInterrupt:
       log_message("\nStopped by user.", "warning")
   except Exception as e:
       log_message(f"Critical system error: {str(e)}", "error")
   finally:
       if 'spark' in locals():
           spark.stop()
       log_message("System shut down.", "warning")

if __name__ == "__main__":
   main()