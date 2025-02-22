import os
import subprocess
from functools import reduce
import findspark
import time
from datetime import datetime
from tqdm import tqdm
from colorama import Fore, Back, Style, init

init()

findspark.init("/opt/spark")
from pyspark.sql import SparkSession, functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import *

def print_banner():
   """Print ASCII banner"""
   banner = f"""
{Fore.CYAN}
╔══════════════════════════════════════════════════════════════╗
║             SENSOR DATA PROCESSING SYSTEM                    ║
║                      Version 3.1                            ║
║----------------------------------------------------------  ║
║    🔄 Preprocessing  |  📊 Analysis  |  💾 CSV Export        ║
╚══════════════════════════════════════════════════════════════╝
{Style.RESET_ALL}"""
   print(banner)

def log_message(message, level="info", indent=0):
   """Enhanced colored log messages"""
   timestamp = datetime.now().strftime("%H:%M:%S")
   colors = {
       "info": Fore.GREEN,
       "warning": Fore.YELLOW,
       "error": Fore.RED + Back.WHITE,
       "highlight": Fore.BLUE,
       "success": Fore.CYAN,
       "processing": Fore.MAGENTA
   }
   
   icons = {
       "info": "ℹ️",
       "warning": "⚠️",
       "error": "❌",
       "highlight": "🔍",
       "success": "✅",
       "processing": "🔄"
   }
   
   indent_str = "  " * indent
   print(f"{colors.get(level, Fore.WHITE)}[{timestamp}] {icons.get(level, '')} {indent_str}{message}{Style.RESET_ALL}")

def format_table_output(df, num_rows=5):
   """Print DataFrame output in formatted table"""
   rows = df.limit(num_rows).collect()
   if not rows:
       return "No data found."
   
   # Define column widths
   col_widths = {
       'event_ts_min': 19,
       'ts_min_bignt': 12,
       'room': 6,
       'co2': 12,
       'light': 12,
       'temp': 12,
       'humidity': 12,
       'pir': 12
   }
   
   # Header row
   header = ""
   for col in df.columns:
       header += f"{col:<{col_widths[col]}} "
   
   # Data rows
   rows_str = []
   for row in rows:
       row_str = ""
       for col in df.columns:
           value = row[col]
           if isinstance(value, float):
               row_str += f"{value:>{col_widths[col]}.6f} "
           else:
               row_str += f"{str(value):<{col_widths[col]}} "
       rows_str.append(row_str)
   
   return header + "\n" + "\n".join(rows_str)

# Process start time
start_time = time.time()

# Print banner
print_banner()

log_message("🚀 Starting Spark Session...", "highlight")

# Initialize SparkSession with optimizations
spark_session = SparkSession.builder \
   .appName("Sensor Data Processing") \
   .master("local[3]") \
   .config("spark.driver.memory", "3g") \
   .config("spark.executor.memory", "3g") \
   .config("spark.sql.shuffle.partitions", 75) \
   .config("spark.default.parallelism", 75) \
   .config("spark.network.timeout", "800s") \
   .config("spark.executor.heartbeatInterval", "60s") \
   .config("spark.storage.blockManagerSlaveTimeoutMs", "800s") \
   .config("spark.sql.autoBroadcastJoinThreshold", -1) \
   .getOrCreate()

log_message("✨ Spark Session created successfully!", "success")

# Data structures and variables
room_data = {}
directory_path = '/opt/final_project/KETI'
dataframes_per_room = {}
sensor_columns = ['co2', 'humidity', 'light', 'pir', 'temperature']

# Checkpoint directory
spark_session.sparkContext.setCheckpointDir("/tmp/checkpoint")

# CSV reading schema
schema = StructType([
   StructField("ts_min_bignt", StringType(), True),
   StructField("sensor_value", StringType(), True)
])

log_message("\n📂 Starting data reading process...", "highlight")
log_message(f"└── Source Directory: {directory_path}", "info", indent=1)

# Get total folder count
total_folders = len([f for f in os.listdir(directory_path)])
log_message(f"📊 Total Folders to Process: {total_folders}", "highlight")

# Process each folder
for folder_idx, folder_name in enumerate(sorted(os.listdir(directory_path)), 1):
   folder_start_time = time.time()
   
   log_message(f"\n{'='*50}", "highlight")
   log_message(f"📁 Processing Folder [{folder_idx}/{total_folders}]: {folder_name}", "processing")
   
   folder_path = os.path.join(directory_path, folder_name)
   sensor_files = ['co2.csv', 'humidity.csv', 'light.csv', 'pir.csv', 'temperature.csv']
   
   for i, file_name in enumerate(tqdm(sensor_files, 
                                    desc=f"{Fore.CYAN}💾 Sensor Files{Style.RESET_ALL}",
                                    bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]")):
       file_path = os.path.join(folder_path, file_name)
       if not os.path.exists(file_path):
           continue
           
       df_key = f"{folder_name}_{file_name.split('.')[0]}"
       room_data[df_key] = spark_session.read \
           .option("delimiter", ",") \
           .schema(schema) \
           .csv(file_path)
       
       room_data[df_key] = room_data[df_key] \
           .withColumn("ts_min_bignt", F.trim(F.col("ts_min_bignt"))) \
           .withColumn("sensor_value", F.trim(F.col("sensor_value"))) \
           .withColumnRenamed("sensor_value", sensor_columns[i])
       
       room_data[df_key].createOrReplaceTempView(f"df_{file_name.split('.')[0]}")

   # SQL joins
   query = f"""
   SELECT
      FROM_UNIXTIME(CAST(df_co2.ts_min_bignt AS BIGINT), 'yyyy-MM-dd HH:mm:ss') as event_ts_min,
      CAST(df_co2.ts_min_bignt AS BIGINT) as ts_min_bignt,
      '{folder_name}' as room,
      ROUND(CAST(df_co2.co2 AS DOUBLE), 6) as co2,
      ROUND(CAST(df_light.light AS DOUBLE), 6) as light,
      ROUND(CAST(df_temperature.temperature AS DOUBLE), 6) as temp,
      ROUND(CAST(df_humidity.humidity AS DOUBLE), 6) as humidity,
      ROUND(CAST(df_pir.pir AS DOUBLE), 6) as pir
   FROM
      df_co2
      INNER JOIN df_humidity ON df_co2.ts_min_bignt = df_humidity.ts_min_bignt
      INNER JOIN df_light ON df_humidity.ts_min_bignt = df_light.ts_min_bignt
      INNER JOIN df_pir ON df_light.ts_min_bignt = df_pir.ts_min_bignt
      INNER JOIN df_temperature ON df_pir.ts_min_bignt = df_temperature.ts_min_bignt
   """
   
   dataframes_per_room[folder_name] = spark_session.sql(query)

# Merge all data
log_message("\n🔄 Merging data from all rooms...", "processing")
df_merged = reduce(DataFrame.unionAll, dataframes_per_room.values()).dropna()
df_merged = df_merged.cache()

# Write to CSV
output_path = "/opt/data-generator/input/sensors.csv"
log_message(f"\n💾 Writing data to CSV: {output_path}", "processing")

df_merged.coalesce(1).write \
   .mode("overwrite") \
   .option("header", "true") \
   .option("delimiter", ",") \
   .csv("/tmp/output_dir")

subprocess.run(f"cat /tmp/output_dir/part-* > {output_path}", shell=True)

# Final output and statistics
log_message("\n📊 Sample Data:", "highlight")
print("\n" + format_table_output(df_merged) + "\n")

duration = time.time() - start_time
total_rows = df_merged.count()

print(f"""
{Fore.CYAN}╔═════════════════════════════════════════════════════╗
║                  PROCESS RESULTS                       ║
╚═════════════════════════════════════════════════════════╝{Style.RESET_ALL}
""")

log_message(f"""
✨ Process Completed Successfully!
├── ⏱️ Total Processing Time: {duration / 60:.2f} minutes
├── 📊 Total Row Count: {total_rows:,}
└── 💾 Output File: {output_path}
""", "success")

spark_session.stop()