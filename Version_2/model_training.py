import sys
import os
import warnings
import shutil
import findspark
import time
from datetime import datetime
from colorama import Fore, Back, Style, init
from tqdm import tqdm
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.sql.types import *
import pyspark.sql.functions as F

init()
findspark.init("/opt/spark")

def print_banner():
   """Print banner"""
   banner = f"""
{Fore.CYAN}
╔═══════════════════════════════════════════════════════╗
║             SENSOR MOTION ML MODEL                    ║
║                 Training v1.0                         ║
║ --------------------------------------------------- ║
║  📊 Data Prep | 🤖 Model Training | 📈 Performance      ║
╚═══════════════════════════════════════════════════════╝
{Style.RESET_ALL}"""
   print(banner)

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

def setup_model_directory(model_path):
   """Prepare model directory"""
   try:
       if os.path.exists(model_path):
           log_message(f"Found existing model directory: {model_path}", "warning")
           shutil.rmtree(model_path)
           log_message("Cleaned old model", "success", indent=1)
       os.makedirs(model_path, exist_ok=True)
       log_message("Model directory prepared", "success")
       return True
   except Exception as e:
       log_message(f"Directory preparation error: {str(e)}", "error")
       return False

def print_performance_report(total, correct, accuracy):
   """Print model performance report"""
   report = f"""
{Fore.CYAN}╔════════════════ MODEL PERFORMANCE REPORT ════════════════╗
║ 📊 Model Evaluation:                                      ║
║   ├── 📝 Total Records : {total:<28} ║
║   ├── ✅ Correct Pred. : {correct:<28} ║
║   └── 🎯 Accuracy     : {accuracy:.2f}%{' ':<26} ║
╚═════════════════════════════════════════════════════════╝{Style.RESET_ALL}
"""
   print(report)

def main():
   print_banner()
   log_message("Starting ML model training...", "highlight")

   model_path = "/opt/spark/ml_model"
   if not setup_model_directory(model_path):
       sys.exit(1)

   try:
       spark = SparkSession.builder \
           .appName("ML Model Training") \
           .master("local[2]") \
           .config("spark.driver.memory", "4g") \
           .config("spark.executor.memory", "4g") \
           .getOrCreate()
       log_message("Spark Session created successfully!", "success")
   except Exception as e:
       log_message(f"Spark Session error: {str(e)}", "error")
       sys.exit(1)

   schema = StructType([
       StructField("event_ts_min", StringType(), True),
       StructField("ts_min_bignt", IntegerType(), True),
       StructField("room", StringType(), True),
       StructField("co2", FloatType(), True),
       StructField("light", FloatType(), True),
       StructField("temp", FloatType(), True),
       StructField("humidity", FloatType(), True),
       StructField("pir", FloatType(), True)
   ])

   try:
       log_message("Reading training data...", "info")
       df = spark.read.csv("/opt/data-generator/input/sensors.csv", 
                          schema=schema, header=True)
       total_records = df.count()
       log_message(f"Read {total_records:,} records", "success", indent=1)
   except Exception as e:
       log_message(f"Data reading error: {str(e)}", "error")
       sys.exit(1)

   log_message("Starting data preprocessing...", "info")
   with tqdm(total=3, desc=f"{Fore.CYAN}Data Preparation{Style.RESET_ALL}", 
             bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}") as pbar:
       
       df = df.withColumn("label", F.when(F.col("pir") > 0, 1.0).otherwise(0.0))
       pbar.update(1)
       
       assembler = VectorAssembler(
           inputCols=["co2", "light", "temp", "humidity"],
           outputCol="features"
       )
       pbar.update(1)
       
       training_data = assembler.transform(df)
       pbar.update(1)

   log_message("Starting model training...", "highlight")
   try:
       lr = LogisticRegression(
           featuresCol="features",
           labelCol="label",
           maxIter=10
       )
       
       with tqdm(total=100, desc=f"{Fore.CYAN}Model Training{Style.RESET_ALL}", 
                 bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}") as pbar:
           model = lr.fit(training_data)
           pbar.update(100)
       
       model.write().overwrite().save(model_path)
       log_message("Model saved successfully!", "success")
   except Exception as e:
       log_message(f"Model training error: {str(e)}", "error")
       sys.exit(1)

   log_message("Evaluating model performance...", "info")
   predictions = model.transform(training_data)
   total = predictions.count()
   correct = predictions.filter(F.col("prediction") == F.col("label")).count()
   accuracy = (correct / total) * 100

   print_performance_report(total, correct, accuracy)

   log_message("""
   ✨ Model training completed successfully!
   ├── 💾 Model Location: /opt/spark/ml_model
   └── 📊 Accuracy: {:.2f}%
   """.format(accuracy), "success")

if __name__ == "__main__":
   main()