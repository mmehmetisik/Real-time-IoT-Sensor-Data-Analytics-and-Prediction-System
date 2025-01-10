import sys
import os
import warnings
import traceback
import logging
import findspark
import time
from datetime import datetime
from colorama import Fore, Back, Style, init

# Initialize Colorama
init()
findspark.init("/opt/spark")

from elasticsearch import Elasticsearch, helpers
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

def print_banner():
    """Displays visual banner at application start"""
    banner = f"""
{Fore.CYAN}
    ╔═══════════════════════════════════════════════════╗
    ║       SPARK STREAMING TO ELASTICSEARCH            ║
    ║                Version 3.3                        ║
    ║ ----------------------------------------         ║
    ║  📥 Kafka → 🔄 Spark → 💾 Elasticsearch → 📊 Kibana ║
    ╚═══════════════════════════════════════════════════╝
{Style.RESET_ALL}"""
    print(banner)

def log_message(message, level="info"):
    """Enhanced logging messages"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    colors = {
        "info": Fore.GREEN,
        "warning": Fore.YELLOW,
        "error": Fore.RED,
        "highlight": Fore.BLUE,
        "success": Fore.CYAN,
        "stats": Fore.MAGENTA
    }
    icons = {
        "info": "ℹ️",
        "warning": "⚠️",
        "error": "❌",
        "highlight": "🔍",
        "success": "✅",
        "stats": "📊"
    }
    print(f"{colors.get(level, Fore.WHITE)}[{timestamp}] {icons.get(level, '')} {message}{Style.RESET_ALL}")

def print_batch_report(batch_id, count, duration):
    """Displays visual batch processing report"""
    report = f"""
{Fore.CYAN}╔════════════════ BATCH PERFORMANCE REPORT ════════════════╗
║ 🆔 Batch ID    : {batch_id:<37} ║
║ 📊 Process Summary:                                    ║
║   ├── 📝 Total Records   : {count:<25} ║
║   └── ⏱️  Process Time    : {duration:.2f} seconds{' ':<16} ║
╚═════════════════════════════════════════════════════════╝{Style.RESET_ALL}
"""
    print(report)
# Program initialization
print_banner()
log_message("Initializing Elasticsearch Stream system...", "highlight")
warnings.filterwarnings('ignore')
checkpointDir = "file:///tmp/streaming/kafka_office_input"

# Initialize Spark Session
try:
    log_message("🚀 Creating Spark Session...")
    spark = (SparkSession.builder
             .appName("Streaming Kafka-Spark")
             .master("local[2]")
             .config("spark.driver.memory", "2g")
             .config("spark.executor.memory", "2g")
             .config("spark.sql.shuffle.partitions", "10")
             .config("spark.default.parallelism", "10")
             .config("spark.network.timeout", "800s")
             .config("spark.executor.heartbeatInterval", "60s")
             .config("spark.storage.blockManagerSlaveTimeoutMs", "800s")
             .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
             .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1")
             .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
             .getOrCreate())

    spark.sparkContext.setLogLevel("ERROR")
    log_message("✅ Spark Session created successfully!", "success")
except Exception as e:
    log_message(f"❌ Spark Session error: {str(e)}", "error")
    traceback.print_exc()
    sys.exit(1)

# Kafka data read configuration
try:
    log_message("📥 Establishing Kafka connection...")
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "office-input") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .option("maxOffsetsPerTrigger", 1000) \
        .load()
    log_message("✅ Kafka connection successful!", "success")
except Exception as e:
    log_message(f"❌ Kafka connection error: {str(e)}", "error")
    sys.exit(1)

# Elasticsearch connection and index configuration
try:
    log_message("📡 Establishing Elasticsearch connection...")
    es = Elasticsearch(
        ["http://es:9200"],
        verify_certs=False,
        timeout=30,
        retry_on_timeout=True,
        max_retries=3
    )
    
    # Check and delete old index
    if es.indices.exists(index="office_input"):
        log_message("⚠️ Found old index, deleting...", "warning")
        es.indices.delete(index="office_input")
        time.sleep(2)

    # New index mapping
    index_mapping = {
        "mappings": {
            "properties": {
                "event_ts_min": {"type": "date"},
                "co2": {"type": "float"},
                "humidity": {"type": "float"},
                "light": {"type": "float"},
                "temperature": {"type": "float"},
                "room": {"type": "keyword"},
                "pir": {"type": "float"},
                "if_movement": {"type": "keyword"}
            }
        },
        "settings": {
            "number_of_shards": 2,
            "number_of_replicas": 0,
            "refresh_interval": "1s"
        }
    }

    es.indices.create(index="office_input", body=index_mapping)
    log_message("✅ Elasticsearch index created successfully!", "success")

except Exception as e:
    log_message(f"❌ Elasticsearch configuration error: {str(e)}", "error")
    sys.exit(1)
# Data transformations
log_message("🔄 Starting data processing pipeline...")

# Parse JSON data
df2 = kafka_df.selectExpr("CAST(value AS STRING)")

# Define sensor schema
schema = StructType([
    StructField("event_ts_min", TimestampType(), True),
    StructField("ts_min_bignt", LongType(), True),
    StructField("room", StringType(), True),
    StructField("co2", FloatType(), True),
    StructField("light", FloatType(), True),
    StructField("temp", FloatType(), True),
    StructField("humidity", FloatType(), True),
    StructField("pir", FloatType(), True)
])

# Extract and configure data from JSON
df3 = df2.select(F.from_json(F.col("value"), schema).alias("data")).select("data.*")
df4 = df3.withColumn("if_movement", F.when(F.col("pir") > 0.0, "movement").otherwise("no_movement"))

def process_batch(batch_df, batch_id):
    """Writes each batch to Elasticsearch"""
    batch_start_time = time.time()
    retry_count = 3
    
    while retry_count > 0:
        try:
            log_message(f"\n{'='*50}", "highlight")
            log_message(f"🔄 Processing Batch {batch_id}...", "info")
            
            rows = batch_df.collect()
            bulk_data = []
            chunk_size = 500
            total_rows = len(rows)
            processed_rows = 0

            for row in rows:
                try:
                    # Create Elasticsearch document for each record
                    doc = {
                        "event_ts_min": row.event_ts_min,
                        "co2": float(row.co2) if row.co2 is not None else 0.0,
                        "humidity": float(row.humidity) if row.humidity is not None else 0.0,
                        "light": float(row.light) if row.light is not None else 0.0,
                        "temperature": float(row.temp) if row.temp is not None else 0.0,
                        "room": row.room,
                        "pir": float(row.pir) if row.pir is not None else 0.0,
                        "if_movement": row.if_movement
                    }
                    
                    bulk_data.append({
                        '_index': 'office_input',
                        '_source': doc
                    })

                    processed_rows += 1
                    # Bulk write when chunk size is reached
                    if len(bulk_data) >= chunk_size:
                        success, failed = helpers.bulk(es, bulk_data, stats_only=True)
                        log_message(f"✅ Chunk written ({processed_rows}/{total_rows})", "success")
                        bulk_data = []

                except Exception as row_error:
                    log_message(f"⚠️ Row processing error: {str(row_error)}", "warning")
                    continue

            # Process remaining data
            if bulk_data:
                success, failed = helpers.bulk(es, bulk_data, stats_only=True)
                log_message(f"✅ Final chunk written (Total: {total_rows})", "success")

            batch_duration = time.time() - batch_start_time
            print_batch_report(batch_id, total_rows, batch_duration)
            log_message(f"{'='*50}\n", "highlight")
            break

        except Exception as e:
            retry_count -= 1
            if retry_count == 0:
                log_message(f"❌ Maximum retry count reached: {str(e)}", "error")
                raise e
            log_message(f"⚠️ Retrying batch. Remaining: {retry_count}", "warning")
            time.sleep(5)

# Start streaming process
try:
    query = (df4.writeStream
             .foreachBatch(process_batch)
             .option("checkpointLocation", checkpointDir)
             .trigger(processingTime='5 seconds')
             .start())

    log_message("""
    ✨ Stream started successfully!
    📊 Data flow details:
    ├── 📥 Source: Kafka (office-input topic)
    ├── 💾 Target: Elasticsearch (office_input index)
    ├── ⚡ Trigger: 5 seconds
    └── 🔄 Checkpoint: /tmp/streaming/kafka_office_input
    """, "success")

    query.awaitTermination()

except Exception as e:
    log_message(f"❌ Stream startup error: {str(e)}", "error")
    sys.exit(1)

log_message("✨ Process completed!", "success")