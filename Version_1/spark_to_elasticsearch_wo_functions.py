import sys
import os
import warnings
import traceback
import logging
import findspark
import time
from datetime import datetime
from colorama import Fore, Back, Style, init

init()

findspark.init("/opt/spark")
from elasticsearch import Elasticsearch, helpers
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

def print_banner():
    banner = f"""
{Fore.CYAN}
    ╔═══════════════════════════════════════════════════╗
    ║       SPARK STREAMING TO ELASTICSEARCH            ║
    ║                Version 3.1                        ║
    ║ ----------------------------------------         ║
    ║  📥 Kafka → 🔄 Spark → 💾 Elasticsearch → 📊 Kibana ║
    ╚═══════════════════════════════════════════════════╝
{Style.RESET_ALL}"""
    print(banner)

def log_message(message, level="info"):
    timestamp = datetime.now().strftime("%H:%M:%S")
    colors = {
        "info": Fore.GREEN,
        "warning": Fore.YELLOW,
        "error": Fore.RED,
        "highlight": Fore.BLUE,
        "success": Fore.CYAN
    }
    print(f"{colors.get(level, Fore.WHITE)}[{timestamp}] {message}{Style.RESET_ALL}")

def print_batch_report(batch_id, count, duration, rate):
    report = f"""
{Fore.CYAN}╔════════════════ BATCH PERFORMANS RAPORU ════════════════╗
║ 🆔 Batch ID    : {batch_id:<37} ║
║ 📊 İşlem Özeti:                                        ║
║   ├── 📝 Toplam Kayıt  : {count:<28} ║
║   └── ⏱️  İşlem Süresi : {duration:.2f} saniye{' ':<20} ║
╚═════════════════════════════════════════════════════════╝{Style.RESET_ALL}
"""
    print(report)

print_banner()
log_message("Sistem başlatılıyor...", "highlight")
warnings.filterwarnings('ignore')
checkpointDir = "file:///tmp/streaming/kafka_office_input"

# Spark Session başlatma
try:
    log_message("🚀 Spark Session oluşturuluyor...")
    spark = (SparkSession.builder
             .appName("Streaming Kafka-Spark")
             .master("local[2]")
             .config("spark.driver.memory", "4g")
             .config("spark.executor.memory", "4g")
             .config("spark.sql.shuffle.partitions", "30")
             .config("spark.default.parallelism", "30")
             .config("spark.network.timeout", "800s")
             .config("spark.executor.heartbeatInterval", "60s")
             .config("spark.storage.blockManagerSlaveTimeoutMs", "800s")
             .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
             .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1")
             .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
             .config("spark.memory.fraction", "0.6")
             .config("spark.memory.storageFraction", "0.3")
             .getOrCreate())

    log_message("✅ Spark Session başarıyla oluşturuldu!", "success")
except Exception:
    traceback.print_exc(file=sys.stderr)
    log_message("❌ Spark Session oluşturma hatası!", "error")
    sys.exit(1)
# Kafka'dan veri okuma
try:
    log_message("📥 Kafka bağlantısı kuruluyor...")
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "office-input") \
        .option("startingOffsets", "latest") \
        .load()
    log_message("✅ Kafka bağlantısı başarılı!", "success")
except Exception as e:
    log_message(f"❌ Kafka bağlantı hatası: {e}", "error")
    sys.exit(1)

# Veri dönüşümleri
log_message("🔄 Veri dönüşümleri başlatılıyor...")

df2 = df.selectExpr("CAST(value AS STRING)")
df3 = df2.withColumn("timestamp", F.split(F.col("value"), ",")[0]) \
    .withColumn("ts_min_bignt", F.split(F.col("value"), ",")[1].cast(IntegerType())) \
    .withColumn("room", F.split(F.col("value"), ",")[2]) \
    .withColumn("co2", F.split(F.col("value"), ",")[3].cast(FloatType())) \
    .withColumn("light", F.split(F.col("value"), ",")[4].cast(FloatType())) \
    .withColumn("temp", F.split(F.col("value"), ",")[5].cast(FloatType())) \
    .withColumn("humidity", F.split(F.col("value"), ",")[6].cast(FloatType())) \
    .withColumn("pir", F.split(F.col("value"), ",")[7].cast(FloatType())) \
    .withColumn("event_ts_min", 
        F.when(F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd HH:mm:ss").isNull(),
               F.current_timestamp().cast("long") * 1000)
        .otherwise(F.unix_timestamp(
            F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd HH:mm:ss")
        ).cast("long") * 1000)) \
    .drop(F.col("value")) \
    .drop(F.col("timestamp"))

df3.createOrReplaceTempView("df3")
log_message("✅ Veri dönüşümleri tamamlandı", "success")

# Hareket durumu hesaplama
log_message("🔄 Hareket analizi yapılıyor...")
df4 = spark.sql("""
   SELECT
       event_ts_min,
       co2,
       humidity,
       light,
       temp,
       room,
       pir,
       CASE
           WHEN pir > 0.0 THEN 'movement'
           ELSE 'no_movement'
       END as if_movement
   FROM df3
""")
# Elasticsearch bağlantısı ve index oluşturma
try:
    log_message("📡 Elasticsearch bağlantısı kuruluyor...")
    es = Elasticsearch(
        ["http://es:9200"],
        verify_certs=False,
        timeout=30,
        retry_on_timeout=True,
        max_retries=3
    )
    
    # Index kontrolü ve mapping
    if es.indices.exists(index="office_input"):
        es.indices.delete(index="office_input")
        log_message("🗑️ Eski index silindi", "warning")
        time.sleep(2)

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
    log_message("✅ Index başarıyla oluşturuldu!", "success")

except Exception as e:
    log_message(f"❌ Elasticsearch işlem hatası: {e}", "error")
    sys.exit(1)

# Streaming işlemi
log_message("\n🚀 Streaming başlatılıyor...")

def process_batch(batch_df, batch_id):
    batch_start_time = time.time()
    
    try:
        rows = batch_df.collect()
        bulk_data = []
        chunk_size = 500
        total_rows = len(rows)
        processed_rows = 0

        for row in rows:
            try:
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
                if len(bulk_data) >= chunk_size:
                    success, failed = helpers.bulk(es, bulk_data, stats_only=True)
                    log_message(f"✅ Chunk yazıldı - Başarılı: {success}, Başarısız: {failed}", "success")
                    bulk_data = []

            except Exception as row_error:
                log_message(f"⚠️ Satır dönüşüm hatası: {str(row_error)}", "warning")
                continue

        if bulk_data:
            success, failed = helpers.bulk(es, bulk_data, stats_only=True)
            log_message(f"✅ Son chunk yazıldı - Başarılı: {success}, Başarısız: {failed}", "success")

        batch_duration = time.time() - batch_start_time
        print_batch_report(batch_id, total_rows, batch_duration, total_rows/batch_duration)

    except Exception as e:
        log_message(f"❌ Batch işleme hatası: {str(e)}", "error")
        raise e

try:
    query = (df4.writeStream
             .foreachBatch(process_batch)
             .option("checkpointLocation", checkpointDir)
             .trigger(processingTime='20 seconds')
             .start())

    log_message("""
    ✨ Stream başarıyla başlatıldı!
    📊 Veri akışı detayları:
    ├── 📥 Kaynak: Kafka (office-input topic)
    ├── 💾 Hedef: Elasticsearch (office_input index)
    └── 🔄 Checkpoint: /tmp/streaming/kafka_office_input
    """, "success")

    query.awaitTermination()

except Exception as e:
    log_message(f"❌ Stream başlatma hatası: {e}", "error")
    sys.exit(1)

log_message("✨ İşlem tamamlandı!", "success")