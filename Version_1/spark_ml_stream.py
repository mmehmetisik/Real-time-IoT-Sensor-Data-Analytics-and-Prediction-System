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

# Temel inizializasyonlar
init()
findspark.init("/opt/spark")

# Spark ve Kafka loglarını sustur
logging.getLogger("org.apache.spark").setLevel(logging.ERROR)
logging.getLogger("org.apache.kafka").setLevel(logging.ERROR)

def print_banner():
    """Renkli banner yazdırma"""
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
    """Batch işlem istatistiklerini yazdır"""
    print(f"\n{Fore.CYAN}{'='*60}")
    stats = f"""
    📊 BATCH {batch_id} ÖZET
    ├── 🏃 Hareket Tespit    : {movement_count}
    ├── 🚫 Hareketsiz Tespit : {no_movement_count}
    └── ⏱️  İşlem Süresi     : {duration:.2f} saniye
    """
    print(stats)
    print(f"{'='*60}{Style.RESET_ALL}\n")

def log_message(message, level="info", indent=0):
    """Renkli log mesajları"""
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

# Ana program başlangıcı
print_banner()
log_message("🚀 ML Stream işlemi başlatılıyor...", "highlight")

# Spark Session başlatma
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
    
    # Log seviyesini ayarla
    spark.sparkContext.setLogLevel("ERROR")
    log_message("✅ Spark Session başarıyla oluşturuldu!", "success")
except Exception as e:
    log_message(f"❌ Spark Session hatası: {str(e)}", "error")
    sys.exit(1)

# Kafka'dan veri okuma
try:
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "office-input") \
        .option("startingOffsets", "latest") \
        .load()
    log_message("✅ Kafka bağlantısı başarılı!", "success")
except Exception as e:
    log_message(f"❌ Kafka bağlantı hatası: {str(e)}", "error")
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
    .withColumn("pir", F.split(F.col("value"), ",")[7].cast(FloatType()))

# Feature vector oluşturma
assembler = VectorAssembler(
    inputCols=["co2", "light", "temp", "humidity"],
    outputCol="features"
)
vectorized_df = assembler.transform(df3)

# ML modelini yükleme
try:
    model = LogisticRegressionModel.load("/opt/spark/ml_model")
    log_message("✅ Model başarıyla yüklendi!", "success")
except Exception as e:
    log_message(f"❌ Model yükleme hatası: {str(e)}", "error")
    sys.exit(1)

# Tahmin yapma
predictions = model.transform(vectorized_df)

def process_batch(batch_df, batch_id):
    """Her bir batch için işleme ve Kafka'ya yazma"""
    start_time = time.time()
    try:
        # Timestamp'i düzenle
        batch_df = batch_df.withColumn(
            "event_ts_min", 
            F.from_unixtime(F.col("ts_min_bignt")).cast("timestamp")
        )
        
        # Vector'den probability değerini al
        batch_df = batch_df.withColumn(
            "confidence",
            F.udf(lambda x: float(x[1]), FloatType())(F.col("probability"))
        )
        
        # Gereksiz sütunları kaldır
        columns_to_drop = ["probability", "features", "rawPrediction", "value"]
        batch_df = batch_df.drop(*columns_to_drop)
        
        # Hareket olan ve olmayan verileri ayır
        movement_df = batch_df.filter(F.col("prediction") == 1.0)
        no_movement_df = batch_df.filter(F.col("prediction") == 0.0)
        
        # Göndermek istediğimiz sütunlar
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
        
        # Movement verileri gönder
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
        
        # No movement verileri gönder
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
        
        # Batch istatistiklerini göster
        duration = time.time() - start_time
        print_batch_stats(batch_id, movement_count, no_movement_count, duration)
        
    except Exception as e:
        log_message(f"❌ Batch işleme hatası: {str(e)}", "error")
        raise e

# Streaming işlemi başlatma
try:
    query = (predictions.writeStream
             .foreachBatch(process_batch)
             .option("checkpointLocation", "/tmp/kafka_ml_checkpoint")
             .trigger(processingTime='10 seconds')
             .start())
    
    log_message("""
    ✨ Stream başarıyla başlatıldı!
    ├── 📥 Input: Kafka (office-input)
    ├── 🤖 Model: Logistic Regression
    ├── 📤 Output 1: Kafka (office-activity)
    └── 📤 Output 2: Kafka (office-no-activity)
    """, "success")
    
    query.awaitTermination()
    
except Exception as e:
    log_message(f"❌ Stream başlatma hatası: {str(e)}", "error")
    sys.exit(1)