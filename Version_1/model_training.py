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

# Colorama başlatma
init()
findspark.init("/opt/spark")

def print_banner():
    """Banner yazdırma"""
    banner = f"""
{Fore.CYAN}
╔═══════════════════════════════════════════════════════╗
║             SENSÖR HAREKETİ ML MODELİ                ║
║                   Eğitim v1.0                        ║
║ --------------------------------------------------- ║
║  📊 Veri Hazırlama | 🤖 Model Eğitimi | 📈 Performans  ║
╚═══════════════════════════════════════════════════════╝
{Style.RESET_ALL}"""
    print(banner)

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

def setup_model_directory(model_path):
    """Model dizini hazırlama"""
    try:
        if os.path.exists(model_path):
            log_message(f"Eski model dizini bulundu: {model_path}", "warning")
            shutil.rmtree(model_path)
            log_message("Eski model temizlendi", "success", indent=1)
        os.makedirs(model_path, exist_ok=True)
        log_message("Model dizini hazırlandı", "success")
        return True
    except Exception as e:
        log_message(f"Dizin hazırlama hatası: {str(e)}", "error")
        return False

def print_performance_report(total, correct, accuracy):
    """Model performans raporu yazdırma"""
    report = f"""
{Fore.CYAN}╔════════════════ MODEL PERFORMANS RAPORU ════════════════╗
║ 📊 Model Değerlendirmesi:                                 ║
║   ├── 📝 Toplam Veri    : {total:<28} ║
║   ├── ✅ Doğru Tahmin   : {correct:<28} ║
║   └── 🎯 Doğruluk Oranı : %{accuracy:.2f}{' ':<26} ║
╚═════════════════════════════════════════════════════════╝{Style.RESET_ALL}
"""
    print(report)

def main():
    # Banner yazdır
    print_banner()
    log_message("ML model eğitimi başlatılıyor...", "highlight")

    # Model dizinini hazırla
    model_path = "/opt/spark/ml_model"
    if not setup_model_directory(model_path):
        sys.exit(1)

    # Spark Session başlat
    try:
        spark = SparkSession.builder \
            .appName("ML Model Training") \
            .master("local[2]") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .getOrCreate()
        log_message("Spark Session başarıyla oluşturuldu!", "success")
    except Exception as e:
        log_message(f"Spark Session hatası: {str(e)}", "error")
        sys.exit(1)

    # Veri şeması
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

    # Veri okuma
    try:
        log_message("Eğitim verisi okunuyor...", "info")
        df = spark.read.csv("/opt/data-generator/input/sensors.csv", 
                           schema=schema, header=True)
        total_records = df.count()
        log_message(f"Toplam {total_records:,} kayıt okundu", "success", indent=1)
    except Exception as e:
        log_message(f"Veri okuma hatası: {str(e)}", "error")
        sys.exit(1)

    # Veri hazırlama
    log_message("Veri ön işleme başlatılıyor...", "info")
    with tqdm(total=3, desc=f"{Fore.CYAN}Veri Hazırlama{Style.RESET_ALL}", 
              bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}") as pbar:
        
        # Label oluştur
        df = df.withColumn("label", F.when(F.col("pir") > 0, 1.0).otherwise(0.0))
        pbar.update(1)
        
        # Feature'ları birleştir
        assembler = VectorAssembler(
            inputCols=["co2", "light", "temp", "humidity"],
            outputCol="features"
        )
        pbar.update(1)
        
        # Eğitim verisi hazırla
        training_data = assembler.transform(df)
        pbar.update(1)

    # Model eğitimi
    log_message("Model eğitimi başlatılıyor...", "highlight")
    try:
        lr = LogisticRegression(
            featuresCol="features",
            labelCol="label",
            maxIter=10
        )
        
        with tqdm(total=100, desc=f"{Fore.CYAN}Model Eğitimi{Style.RESET_ALL}", 
                  bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}") as pbar:
            model = lr.fit(training_data)
            pbar.update(100)
        
        # Modeli kaydet
        model.write().overwrite().save(model_path)
        log_message("Model başarıyla kaydedildi!", "success")
    except Exception as e:
        log_message(f"Model eğitim hatası: {str(e)}", "error")
        sys.exit(1)

    # Model performansı
    log_message("Model performansı değerlendiriliyor...", "info")
    predictions = model.transform(training_data)
    total = predictions.count()
    correct = predictions.filter(F.col("prediction") == F.col("label")).count()
    accuracy = (correct / total) * 100

    # Performans raporu
    print_performance_report(total, correct, accuracy)

    # Başarılı tamamlanma mesajı
    log_message("""
    ✨ Model eğitimi başarıyla tamamlandı!
    ├── 💾 Model Konumu: /opt/spark/ml_model
    └── 📊 Doğruluk Oranı: {:.2f}%
    """.format(accuracy), "success")

if __name__ == "__main__":
    main()