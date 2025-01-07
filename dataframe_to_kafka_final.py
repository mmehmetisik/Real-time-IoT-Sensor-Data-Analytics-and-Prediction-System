import pandas as pd
from kafka import KafkaProducer
import time
import argparse
from tqdm import tqdm
from colorama import Fore, Back, Style, init
from datetime import datetime

init()

def print_banner():
    """Kafka Producer Banner"""
    banner = f"""
{Fore.CYAN}
╔════════════════════════════════════════════════════════════╗
║             SENSÖR VERİSİ KAFKA PRODUCER                   ║
║                     Version 2.0                            ║
║ -------------------------------------------------------- ║
║    📤 CSV Okuma  |  🔄 Kafka Streaming  |  📊 Monitoring    ║
╚════════════════════════════════════════════════════════════╝
{Style.RESET_ALL}"""
    print(banner)

def log_message(message, level="info"):
    """Renkli log mesajları"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    colors = {
        "info": Fore.GREEN,
        "warning": Fore.YELLOW,
        "error": Fore.RED,
        "success": Fore.CYAN
    }
    icons = {
        "info": "ℹ️",
        "warning": "⚠️",
        "error": "❌",
        "success": "✅"
    }
    print(f"{colors.get(level, Fore.WHITE)}[{timestamp}] {icons.get(level, '')} {message}{Style.RESET_ALL}")
class DataFrameToKafka:
    def __init__(self, input, sep, kafka_sep, row_sleep_time, source_file_extension, bootstrap_servers,
                 topic, repeat, shuffle, key_index, excluded_cols):
        log_message("Producer başlatılıyor...", "info")
        self.input = input
        self.sep = sep
        self.kafka_sep = kafka_sep
        self.row_sleep_time = row_sleep_time
        self.repeat = repeat
        self.shuffle = shuffle
        self.excluded_cols = excluded_cols
        self.df = self.read_source_file(source_file_extension)
        self.topic = topic
        self.key_index = key_index
        
        # Parametre özeti
        log_message("Yapılandırma parametreleri:", "info")
        print(f"{Fore.CYAN}├── 📁 Input: {self.input}")
        print(f"├── 📋 Topic: {self.topic}")
        print(f"├── ⏱️ Sleep Time: {self.row_sleep_time}")
        print(f"├── 🔄 Repeat: {self.repeat}")
        print(f"└── 🔌 Bootstrap Servers: {bootstrap_servers}{Style.RESET_ALL}")
        
        try:
            self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
            log_message("Kafka bağlantısı başarılı!", "success")
        except Exception as e:
            log_message(f"Kafka bağlantı hatası: {str(e)}", "error")
            raise

    def turn_df_to_str(self, df):
        """DataFrame'i string formatına dönüştür"""
        x = df.values.astype(str)
        vals = [self.kafka_sep.join(ele) for ele in x]
        return vals

    def read_source_file(self, extension='csv'):
        """Kaynak dosyayı oku"""
        log_message(f"Veri dosyası okunuyor: {self.input}", "info")
        try:
            if extension == 'csv':
                df = pd.read_csv(self.input, sep=self.sep, low_memory=False)
                if self.shuffle:
                    df = df.sample(frac=1)
            else:
                df = pd.read_parquet(self.input, 'auto')
                if self.shuffle:
                    df = df.sample(frac=1)
            
            df = df.dropna()
            columns_to_write = [x for x in df.columns if x not in self.excluded_cols]
            log_message(f"Toplam kolon sayısı: {len(columns_to_write)}", "info")
            df = df[columns_to_write]
            df['value'] = self.turn_df_to_str(df)
            return df
            
        except Exception as e:
            log_message(f"Dosya okuma hatası: {str(e)}", "error")
            raise

    def df_to_kafka(self):
        """Verileri Kafka'ya gönder"""
        counter = 0
        df_size = len(self.df) * self.repeat
        total_time = self.row_sleep_time * df_size
        start_time = time.time()
        
        log_message("Veri akışı başlatılıyor...", "info")
        print(f"{Fore.CYAN}Toplam gönderilecek kayıt: {df_size:,}{Style.RESET_ALL}")
        
        for _ in range(self.repeat):
            with tqdm(total=len(self.df), desc="📤 Veri Gönderimi", 
                     bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]") as pbar:
                for index, row in self.df.iterrows():
                    try:
                        if self.key_index == 1000:
                            self.producer.send(self.topic, 
                                             key=str(index).encode(), 
                                             value=row[-1].encode())
                        else:
                            self.producer.send(self.topic,
                                             key=str(row[self.key_index]).encode(),
                                             value=row[-1].encode())
                        
                        self.producer.flush()
                        time.sleep(self.row_sleep_time)
                        
                        counter += 1
                        pbar.update(1)
                        
                    except Exception as e:
                        log_message(f"Veri gönderim hatası: {str(e)}", "error")
                        continue
                        
            if counter >= df_size:
                break
        
        duration = time.time() - start_time
        log_message("\n📊 İşlem Özeti", "success")
        print(f"{Fore.CYAN}├── ✅ Toplam gönderilen kayıt: {counter:,}")
        print(f"├── ⏱️ Toplam süre: {duration/60:.2f} dakika")
        print(f"└── 📈 Ortalama hız: {counter/duration:.2f} kayıt/saniye{Style.RESET_ALL}")
        
        self.producer.close()
        log_message("Producer kapatıldı.", "success")
if __name__ == "__main__":
    def str2bool(v):
        if isinstance(v, bool):
            return v
        if v.lower() in ('yes', 'true', 't', 'y', '1'):
            return True
        elif v.lower() in ('no', 'false', 'f', 'n', '0'):
            return False
        else:
            raise argparse.ArgumentTypeError('Boolean value expected.')

    print_banner()

    ap = argparse.ArgumentParser()
    ap.add_argument("-i", "--input", required=False, type=str, default="/tmp/sensors.csv",
                    help="Source data path. Default: /tmp/sensors.csv") 
    ap.add_argument("-s", "--sep", required=False, type=str, default=",",
                    help="Source data file delimiter. Default: ,")
    ap.add_argument("-e", "--source_file_extension", required=False, type=str, default="csv",
                    help="Extension of data file. Default: csv")
    ap.add_argument("-ks", "--kafka_sep", required=False, type=str, default=",",
                    help="Kafka value separator. Default: ,")
    ap.add_argument("-rst", "--row_sleep_time", required=False, type=float, default=0.5,
                    help="Sleep time in seconds per row. Default: 0.5")
    ap.add_argument("-t", "--topic", required=False, type=str, default="office-input",
                    help="Kafka topic. Default: office-input")
    ap.add_argument("-b", "--bootstrap_servers", required=False, nargs='+', default=["localhost:9092"],
                    help="Kafka bootstrap servers. Default: [localhost:9092]")
    ap.add_argument("-r", "--repeat", required=False, type=int, default=1,
                    help="How many times to repeat dataset. Default: 1")
    ap.add_argument("-shf", "--shuffle", required=False, type=str2bool, default=False,
                    help="Shuffle the rows?. Default: False")
    ap.add_argument("-k", "--key_index", required=False, type=int, default=1000,
                    help="Column index for Kafka key. Default: 1000 (uses pandas index)")
    ap.add_argument("-exc", "--excluded_cols", required=False, nargs='+', default=['it_is_impossible_column'],
                    help="Columns to exclude. Default: ['it_is_impossible_column']")

    args = vars(ap.parse_args())

    df_to_kafka = DataFrameToKafka(
        input=args['input'],
        sep=args['sep'],
        kafka_sep=args['kafka_sep'],
        row_sleep_time=args['row_sleep_time'],
        source_file_extension=args['source_file_extension'],
        topic=args['topic'],
        bootstrap_servers=args['bootstrap_servers'],
        repeat=args['repeat'],
        shuffle=args['shuffle'],
        key_index=args['key_index'],
        excluded_cols=args['excluded_cols']
    )
    
    df_to_kafka.df_to_kafka()