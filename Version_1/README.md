![version](https://img.shields.io/badge/version-1-blue.svg)

![System_architecture](https://github.com/user-attachments/assets/5389dfc7-fd3e-4d45-8e2c-d44e12de8db1)

## Project Overview
This project implements a real-time data processing pipeline for analyzing sensor data from office buildings. It processes various sensor readings including temperature, humidity, CO2, and motion data, visualizes them in real-time, and makes ML predictions for activity detection.

## System Architecture
The project consists of several interconnected components:
- Data Collection Layer: IoT sensors and data preprocessing
- Stream Processing Layer: Kafka and Spark streaming
- Storage Layer: Elasticsearch
- Visualization Layer: Kibana dashboards
- ML Layer: Real-time activity prediction

## Prerequisites
- Docker and Docker Compose
- Python 3.x
- Minimum 8GB RAM
- Required system settings: `vm.max_map_count=262144`

## Technologies Used
- Apache Kafka
- Apache Spark
- Elasticsearch
- Kibana
- Python (PySpark, pandas)
- Machine Learning (scikit-learn)

## Pipeline Components

### 1. Data Ingestion
- Downloads and preprocesses KETI sensor dataset
- Converts raw data into streamable format
- Sets up Kafka topics for data streaming

### 2. Data Processing
- Real-time stream processing with Apache Spark
- Data storage in Elasticsearch
- Real-time visualization in Kibana

### 3. Machine Learning
- Model training for motion detection
- Real-time predictions using trained model
- Separate Kafka topics for activity predictions

## Monitoring and Visualization

### Kibana Dashboard
Access dashboard at http://localhost:5601
- Real-time sensor data visualization
- Motion state distribution
- Environmental metrics (CO2, temperature, humidity)
- Room-specific analytics
- Interactive filtering and time range selection

### Data Monitoring
- Elasticsearch indexes: http://localhost:9200/_cat/indices
- Kafka topics monitoring through console consumers
- ML prediction monitoring through dedicated topics

## Kafka Topics

### office-input
- Receives CSV data from preprocessing
- Source for Elasticsearch stream and ML model

### office-activity
- Used for motion detection events
- Records when activity is detected in rooms

### office-no-activity
- Used for no-motion detection events
- Records when no activity is detected in rooms

### Process Steps with Screenshots:
### System Testing & Component Integration
1. **Connection Tests**
![test_connection_01](https://github.com/user-attachments/assets/cda35cef-c97d-4672-b375-0d75855a402e)
![test_connection_02](https://github.com/user-attachments/assets/dc39069c-a4a9-4a6e-ac85-b3ba68c03d82)

- Testing connectivity between Kafka, Spark, and Elasticsearch
- Verifying system readiness

### Data Pipeline Implementation
2. **Data Preprocessing Phase**
![preprocess_01](https://github.com/user-attachments/assets/66aa7e4a-3005-49e6-b7f6-d82c6bfeb068)
![preprocess_02](https://github.com/user-attachments/assets/9f945882-2d94-486e-b221-b2748053fe5b)
![preprocess_03](https://github.com/user-attachments/assets/6f410e4a-0489-48ff-9085-483a153f7993)
![preprocess_04](https://github.com/user-attachments/assets/a00a1f8c-5005-4410-8651-6d9e672d8f49)
- Raw sensor data extraction and cleaning
- Data standardization and formatting
- Quality checks and validation

3. **Kafka Topic Monitoring**

![topic_monitoring](https://github.com/user-attachments/assets/aa2f5f02-4681-4b50-a50f-672b29fab139)
- Real-time topic data visualization
- Sample data output:
```bash
2013-08-26 22:12:11,1377555131,413,575.0,102.0,24.06,48.09,0.0
2013-08-27 15:25:07,1377617107,413,449.0,42.0,23.28,47.28,0.0
2013-08-27 05:39:03,1377581943,413,474.0,3.0,24.59,43.98,0.0
```

4. **Real-time Data Streaming**
![produce_01](https://github.com/user-attachments/assets/d643806d-790f-4649-b6d0-c79be9aa449a)
![stream_elastic_01](https://github.com/user-attachments/assets/62346d1a-bef9-4c3c-b94a-b17c5acc4c9b)
![stream_elastic_02](https://github.com/user-attachments/assets/5c13c78b-10e1-459b-86c8-9c7214c57297)
- Kafka producer initialization
- Spark streaming pipeline setup
- Elasticsearch data ingestion
- Real-time data flow monitoring

5. **Machine Learning Pipeline**
![model_training_01](https://github.com/user-attachments/assets/6669c6e1-3bff-4191-a8a4-497ada16133c)
![model_training_02](https://github.com/user-attachments/assets/fb6b1a6b-a17c-4d4b-8965-eabf429dd57b)
- Model training initialization
- Feature engineering
- Training progress monitoring
- Performance evaluation

6. **Real-time Activity Detection**
![ml_streamin_01](https://github.com/user-attachments/assets/493d24d9-b273-49ba-82be-ade6a0b65721)
![ml_streamin_02](https://github.com/user-attachments/assets/3bc1ac6e-7cfa-44e7-a219-386915700a47)
![ml_streamin_03](https://github.com/user-attachments/assets/bf529873-f11f-4a33-88b2-552c91a8b489)
![ml_streamin_04](https://github.com/user-attachments/assets/09fecf1f-563f-4347-941c-4d88f8043466)
- Live prediction streaming
- Movement classification
- Real-time accuracy monitoring
- System performance metrics

7. **Data Visualization & Analytics**
![elastic_kibana_01](https://github.com/user-attachments/assets/1175c813-19ea-439a-969a-1f65f2981245)
![elastic_kibana_02](https://github.com/user-attachments/assets/9e4fd8f2-3dd2-40a9-a01e-01080f552354)
![elastic_kibana_03](https://github.com/user-attachments/assets/4e9c2ea1-0496-4873-8173-6246fe43ec90)
![elastic_kibana_04](https://github.com/user-attachments/assets/b06fd2ca-b02d-4d36-ab08-498df831adfd)
![elastic_kibana_05](https://github.com/user-attachments/assets/198dc190-7a14-405f-be0f-5b773c6c808d)
![elastic_kibana_06](https://github.com/user-attachments/assets/0ed9179b-bb32-4359-9daf-54579c04e440)
- Real-time dashboard implementation
- Interactive data exploration
- Sensor metrics visualization
- Movement pattern analysis
- Environmental monitoring

## Project Structure
```bash
project_root/
├── docker/
│   └── docker-compose.yml
├── python_files/
│   ├── preprocess_data.py
│   ├── dataframe_to_kafka_final.py
│   ├── spark_to_elasticsearch_wo_functions.py
│   ├── model_training.py
│   └── spark_ml_stream.py
├── data/
│   └── KETI/
├── scripts/
│   └── test_connections.py
└── README.md
```

