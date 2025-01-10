# Real-time-IoT-Sensor-Data-Analytics-and-Prediction-System
![version](https://img.shields.io/badge/version-2-blue.svg)

[SYSTEM ARCHITECTURE IMAGE PLACEHOLDER]

# Version Features
This is Version 2 of the project, featuring:
- Direct GitHub to Spark data ingestion
- Parallel data processing and streaming
- Combined preprocessing steps
- Automated Kibana dashboard setup
- Ready-to-run VM image
- Bilingual documentation (EN/TR)

# Project Overview
This project implements a real-time data processing pipeline for analyzing sensor data from office buildings. It processes various sensor readings including temperature, humidity, CO2, and motion data, visualizes them in real-time, and makes ML predictions for activity detection using a direct GitHub to Spark approach.

# System Architecture
The project consists of several interconnected components:
- Data Collection Layer: Direct GitHub data ingestion and preprocessing
- Stream Processing Layer: Parallel Kafka and Spark streaming
- Storage Layer: Elasticsearch
- Visualization Layer: Automated Kibana dashboards
- ML Layer: Real-time activity prediction

# Prerequisites
- Docker and Docker Compose
- Python 3.x
- Minimum 8GB RAM
- Required system settings: `vm.max_map_count=262144`
- RockyLinux VM environment

# Technologies Used
- Apache Kafka
- Apache Spark
- Elasticsearch
- Kibana
- Python (PySpark, pandas)
- Machine Learning (scikit-learn)

# Pipeline Components

## 1. Data Ingestion
- Direct download of KETI sensor dataset from GitHub
- Combined preprocessing and streaming
- Incremental CSV building with room-by-room append
- Sets up Kafka topics for streaming

## 2. Data Processing
- Parallel stream processing with Apache Spark
- Direct Elasticsearch integration
- Real-time visualization in Kibana

## 3. Machine Learning
- Model training using incrementally built CSV
- Real-time predictions using trained model
- Separate Kafka topics for activity predictions

# Monitoring and Visualization

## Kibana Dashboard
Access dashboard at http://localhost:5601
- Pre-configured visualization templates
- Auto-created graphs using export.ndjson
- Real-time sensor data visualization
- Motion state distribution
- Environmental metrics (CO2, temperature, humidity)
- Room-specific analytics
- Interactive filtering and time range selection

## Data Monitoring
- Elasticsearch indexes: http://localhost:9200/_cat/indices
- Kafka topics monitoring through console consumers
- ML prediction monitoring through dedicated topics

# Kafka Topics

## office-input
- Receives preprocessed sensor data directly
- Source for both Elasticsearch and ML model

## office-activity
- Used for motion detection events
- Records when activity is detected in rooms

## office-no-activity
- Used for no-motion detection events
- Records when no activity is detected in rooms

# Process Steps with Screenshots:

## 1. Connection Tests
[PLACE CONNECTION TEST SCREENSHOTS HERE]
- Testing connectivity between Kafka, Spark, and Elasticsearch
- Verifying system readiness

## 2. Data Preprocessing Phase
[PLACE PREPROCESSING SCREENSHOTS HERE]
- Direct GitHub data download and preprocessing
- Combined streaming and CSV creation
- Data standardization and formatting
- Quality checks and validation

## 3. Kafka Topic Monitoring
[PLACE KAFKA MONITORING SCREENSHOT HERE]
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
- Parallel stream processing
- Direct Elasticsearch integration
- Real-time data flow monitoring

5. **Machine Learning Pipeline**
![model_training_01](https://github.com/user-attachments/assets/6669c6e1-3bff-4191-a8a4-497ada16133c)
![model_training_02](https://github.com/user-attachments/assets/fb6b1a6b-a17c-4d4b-8965-eabf429dd57b)
- Streamlined model training
- Direct CSV data utilization
- Performance monitoring
- Accuracy evaluation

6. **Real-time Activity Detection**
![ml_streamin_01](https://github.com/user-attachments/assets/493d24d9-b273-49ba-82be-ade6a0b65721)
![ml_streamin_02](https://github.com/user-attachments/assets/3bc1ac6e-7cfa-44e7-a219-386915700a47)
![ml_streamin_03](https://github.com/user-attachments/assets/bf529873-f11f-4a33-88b2-552c91a8b489)
![ml_streamin_04](https://github.com/user-attachments/assets/09fecf1f-563f-4347-941c-4d88f8043466)
- Live prediction streaming
- Dual-topic classification
- Real-time accuracy tracking
- System performance metrics

7. **Data Visualization & Analytics**
![elastic_kibana_01](https://github.com/user-attachments/assets/1175c813-19ea-439a-969a-1f65f2981245)
![elastic_kibana_02](https://github.com/user-attachments/assets/9e4fd8f2-3dd2-40a9-a01e-01080f552354)
![elastic_kibana_03](https://github.com/user-attachments/assets/4e9c2ea1-0496-4873-8173-6246fe43ec90)
![elastic_kibana_04](https://github.com/user-attachments/assets/b06fd2ca-b02d-4d36-ab08-498df831adfd)
![elastic_kibana_05](https://github.com/user-attachments/assets/198dc190-7a14-405f-be0f-5b773c6c808d)
![elastic_kibana_06](https://github.com/user-attachments/assets/0ed9179b-bb32-4359-9daf-54579c04e440)
- Automated dashboard setup
- Pre-configured visualizations
- Real-time metrics tracking
- Environmental monitoring

project_root/
├── docker/
│   └── docker-compose.yml
├── python_files/
│   ├── stream_reader.py              # Combined preprocessing and streaming
│   ├── spark_to_elasticsearch_wo_functions.py
│   ├── model_training.py
│   └── spark_ml_stream.py
├── data/
│   └── KETI/
├── visualization/
│   └── export.ndjson                 # Kibana dashboard template
├── vm/
│   └── version_01.ova               # Ready-to-run VM image
└── README.md

