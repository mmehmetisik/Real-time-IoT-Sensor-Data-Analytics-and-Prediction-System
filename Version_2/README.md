# Real-time-IoT-Sensor-Data-Analytics-and-Prediction-System
![version](https://img.shields.io/badge/version-2-blue.svg)

![System_architecture](https://github.com/user-attachments/assets/352d5318-f2cc-4090-91ee-5b3aebbebe59)

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
![test_connection_01](https://github.com/user-attachments/assets/febb33c9-27e6-4c72-900e-3418fe5f600c)
![test_connection_02](https://github.com/user-attachments/assets/12d42ec4-d297-4242-ba36-1a8d487ba85a)

- Testing connectivity between Kafka, Spark, and Elasticsearch
- Verifying system readiness

## 2. Data Preprocessing Phase
![strea_reader_01](https://github.com/user-attachments/assets/dc5ddb47-9a6b-4a80-8b66-ca91ad5d2f8d)
![strea_reader_02](https://github.com/user-attachments/assets/fd74af94-e435-479e-8b8b-893f3364fd0a)
![strea_reader_03](https://github.com/user-attachments/assets/654e28e6-dadb-4bde-a0e8-c5862245f793)

- Direct GitHub data download and preprocessing
- Combined streaming and CSV creation
- Data standardization and formatting
- Quality checks and validation

## 3. Kafka Topic Monitoring
![topic_monitoring](https://github.com/user-attachments/assets/2b3a2710-2b4b-47f9-b2f6-378c4a86bf91)

- Real-time topic data visualization
- Sample data output:
```bash
2013-08-26 22:12:11,1377555131,413,575.0,102.0,24.06,48.09,0.0
2013-08-27 15:25:07,1377617107,413,449.0,42.0,23.28,47.28,0.0
2013-08-27 05:39:03,1377581943,413,474.0,3.0,24.59,43.98,0.0
```
4. **Real-time Data Streaming**
![stream_to_elastic_01](https://github.com/user-attachments/assets/d459a5cd-1f38-4188-9829-3567cc77f235)
![stream_to_elastic_02](https://github.com/user-attachments/assets/0aab6bbd-0b23-4cf4-b7ae-22b0322bdcb9)
![strea_reader_03](https://github.com/user-attachments/assets/9655e438-9f1c-4357-9557-3318ff46fd91)

- Parallel stream processing
- Direct Elasticsearch integration
- Real-time data flow monitoring

5. **Machine Learning Pipeline**
![model_training_01](https://github.com/user-attachments/assets/fa872a39-8196-4c24-b475-3ebd8abf780a)
![model_training_02](https://github.com/user-attachments/assets/80f69a1b-14fb-4273-bc30-e7a2665ea03e)
![ml_streamin_01](https://github.com/user-attachments/assets/04f86925-ad7c-4ab7-ae86-87229c5af06a)
![ml_streamin_02](https://github.com/user-attachments/assets/bb56818c-cbef-4afc-9d5c-720d1589bd63)
![ml_streamin_03](https://github.com/user-attachments/assets/beca61e2-0d9a-4b8a-8da1-2a080885bf99)

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
![elastic_kibana_01](https://github.com/user-attachments/assets/7fa5f174-bd4c-46ec-868c-56fafe5a8b67)
![elastic_kibana_02](https://github.com/user-attachments/assets/fb80d6b0-1323-41cf-8198-74f8c4b5c411)
![elastic_kibana_03](https://github.com/user-attachments/assets/06ea3bb5-a7d0-4f9c-bcea-ddbf96a45c3c)
![elastic_kibana_04](https://github.com/user-attachments/assets/d1921e21-7560-4644-bf68-4fabdc4d2159)
![elastic_kibana_05](https://github.com/user-attachments/assets/07d8a6cf-1fc7-44a6-8c4d-f221eb44334e)
![elastic_kibana_06](https://github.com/user-attachments/assets/64e5a1cf-bb4b-4c2e-b38e-bcb897c1b704)

- Automated dashboard setup
- Pre-configured visualizations
- Real-time metrics tracking
- Environmental monitoring

Project Structure
```bash
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
```
