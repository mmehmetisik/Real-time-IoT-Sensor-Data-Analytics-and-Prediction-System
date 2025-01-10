# Real-time-IoT-Sensor-Data-Analytics-and-Prediction-System
![version](https://img.shields.io/badge/version-2-blue.svg)

![System_architecture](https://github.com/user-attachments/assets/5389dfc7-fd3e-4d45-8e2c-d44e12de8db1)

## Project Overview
This project implements a real-time data processing pipeline for analyzing sensor data from office buildings. It processes various sensor readings including temperature, humidity, CO2, and motion data, visualizes them in real-time, and makes ML predictions for activity detection using a direct GitHub to Spark approach.

## System Architecture
The project consists of several interconnected components:
- Data Collection Layer: Direct GitHub data ingestion and preprocessing
- Stream Processing Layer: Parallel Kafka and Spark streaming
- Storage Layer: Elasticsearch
- Visualization Layer: Automated Kibana dashboards
- ML Layer: Real-time activity prediction

## Prerequisites
- Docker and Docker Compose
- Python 3.x
- Minimum 8GB RAM
- Required system settings: `vm.max_map_count=262144`
- RockyLinux VM environment

## Technologies Used
- Apache Kafka
- Apache Spark
- Elasticsearch
- Kibana
- Python (PySpark, pandas)
- Machine Learning (scikit-learn)

## Pipeline Components

### 1. Data Ingestion
- Direct download of KETI sensor dataset from GitHub
- Combined preprocessing and streaming
- Incremental CSV building with room-by-room append
- Sets up Kafka topics for streaming

### 2. Data Processing
- Parallel stream processing with Apache Spark
- Direct Elasticsearch integration
- Real-time visualization in Kibana

### 3. Machine Learning
- Model training using incrementally built CSV
- Real-time predictions using trained model
- Separate Kafka topics for activity predictions

## Monitoring and Visualization

### Kibana Dashboard
Access dashboard at http://localhost:5601
- Pre-configured visualization templates
- Auto-created graphs using export.ndjson
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
- Receives preprocessed sensor data directly
- Source for both Elasticsearch and ML model

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
- Direct GitHub data download and preprocessing
- Combined streaming and CSV creation
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
