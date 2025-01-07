# Real-time-IoT-Sensor-Data-Analytics-and-Prediction-System 

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
![version](https://img.shields.io/badge/version-2-blue.svg)
