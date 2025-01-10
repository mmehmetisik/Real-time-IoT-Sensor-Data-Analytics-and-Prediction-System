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
