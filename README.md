# Real-time-IoT-Sensor-Data-Analytics-and-Prediction-System - Version 1


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
