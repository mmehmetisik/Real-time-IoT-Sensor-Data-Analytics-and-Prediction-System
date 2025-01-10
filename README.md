# Real-time-IoT-Sensor-Data-Analytics-and-Prediction-System 

![System_architecture](https://github.com/user-attachments/assets/5389dfc7-fd3e-4d45-8e2c-d44e12de8db1)

## Project Overview
This project implements a real-time data processing pipeline for analyzing sensor data from office buildings. It processes various sensor readings including temperature, humidity, CO2, and motion data, visualizes them in real-time, and makes ML predictions for activity detection.

## Version 1  
The first version provides a standard data processing pipeline with the following components:  
- **Apache Kafka**: Streaming sensor data in real-time.  
- **Apache Spark**: Real-time data processing.  
- **Elasticsearch**: Storing processed data.  
- **Kibana**: Visualizing real-time data.  
- **Machine Learning**: Real-time activity prediction.

## Version 2  
The second version introduces enhanced features and a more efficient pipeline:  
- **Direct data ingestion from GitHub** with **parallel processing**.  
- **Combined preprocessing and streaming steps** for faster data handling.  
- **Automated Kibana dashboard setup** for instant visualization.  
- **Ready-to-run VM image** for quick deployment.  
- **Bilingual documentation (English/Turkish)** for broader accessibility.

Both versions utilize the following technologies:  
- **Docker**  
- **Apache Kafka**  
- **Apache Spark**  
- **Elasticsearch**  
- **Kibana**  
- **Python (PySpark, pandas)**  
- **Machine Learning (scikit-learn)**

This project enables real-time monitoring of sensor data, environmental analytics, and motion detection.
