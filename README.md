# NYC-BUS-Real-Time-Data-Pipeline
Proyect for data in real time 

# Overview 
End-to-end data enginnering project that ingests real-time data from the NYC MTA
#Bus API, processes it using Airflow, stores it in PostgreSQL, and visualizes it
#with Metabase

## Tech Stack 
-Python
-Apache Airflow 
-PostgreSQL 
-Docker and Docker Compose
-Metabase

## Architecture 
API -> Extract -> Transform -> Load -> PostgreSQL -> Metabase

## Features

-Hurly scheduler ETL pipeline 
- windowed aggregations (1-minute)
- Spatial density analysis
- Dockerized enviorenment

## How to Run 

1. Set enviorenment variables
2. Run 'docker compose up'
3. Access Airflow UI at 'localhost:8181'
4. Access Metabase at 'localhost:3000'

## Author

Víctor Eduardo 
