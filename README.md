
# 🗞️ **Modeling Gold Price Data on medallion architecture with Data Lake House approach**

This project builds a gold price prediction system based on the Medallion (Bronze-Silver-Gold) architecture in the Data Lakehouse environment. Macroeconomic data such as BI Rate, inflation, and rupiah exchange rate are batch processed using Apache Spark, stored in HDFS, and analyzed by multiple linear regression through Spark MLlib. Flow orchestration is performed with Airflow, while the final results are visualized in an interactive dashboard using Apache Superset. This system demonstrates a real-world implementation of a large-scale data pipeline for open-source predictive analytics.

## 📋Project Overview

This repository is an end-to-end implementation of a gold price prediction system based on modern Data Lakehouse architecture. The project demonstrates the practical application of data engineering principles, batch workflow, statistical modeling using multiple linear regression, and data visualization-all orchestrated in a modular and reproducible pipeline.

💡 Key System Components:

- Data Lake Design: Implement Medallion Architecture (Bronze → Silver → Gold) to organize raw data, cleaned data, and analysis-ready data.

- Data Engineering Workflow: Import and process macroeconomic data (BI Rate, inflation, exchange rate) using Apache Spark, convert source data (CSV) into efficient analytic formats such as Parquet.

- Machine Learning Pipeline: Build Multiple Linear Regression models using Spark MLlib to predict gold prices based on economic variables.

- Data Modeling & Storage: Store structured data in Hive Tables for easy querying and integration with business analytics tools.

- Visualization & Insights: The final results in the form of predictive models and visual analysis are presented in the form of posters as a medium for documentation and project exposure to lecture.

## ⚙️Tools

| No | Teknologi       | Kategori              | Fungsi Utama                                                                 |
|----|------------------|------------------------|-------------------------------------------------------------------------------|
| 1  | Hadoop HDFS      | Storage                | Store data and processing results in a distributed manner                    |
| 2  | Apache Spark     | Processing             | Process batch data for transformation and regression modeling                |
| 3  | YARN             | Resource Management    | Manage cluster resources and run applications in parallel                    |
| 4  | Apache Hive      | Query Engine           | Provide SQL interfaces for data access and analysis                          |
| 5  | Apache Oozie     | Workflow Scheduling    | Schedule batch pipelines for periodic data processing                        |
| 6  | Apache Kafka     | Data Ingestion         | Stream and batch collect data from multiple sources                          |
| 7  | Ambari           | Monitoring & Management| Monitor and manage Hadoop clusters                                           |

## Pipeline

## 🗃️  Project Directory Structure
```
gold-price-prediction-datalakehouse/
├── README.md
│
├── datasets/
│   ├── gold_price.csv       
│   ├── kurs.csv     
│   ├── inflation.csv        
│   └── bi_rate.csv           
│
├── src /
│   ├── ingest_data.py 
│   └── etl_spark.py
│   └── spark_model.py         
│   └── airflow_dag.py
└── models/
    └── gold_price_model.pkl


```
### 🧑‍🤝‍🧑**Team Member 10**
#### Muhammad Bayu Syuhada
#### Eksanty F Sugma Islamiaty
#### Eli Dwi Putra Berema
#### Syalaisha Andina Putriansyah
