
# 🗞️ **Modeling Gold Price Data on medallion architecture with Data Lake House approach**

This project builds a gold price prediction system based on the Medallion (Bronze-Silver-Gold) architecture in the Data Lakehouse environment. Macroeconomic data such as BI Rate, inflation, and rupiah exchange rate are batch processed using Apache Spark, stored in HDFS, and analyzed by multiple linear regression through Spark MLlib. Flow orchestration is performed with Airflow, while the final results are visualized in an interactive dashboard using Apache Superset. This system demonstrates a real-world implementation of a large-scale data pipeline for open-source predictive analytics.

## 📋Project Overview

This repository is an end-to-end implementation of a gold price prediction system based on modern Data Lakehouse architecture. The project demonstrates the practical application of data engineering principles, batch workflow, statistical modeling using multiple linear regression, and data visualization-all orchestrated in a modular and reproducible pipeline.

💡 Key System Components:

- Data Lake Design: Implement Medallion Architecture (Bronze → Silver → Gold) to organize raw data, cleaned data, and analysis-ready data.

- Machine Learning Pipeline: Build Multiple Linear Regression models using Spark MLlib to predict gold prices based on economic variables.

- Data Modeling & Storage: Store structured data in Hive Tables for easy querying and integration with business analytics tools.

- Visualization & Insights: The final results in the form of predictive models and visual analysis are presented in the form of posters as a medium for documentation and project exposure to lecture.

## ⚙️Tools

| No | Teknologi       | Kategori              | Fungsi Utama                                                                 |
|----|------------------|------------------------|-------------------------------------------------------------------------------|
| 1  | Hadoop HDFS          | Storage                | Store data and processing results in a distributed manner                    |
| 2  | Apache Spark         | Processing             | Process batch data for transformation and                                    |
| 3  | Apache Hive          | Query Engine           | Provide SQL interfaces for data access and analysis                          |
| 4  | Apache Spark MLlib   | Machine  Learning      | Regression modeling                                                                    |


## 🗃️  Project Directory Structure
```
gold-price-prediction-datalakehouse/
├── README.md
│
├── dataset/
│   ├── bronxe/
│   │    ├── gold_price.csv       
│   │    ├── kurs.csv     
│   │    ├── inflation.csv        
│   │    └── bi_rate.csv 
│   ├── silver/
│   ├── gold/
├── Scripts /
│   ├── data_ingestion/
│   │   └── ingest_data.py
│   ├── broze_to_silver/
│   │   └── etl_spark.py
│   └── silver-to_gold
│       └── spark_model.py
├── docker-compose.yml
├── Dockerfile.dananode
├── Dockerfile.namenode
└── models/
    └── gold_price_model.pkl


```
### 🧑‍🤝‍🧑**Team Member 10**
#### Muhammad Bayu Syuhada
#### Eksanty F Sugma Islamiaty
#### Eli Dwi Putra Berema
#### Syalaisha Andina Putriansyah
