from pyspark.sql import SparkSession
import os

def create_spark_session():
    """Membuat Spark Session"""
    spark = SparkSession.builder \
        .appName("Gold Price Data Ingestion") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
        .getOrCreate()
    return spark

def ingest_csv_to_hdfs(spark, local_path, hdfs_path):
    """Ingest CSV dari local ke HDFS"""
    try:
        df = spark.read.option("header", "true").csv(local_path)
        df.write.mode("overwrite").csv(hdfs_path)
        print(f"✅ Berhasil ingest {local_path} ke {hdfs_path}")
        df.show(5)
    except Exception as e:
        print(f"❌ Error ingesting {local_path}: {str(e)}")

def main():
    spark = create_spark_session()
    
    # Mapping dataset ke HDFS paths
    datasets = {
        "/opt/dataset/bronze/harga_emas.csv": "hdfs://namenode:8020/bronze/harga_emas/",
        "/opt/dataset/bronze/kurs.csv": "hdfs://namenode:8020/bronze/kurs/", 
        "/opt/dataset/bronze/bi_rate.csv": "hdfs://namenode:8020/bronze/bi_rate/",
        "/opt/dataset/bronze/inflasi.csv": "hdfs://namenode:8020/bronze/inflasi/"
    }
    
    for local_path, hdfs_path in datasets.items():
        ingest_csv_to_hdfs(spark, local_path, hdfs_path)
    
    spark.stop()

if __name__ == "__main__":
    main()