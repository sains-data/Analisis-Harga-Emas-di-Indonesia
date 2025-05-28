from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def create_spark_session():
    spark = SparkSession.builder \
        .appName("Bronze to Silver Processing") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
        .getOrCreate()
    return spark

def clean_and_standardize_date(df, date_column, new_date_column="tanggal_std"):
    """Standardisasi format tanggal"""
    return df.withColumn(
        new_date_column, 
        to_date(col(date_column), "yyyy-MM-dd")
    ).withColumn(
        "tahun", year(col(new_date_column))
    ).withColumn(
        "bulan", month(col(new_date_column))
    )

def process_harga_emas(spark):
    """Proses data harga emas"""
    df = spark.read.option("header", "true") \
        .csv("hdfs://namenode:8020/bronze/harga_emas/")
    
    # Clean data
    df_clean = df.dropna() \
        .dropDuplicates() \
        .withColumn("harga_emas", col("harga_emas").cast("double"))
    
    # Standardize date
    df_std = clean_and_standardize_date(df_clean, "tanggal")
    
    return df_std.select("tahun", "bulan", "harga_emas")

def process_kurs(spark):
    """Proses data kurs"""
    df = spark.read.option("header", "true") \
        .csv("hdfs://namenode:8020/bronze/kurs/")
    
    df_clean = df.dropna() \
        .dropDuplicates() \
        .withColumn("kurs_jual", col("kurs_jual").cast("double"))
    
    df_std = clean_and_standardize_date(df_clean, "tanggal")
    
    return df_std.select("tahun", "bulan", "kurs_jual")

def process_bi_rate(spark):
    """Proses data BI Rate"""
    df = spark.read.option("header", "true") \
        .csv("hdfs://namenode:8020/bronze/bi_rate/")
    
    df_clean = df.dropna() \
        .dropDuplicates() \
        .withColumn("bi_rate", col("percentage").cast("double"))
    
    df_std = clean_and_standardize_date(df_clean, "tanggal")
    
    return df_std.select("tahun", "bulan", "bi_rate")

def process_inflasi(spark):
    """Proses data inflasi"""
    df = spark.read.option("header", "true") \
        .csv("hdfs://namenode:8020/bronze/inflasi/")
    
    df_clean = df.dropna() \
        .dropDuplicates() \
        .withColumn("inflasi", col("data_inflasi").cast("double"))
    
    df_std = clean_and_standardize_date(df_clean, "tanggal")
    
    return df_std.select("tahun", "bulan", "inflasi")

def integrate_all_data(spark):
    """Integrasi semua dataset berdasarkan tahun dan bulan"""
    
    # Process each dataset
    harga_emas_df = process_harga_emas(spark)
    kurs_df = process_kurs(spark)
    bi_rate_df = process_bi_rate(spark)
    inflasi_df = process_inflasi(spark)
    
    # Join semua dataset
    integrated_df = harga_emas_df \
        .join(kurs_df, ["tahun", "bulan"], "inner") \
        .join(bi_rate_df, ["tahun", "bulan"], "inner") \
        .join(inflasi_df, ["tahun", "bulan"], "inner")
    
    # Agregasi jika ada multiple records per bulan
    final_df = integrated_df.groupBy("tahun", "bulan") \
        .agg(
            avg("harga_emas").alias("harga_emas"),
            avg("kurs_jual").alias("kurs_jual"), 
            avg("bi_rate").alias("bi_rate"),
            avg("inflasi").alias("inflasi")
        )
    
    return final_df

def main():
    spark = create_spark_session()
    
    try:
        # Integrate all data
        integrated_df = integrate_all_data(spark)
        
        print("🔍 Sample data terintegrasi:")
        integrated_df.show(10)
        print(f"📊 Total records: {integrated_df.count()}")
        
        # Save to Silver layer
        integrated_df.write \
            .mode("overwrite") \
            .parquet("hdfs://namenode:8020/silver/integrated_data/")
        
        print("✅ Data berhasil disimpan ke Silver layer")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()