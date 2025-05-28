from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.sql.functions import *

def create_spark_session():
    spark = SparkSession.builder \
        .appName("Model Evaluation") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
        .getOrCreate()
    return spark

def evaluate_predictions(spark):
    """Evaluasi hasil prediksi"""
    
    # Load predictions
    predictions_df = spark.read.parquet("hdfs://namenode:8020/gold/predictions/")
    
    # Analisis error
    error_analysis = predictions_df.select(
        mean(abs(col("harga_emas") - col("predicted_harga_emas"))).alias("MAE"),
        sqrt(mean(pow(col("harga_emas") - col("predicted_harga_emas"), 2))).alias("RMSE"),
        mean(col("harga_emas")).alias("Mean_Actual"),
        stddev(col("harga_emas")).alias("Std_Actual")
    )
    
    print("📊 Model Error Analysis:")
    error_analysis.show()
    
    # Feature importance (jika menggunakan tree-based model)
    print("🔍 Sample predictions with details:")
    predictions_df.select(
        "tahun", "bulan", 
        "harga_emas", "predicted_harga_emas",
        "kurs_jual", "bi_rate", "inflasi"
    ).orderBy("tahun", "bulan").show(20)

def main():
    spark = create_spark_session()
    
    try:
        evaluate_predictions(spark)
    except Exception as e:
        print(f"❌ Error: {str(e)}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()