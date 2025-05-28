from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline

def create_spark_session():
    spark = SparkSession.builder \
        .appName("Gold Layer - Feature Engineering & Modeling") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
        .getOrCreate()
    return spark

def feature_engineering(df):
    """Feature engineering untuk data harga emas"""
    
    # Lag features (data bulan sebelumnya)
    window_spec = Window.partitionBy().orderBy("tahun", "bulan")
    
    df_features = df.withColumn(
        "kurs_jual_lag1", lag("kurs_jual", 1).over(window_spec)
    ).withColumn(
        "bi_rate_lag1", lag("bi_rate", 1).over(window_spec)
    ).withColumn(
        "inflasi_lag1", lag("inflasi", 1).over(window_spec)
    ).withColumn(
        "harga_emas_lag1", lag("harga_emas", 1).over(window_spec)
    )
    
    # Moving averages (3 bulan)
    df_features = df_features.withColumn(
        "kurs_ma3", avg("kurs_jual").over(
            Window.partitionBy().orderBy("tahun", "bulan")
            .rowsBetween(-2, 0)
        )
    ).withColumn(
        "bi_rate_ma3", avg("bi_rate").over(
            Window.partitionBy().orderBy("tahun", "bulan")
            .rowsBetween(-2, 0)
        )
    )
    
    # Rate of change
    df_features = df_features.withColumn(
        "kurs_change", 
        (col("kurs_jual") - col("kurs_jual_lag1")) / col("kurs_jual_lag1") * 100
    ).withColumn(
        "bi_rate_change",
        col("bi_rate") - col("bi_rate_lag1")
    )
    
    return df_features.dropna()

def build_ml_model(spark):
    """Build dan train model machine learning"""
    
    # Load data dari Silver layer
    df = spark.read.parquet("hdfs://namenode:8020/silver/integrated_data/")
    
    # Feature engineering
    df_features = feature_engineering(df)
    
    print("🔍 Data dengan features:")
    df_features.show(5)
    
    # Prepare features untuk modeling
    feature_cols = [
        "kurs_jual", "bi_rate", "inflasi", 
        "kurs_jual_lag1", "bi_rate_lag1", "inflasi_lag1",
        "kurs_ma3", "bi_rate_ma3", 
        "kurs_change", "bi_rate_change"
    ]
    
    # Vector assembler
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features"
    )
    
    # Linear regression
    lr = LinearRegression(
        featuresCol="features",
        labelCol="harga_emas",
        predictionCol="predicted_harga_emas"
    )
    
    # Pipeline
    pipeline = Pipeline(stages=[assembler, lr])
    
    # Split data
    train_df, test_df = df_features.randomSplit([0.8, 0.2], seed=42)
    
    print(f"📊 Training data: {train_df.count()} records")
    print(f"📊 Testing data: {test_df.count()} records")
    
    # Train model
    model = pipeline.fit(train_df)
    
    # Predictions
    predictions = model.transform(test_df)
    
    # Evaluation
    evaluator = RegressionEvaluator(
        labelCol="harga_emas",
        predictionCol="predicted_harga_emas",
        metricName="rmse"
    )
    
    rmse = evaluator.evaluate(predictions)
    
    evaluator_r2 = RegressionEvaluator(
        labelCol="harga_emas", 
        predictionCol="predicted_harga_emas",
        metricName="r2"  
    )
    
    r2 = evaluator_r2.evaluate(predictions)
    
    print(f"📈 Model Performance:")
    print(f"   RMSE: {rmse:.2f}")
    print(f"   R²: {r2:.4f}")
    
    # Show predictions
    print("🔍 Sample predictions:")
    predictions.select(
        "tahun", "bulan", "harga_emas", 
        "predicted_harga_emas", 
        (col("harga_emas") - col("predicted_harga_emas")).alias("error")
    ).show(10)
    
    # Save results to Gold layer
    predictions.write \
        .mode("overwrite") \
        .parquet("hdfs://namenode:8020/gold/predictions/")
    
    # Save model
    model.write().overwrite().save("hdfs://namenode:8020/gold/model/")
    
    return model, predictions

def main():
    spark = create_spark_session()
    
    try:
        model, predictions = build_ml_model(spark)
        print("✅ Model berhasil dilatih dan disimpan ke Gold layer")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()