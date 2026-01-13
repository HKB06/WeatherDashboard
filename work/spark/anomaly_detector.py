"""
Détection d'anomalies climatiques - Architecture Lambda (Batch + Speed)
Compare les données temps réel avec les profils historiques
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, month, when, lit, abs as spark_abs,
    round as spark_round, current_timestamp, window, count,
    avg, max as spark_max, to_json, struct, broadcast
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)

KAFKA_BOOTSTRAP = "kafka:9092"
INPUT_TOPIC = "weather_transformed"
OUTPUT_TOPIC = "weather_anomalies"
HDFS_NORMALS = "hdfs://namenode:9000/weather/seasonal_profiles/monthly_normals"
HDFS_THRESHOLDS = "hdfs://namenode:9000/weather/seasonal_profiles/thresholds"

# Schéma weather_transformed
SCHEMA = StructType([
    StructField("event_time", StringType(), True),
    StructField("processing_time", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("timezone", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("apparent_temperature", DoubleType(), True),
    StructField("humidity", IntegerType(), True),
    StructField("windspeed", DoubleType(), True),
    StructField("wind_direction", IntegerType(), True),
    StructField("wind_gusts", DoubleType(), True),
    StructField("precipitation", DoubleType(), True),
    StructField("weather_code", IntegerType(), True),
    StructField("cloud_cover", IntegerType(), True),
    StructField("pressure_msl", DoubleType(), True),
    StructField("wind_alert_level", StringType(), True),
    StructField("heat_alert_level", StringType(), True)
])

def main():
    print("=" * 60)
    print("Anomaly Detector - Lambda Architecture")
    print("=" * 60)
    print(f"📥 Speed Layer: {INPUT_TOPIC}")
    print(f"📊 Batch Layer: {HDFS_NORMALS}")
    print(f"📤 Output: {OUTPUT_TOPIC}")
    print("-" * 60)

    spark = SparkSession.builder \
        .appName("AnomalyDetector") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    # Batch Layer - Charger les profils
    print("\n📊 BATCH LAYER - Chargement des profils historiques...")
    try:
        normals = spark.read.parquet(HDFS_NORMALS).select(
            col("city").alias("n_city"),
            col("month").alias("n_month"),
            col("temp_normale"),
            col("ecart_type")
        )
        thresholds = spark.read.parquet(HDFS_THRESHOLDS).select(
            col("city").alias("t_city"),
            col("month").alias("t_month"),
            col("temp_max_P5"),
            col("temp_max_P95"),
            col("vent_P95"),
            col("precip_P95")
        )
        print(f"   ✅ Normales et seuils chargés")
    except Exception as e:
        print(f"   ⚠️ Erreur: {e}")
        print("   Lancez d'abord seasonal_profile.py")
        spark.stop()
        return

    # Speed Layer - Stream temps réel
    print("\n⚡ SPEED LAYER - Lecture du stream temps réel...")
    
    df_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", INPUT_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # Parser
    df_parsed = df_stream.select(
        col("key").cast("string").alias("key"),
        from_json(col("value").cast("string"), SCHEMA).alias("data")
    ).select(
        col("key"),
        col("data.*")
    ).withColumn("current_month", month(col("event_time")))

    # Joindre avec normales et seuils
    df_with_ref = df_parsed \
        .join(broadcast(normals),
              (col("city") == col("n_city")) & (col("current_month") == col("n_month")),
              "left") \
        .join(broadcast(thresholds),
              (col("city") == col("t_city")) & (col("current_month") == col("t_month")),
              "left")

    # Détecter les anomalies
    df_anomalies = df_with_ref \
        .withColumn("temp_ecart", 
            spark_round(col("temperature") - col("temp_normale"), 1)) \
        .withColumn("z_score",
            when(col("ecart_type").isNotNull() & (col("ecart_type") != 0),
                spark_round((col("temperature") - col("temp_normale")) / col("ecart_type"), 2)
            ).otherwise(lit(0.0))) \
        .withColumn("anomaly_temp",
            when(col("temperature") > col("temp_max_P95"), lit("EXTREME_CHAUD"))
            .when(col("temperature") < col("temp_max_P5"), lit("EXTREME_FROID"))
            .when(col("z_score") > 2, lit("ANORMALEMENT_CHAUD"))
            .when(col("z_score") < -2, lit("ANORMALEMENT_FROID"))
            .otherwise(lit("NORMAL"))) \
        .withColumn("anomaly_vent",
            when(col("windspeed") > col("vent_P95"), lit("VENT_EXTREME"))
            .otherwise(lit("NORMAL"))) \
        .withColumn("anomaly_precip",
            when(col("precipitation") > col("precip_P95"), lit("PRECIP_EXTREME"))
            .otherwise(lit("NORMAL"))) \
        .withColumn("has_anomaly",
            when(
                (col("anomaly_temp") != "NORMAL") |
                (col("anomaly_vent") != "NORMAL") |
                (col("anomaly_precip") != "NORMAL"),
                lit(True)
            ).otherwise(lit(False))) \
        .withColumn("detection_time", current_timestamp())

    # Colonnes de sortie
    df_output = df_anomalies.select(
        "key", "event_time", "city", "country",
        "temperature", "temp_normale", "temp_ecart", "z_score",
        "windspeed", "precipitation",
        "anomaly_temp", "anomaly_vent", "anomaly_precip", "has_anomaly",
        "detection_time"
    )

    # Console - toutes les données
    query_console = df_output.writeStream \
        .format("console") \
        .option("truncate", False) \
        .outputMode("append") \
        .start()

    # Kafka - seulement les anomalies
    df_anomalies_only = df_output.filter(col("has_anomaly") == True)
    df_kafka = df_anomalies_only.select(
        col("key"),
        to_json(struct("*")).alias("value")
    )
    
    query_kafka = df_kafka.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("topic", OUTPUT_TOPIC) \
        .option("checkpointLocation", "/tmp/checkpoint/anomaly_detector") \
        .outputMode("append") \
        .start()

    print("\n✅ Détection d'anomalies en cours...")
    print("   🔴 Anomalies envoyées dans:", OUTPUT_TOPIC)
    print("   Ctrl+C pour arrêter")

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\n⏹️ Arrêt...")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
