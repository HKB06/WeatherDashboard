"""
Validation et enrichissement des profils saisonniers
Compare les données temps réel avec les normales climatiques
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, month, when, lit, abs as spark_abs,
    round as spark_round, current_timestamp, to_json, struct, broadcast
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)

KAFKA_BOOTSTRAP = "kafka:9092"
INPUT_TOPIC = "weather_transformed"
OUTPUT_TOPIC = "weather_enriched"
HDFS_NORMALS = "hdfs://namenode:9000/weather/seasonal_profiles/monthly_normals"

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
    print("Seasonal Profile Enricher")
    print("=" * 60)
    print(f"📥 Source: {INPUT_TOPIC}")
    print(f"📤 Destination: {OUTPUT_TOPIC}")
    print("-" * 60)

    spark = SparkSession.builder \
        .appName("SeasonalEnricher") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    # Charger les normales depuis HDFS
    print("\n⏳ Chargement des normales climatiques...")
    try:
        normals_raw = spark.read.parquet(HDFS_NORMALS)
        # Renommer les colonnes pour éviter les conflits
        normals = normals_raw.select(
            col("city").alias("norm_city"),
            col("month").alias("norm_month"),
            col("temp_normale"),
            col("ecart_type")
        )
        print(f"✅ Normales chargées: {normals.count()} entrées")
    except Exception as e:
        print(f"⚠️ Normales non trouvées: {e}")
        print("   Lancez d'abord seasonal_profile.py")
        spark.stop()
        return

    # Lire le stream
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

    # Joindre avec les normales (broadcast pour petite table)
    df_with_normals = df_parsed.join(
        broadcast(normals),
        (col("city") == col("norm_city")) & (col("current_month") == col("norm_month")),
        "left"
    )

    # Calculer les écarts et enrichir
    df_enriched = df_with_normals \
        .withColumn("temp_ecart_normale", 
            spark_round(col("temperature") - col("temp_normale"), 1)) \
        .withColumn("temp_z_score",
            when(col("ecart_type").isNotNull() & (col("ecart_type") != 0),
                spark_round((col("temperature") - col("temp_normale")) / col("ecart_type"), 2)
            ).otherwise(lit(0.0))) \
        .withColumn("is_anomaly",
            when(spark_abs(col("temp_z_score")) > 2, lit(True)).otherwise(lit(False))) \
        .withColumn("anomaly_type",
            when(col("temp_z_score") > 2, lit("CHAUD"))
            .when(col("temp_z_score") < -2, lit("FROID"))
            .otherwise(lit("NORMAL"))) \
        .withColumn("enrichment_time", current_timestamp())

    # Sélectionner les colonnes finales
    df_output = df_enriched.select(
        "key", "event_time", "city", "country",
        "temperature", "temp_normale", "temp_ecart_normale", "temp_z_score",
        "is_anomaly", "anomaly_type",
        "windspeed", "precipitation",
        "wind_alert_level", "heat_alert_level",
        "enrichment_time"
    )

    # Afficher en console
    query_console = df_output.writeStream \
        .format("console") \
        .option("truncate", False) \
        .outputMode("append") \
        .start()

    # Écrire dans Kafka
    df_kafka = df_output.select(
        col("key"),
        to_json(struct("*")).alias("value")
    )
    
    query_kafka = df_kafka.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("topic", OUTPUT_TOPIC) \
        .option("checkpointLocation", "/tmp/checkpoint/seasonal_enricher") \
        .outputMode("append") \
        .start()

    print("\n✅ Enrichissement en cours...")
    print("   Ctrl+C pour arrêter")

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\n⏹️ Arrêt...")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
