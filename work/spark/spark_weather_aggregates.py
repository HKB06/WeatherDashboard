"""
Agrégats temps réel avec Spark Structured Streaming
Sliding windows de 5 minutes avec glissement de 1 minute
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, count, avg, min as spark_min, max as spark_max,
    sum as spark_sum, when, to_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)

KAFKA_BOOTSTRAP = "kafka:9092"
INPUT_TOPIC = "weather_transformed"

# Schéma des messages weather_transformed
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
    print("Weather Aggregates - Sliding Windows")
    print("=" * 60)
    print(f"📥 Source: {INPUT_TOPIC}")
    print("📊 Fenêtre: 5 min / glissement 1 min")
    print("-" * 60)

    spark = SparkSession.builder \
        .appName("WeatherAggregates") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    # Lire le stream Kafka
    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", INPUT_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    # Parser le JSON
    df_parsed = df_raw.select(
        from_json(col("value").cast("string"), SCHEMA).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    ).select(
        to_timestamp(col("data.event_time")).alias("event_time"),
        col("data.city").alias("city"),
        col("data.country").alias("country"),
        col("data.temperature").alias("temperature"),
        col("data.windspeed").alias("windspeed"),
        col("data.wind_alert_level").alias("wind_alert_level"),
        col("data.heat_alert_level").alias("heat_alert_level")
    ).filter(col("event_time").isNotNull())

    # Watermark pour gérer les données tardives
    df_with_watermark = df_parsed.withWatermark("event_time", "2 minutes")

    # Agrégat 1: Stats température par pays
    temp_stats = df_with_watermark \
        .groupBy(
            window(col("event_time"), "5 minutes", "1 minute"),
            col("country")
        ) \
        .agg(
            avg("temperature").alias("avg_temp"),
            spark_min("temperature").alias("min_temp"),
            spark_max("temperature").alias("max_temp"),
            count("*").alias("nb_mesures")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("country"),
            col("avg_temp"),
            col("min_temp"),
            col("max_temp"),
            col("nb_mesures")
        )

    # Agrégat 2: Comptage alertes par type
    df_with_alerts = df_with_watermark \
        .withColumn("is_wind_alert", 
            when(col("wind_alert_level").isin(["level_1", "level_2"]), 1).otherwise(0)) \
        .withColumn("is_heat_alert",
            when(col("heat_alert_level").isin(["level_1", "level_2"]), 1).otherwise(0)) \
        .withColumn("is_wind_level2",
            when(col("wind_alert_level") == "level_2", 1).otherwise(0)) \
        .withColumn("is_heat_level2",
            when(col("heat_alert_level") == "level_2", 1).otherwise(0))

    alert_counts = df_with_alerts \
        .groupBy(
            window(col("event_time"), "5 minutes", "1 minute"),
            col("country")
        ) \
        .agg(
            spark_sum("is_wind_alert").alias("wind_alerts_total"),
            spark_sum("is_heat_alert").alias("heat_alerts_total"),
            spark_sum("is_wind_level2").alias("wind_alerts_level2"),
            spark_sum("is_heat_level2").alias("heat_alerts_level2"),
            count("*").alias("total_mesures")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("country"),
            col("wind_alerts_total"),
            col("heat_alerts_total"),
            col("wind_alerts_level2"),
            col("heat_alerts_level2"),
            col("total_mesures")
        )

    # Démarrer les queries
    print("\n📊 Stats Température par Pays:")
    query_temp = temp_stats.writeStream \
        .format("console") \
        .option("truncate", False) \
        .outputMode("update") \
        .queryName("temp_stats") \
        .start()

    print("\n🚨 Comptage Alertes par Pays:")
    query_alerts = alert_counts.writeStream \
        .format("console") \
        .option("truncate", False) \
        .outputMode("update") \
        .queryName("alert_counts") \
        .start()

    print("\n✅ Agrégateur en cours...")
    print("   Ctrl+C pour arrêter")

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\n⏹️ Arrêt...")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
