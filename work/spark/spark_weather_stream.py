"""
Transformation Spark Streaming + Détection d'alertes
Lit weather_stream → Calcule alertes vent/chaleur → Écrit dans weather_transformed
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, when, current_timestamp, to_json, struct
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)

KAFKA_BOOTSTRAP = "kafka:9092"
INPUT_TOPIC = "weather_stream"
OUTPUT_TOPIC = "weather_transformed"

# Schéma des messages entrants
WEATHER_SCHEMA = StructType([
    StructField("event_time", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("timezone", StringType(), True),
    StructField("elevation", DoubleType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("apparent_temperature", DoubleType(), True),
    StructField("humidity", IntegerType(), True),
    StructField("precipitation", DoubleType(), True),
    StructField("weather_code", IntegerType(), True),
    StructField("cloud_cover", IntegerType(), True),
    StructField("pressure_msl", DoubleType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("wind_direction", IntegerType(), True),
    StructField("wind_gusts", DoubleType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True)
])

def get_wind_alert(wind_col):
    """
    Niveaux d'alerte vent:
    - level_0: ≤ 10 m/s (faible)
    - level_1: 10-20 m/s (modéré)
    - level_2: > 20 m/s (fort)
    """
    return when(wind_col <= 10, "level_0") \
           .when((wind_col > 10) & (wind_col <= 20), "level_1") \
           .otherwise("level_2")

def get_heat_alert(temp_col):
    """
    Niveaux d'alerte chaleur:
    - level_0: ≤ 25°C (normal)
    - level_1: 25-35°C (modéré)
    - level_2: > 35°C (canicule)
    """
    return when(temp_col <= 25, "level_0") \
           .when((temp_col > 25) & (temp_col <= 35), "level_1") \
           .otherwise("level_2")

def main():
    print("=" * 60)
    print("Spark Weather Transformer")
    print("=" * 60)
    print(f"📥 Source: {INPUT_TOPIC}")
    print(f"📤 Destination: {OUTPUT_TOPIC}")
    print("-" * 60)

    spark = SparkSession.builder \
        .appName("WeatherStreamTransformer") \
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
        col("key").cast("string").alias("key"),
        from_json(col("value").cast("string"), WEATHER_SCHEMA).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    )

    # Extraire les champs et ajouter les alertes
    df_transformed = df_parsed.select(
        col("key"),
        col("data.event_time").alias("event_time"),
        col("data.latitude").alias("latitude"),
        col("data.longitude").alias("longitude"),
        col("data.city").alias("city"),
        col("data.country").alias("country"),
        col("data.timezone").alias("timezone"),
        col("data.temperature").alias("temperature"),
        col("data.apparent_temperature").alias("apparent_temperature"),
        col("data.humidity").alias("humidity"),
        col("data.wind_speed").alias("windspeed"),
        col("data.wind_direction").alias("wind_direction"),
        col("data.wind_gusts").alias("wind_gusts"),
        col("data.precipitation").alias("precipitation"),
        col("data.weather_code").alias("weather_code"),
        col("data.cloud_cover").alias("cloud_cover"),
        col("data.pressure_msl").alias("pressure_msl")
    ).withColumn(
        "wind_alert_level", get_wind_alert(col("windspeed"))
    ).withColumn(
        "heat_alert_level", get_heat_alert(col("temperature"))
    ).withColumn(
        "processing_time", current_timestamp()
    )

    # Préparer le message de sortie
    df_output = df_transformed.select(
        col("key"),
        to_json(struct(
            "event_time", "processing_time",
            "latitude", "longitude", "city", "country", "timezone",
            "temperature", "apparent_temperature", "humidity",
            "windspeed", "wind_direction", "wind_gusts",
            "precipitation", "weather_code", "cloud_cover", "pressure_msl",
            "wind_alert_level", "heat_alert_level"
        )).alias("value")
    )

    # Écrire dans Kafka
    query_kafka = df_output.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("topic", OUTPUT_TOPIC) \
        .option("checkpointLocation", "/tmp/checkpoint/weather_transformer") \
        .outputMode("append") \
        .start()

    # Afficher dans la console pour debug
    query_console = df_transformed.writeStream \
        .format("console") \
        .option("truncate", False) \
        .outputMode("append") \
        .start()

    print("✅ Streaming en cours...")
    print("   Ctrl+C pour arrêter")
    
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\n⏹️ Arrêt...")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
