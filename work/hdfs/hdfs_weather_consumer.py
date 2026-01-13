"""
Consumer Kafka vers HDFS
Lit weather_transformed et stocke dans HDFS organisé par pays/ville
Structure: /weather/country={pays}/city={ville}/YYYY-MM-DD/data.json
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_date, year, month, dayofmonth, coalesce, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)

KAFKA_BOOTSTRAP = "kafka:9092"
INPUT_TOPIC = "weather_transformed"
HDFS_PATH = "hdfs://namenode:9000/weather/realtime"
CHECKPOINT_PATH = "/tmp/checkpoint/hdfs_weather"

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
    print("HDFS Weather Consumer")
    print("=" * 60)
    print(f"📥 Source: {INPUT_TOPIC}")
    print(f"📁 Destination: {HDFS_PATH}")
    print("-" * 60)

    spark = SparkSession.builder \
        .appName("HDFSWeatherConsumer") \
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

    # Parser et préparer les données
    df_parsed = df_raw.select(
        from_json(col("value").cast("string"), SCHEMA).alias("data")
    ).select("data.*")

    # Ajouter colonnes de partitionnement
    df_partitioned = df_parsed \
        .withColumn("country", coalesce(col("country"), lit("Unknown"))) \
        .withColumn("city", coalesce(col("city"), lit("Unknown"))) \
        .withColumn("date", to_date(col("event_time"))) \
        .withColumn("year", year(col("date"))) \
        .withColumn("month", month(col("date"))) \
        .withColumn("day", dayofmonth(col("date")))

    # Écrire dans HDFS avec partitionnement
    query = df_partitioned.writeStream \
        .format("parquet") \
        .option("path", HDFS_PATH) \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .partitionBy("country", "city", "year", "month", "day") \
        .outputMode("append") \
        .start()

    print("✅ Consumer HDFS en cours...")
    print("   Partitionnement: country/city/year/month/day")
    print("   Ctrl+C pour arrêter")

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n⏹️ Arrêt...")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
