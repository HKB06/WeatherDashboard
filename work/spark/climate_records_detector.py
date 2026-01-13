"""
Détection de records climatiques
Analyse les données historiques pour identifier les extrêmes
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, min as spark_min, max as spark_max, count,
    round as spark_round, desc, asc, year, month, dayofmonth,
    when, lit, percentile_approx, stddev, row_number
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)

KAFKA_BOOTSTRAP = "kafka:9092"
INPUT_TOPIC = "weather_history"

# Schéma des données historiques
SCHEMA = StructType([
    StructField("date", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("timezone", StringType(), True),
    StructField("temp_max", DoubleType(), True),
    StructField("temp_min", DoubleType(), True),
    StructField("temp_mean", DoubleType(), True),
    StructField("apparent_temp_max", DoubleType(), True),
    StructField("apparent_temp_min", DoubleType(), True),
    StructField("precipitation", DoubleType(), True),
    StructField("rain", DoubleType(), True),
    StructField("snowfall", DoubleType(), True),
    StructField("precipitation_hours", DoubleType(), True),
    StructField("wind_max", DoubleType(), True),
    StructField("wind_gusts_max", DoubleType(), True),
    StructField("wind_direction", IntegerType(), True),
    StructField("radiation", DoubleType(), True)
])

def create_spark():
    return SparkSession.builder \
        .appName("ClimateRecordsDetector") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()

def load_from_kafka(spark):
    """Charge toutes les données historiques depuis Kafka"""
    from pyspark.sql.functions import from_json
    
    df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", INPUT_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()
    
    return df.select(
        from_json(col("value").cast("string"), SCHEMA).alias("data")
    ).select("data.*").filter(col("date").isNotNull())

def detect_temperature_records(df):
    """Détecte les records de température"""
    print("\n" + "=" * 60)
    print("🌡️ RECORDS DE TEMPÉRATURE")
    print("=" * 60)
    
    # Top 10 journées les plus chaudes
    print("\n🔥 Top 10 - Journées les plus chaudes:")
    df.orderBy(desc("temp_max")) \
        .select("date", "city", "country", "temp_max", "temp_min") \
        .show(10, truncate=False)
    
    # Top 10 journées les plus froides
    print("\n❄️ Top 10 - Journées les plus froides:")
    df.filter(col("temp_min").isNotNull()) \
        .orderBy(asc("temp_min")) \
        .select("date", "city", "country", "temp_min", "temp_max") \
        .show(10, truncate=False)
    
    # Plus grande amplitude thermique
    print("\n📊 Top 10 - Plus grande amplitude journalière:")
    df.withColumn("amplitude", col("temp_max") - col("temp_min")) \
        .orderBy(desc("amplitude")) \
        .select("date", "city", "country", "temp_min", "temp_max", "amplitude") \
        .show(10, truncate=False)

def detect_wind_records(df):
    """Détecte les records de vent"""
    print("\n" + "=" * 60)
    print("💨 RECORDS DE VENT")
    print("=" * 60)
    
    # Top 10 vents les plus forts
    print("\n🌪️ Top 10 - Vents les plus forts:")
    df.filter(col("wind_max").isNotNull()) \
        .orderBy(desc("wind_max")) \
        .select("date", "city", "country", "wind_max", "wind_gusts_max") \
        .show(10, truncate=False)
    
    # Top 10 rafales
    print("\n💥 Top 10 - Rafales les plus fortes:")
    df.filter(col("wind_gusts_max").isNotNull()) \
        .orderBy(desc("wind_gusts_max")) \
        .select("date", "city", "country", "wind_gusts_max", "wind_max") \
        .show(10, truncate=False)

def detect_precipitation_records(df):
    """Détecte les records de précipitations"""
    print("\n" + "=" * 60)
    print("🌧️ RECORDS DE PRÉCIPITATIONS")
    print("=" * 60)
    
    # Top 10 journées les plus pluvieuses
    print("\n☔ Top 10 - Journées les plus pluvieuses:")
    df.filter(col("precipitation").isNotNull()) \
        .orderBy(desc("precipitation")) \
        .select("date", "city", "country", "precipitation", "rain", "snowfall") \
        .show(10, truncate=False)
    
    # Top 10 chutes de neige
    print("\n❄️ Top 10 - Plus fortes chutes de neige:")
    df.filter(col("snowfall") > 0) \
        .orderBy(desc("snowfall")) \
        .select("date", "city", "country", "snowfall", "temp_min", "temp_max") \
        .show(10, truncate=False)

def compute_monthly_stats(df):
    """Calcule les statistiques mensuelles"""
    print("\n" + "=" * 60)
    print("📅 STATISTIQUES MENSUELLES")
    print("=" * 60)
    
    monthly = df \
        .withColumn("month", month(col("date"))) \
        .groupBy("city", "country", "month") \
        .agg(
            spark_round(avg("temp_max"), 1).alias("avg_temp_max"),
            spark_round(avg("temp_min"), 1).alias("avg_temp_min"),
            spark_round(spark_max("temp_max"), 1).alias("record_max"),
            spark_round(spark_min("temp_min"), 1).alias("record_min"),
            spark_round(avg("precipitation"), 1).alias("avg_precip"),
            spark_round(avg("wind_max"), 1).alias("avg_wind")
        ) \
        .orderBy("city", "month")
    
    monthly.show(24, truncate=False)
    return monthly

def compute_yearly_evolution(df):
    """Analyse l'évolution annuelle"""
    print("\n" + "=" * 60)
    print("📈 ÉVOLUTION ANNUELLE")
    print("=" * 60)
    
    yearly = df \
        .withColumn("year", year(col("date"))) \
        .groupBy("city", "country", "year") \
        .agg(
            spark_round(avg("temp_mean"), 2).alias("temp_moyenne"),
            spark_round(spark_max("temp_max"), 1).alias("temp_max_annee"),
            spark_round(spark_min("temp_min"), 1).alias("temp_min_annee"),
            spark_round(avg("precipitation") * 365, 0).alias("precip_totale_mm"),
            count("*").alias("nb_jours")
        ) \
        .orderBy("city", "year")
    
    yearly.show(20, truncate=False)
    return yearly

def detect_anomalies(df):
    """Détecte les anomalies statistiques"""
    print("\n" + "=" * 60)
    print("⚠️ ANOMALIES DÉTECTÉES")
    print("=" * 60)
    
    # Calculer moyenne et écart-type
    stats = df.groupBy("city", "country").agg(
        avg("temp_max").alias("avg_temp"),
        stddev("temp_max").alias("std_temp")
    )
    
    # Joindre et détecter les anomalies (> 3 écarts-types)
    anomalies = df.join(stats, ["city", "country"]) \
        .withColumn("z_score", (col("temp_max") - col("avg_temp")) / col("std_temp")) \
        .filter((col("z_score") > 3) | (col("z_score") < -3)) \
        .select("date", "city", "country", "temp_max", 
                spark_round("z_score", 2).alias("z_score")) \
        .orderBy(desc("z_score"))
    
    print("Journées avec température anormale (|z| > 3):")
    anomalies.show(20, truncate=False)
    return anomalies

def main():
    print("=" * 60)
    print("Climate Records Detector")
    print("=" * 60)
    print(f"📥 Source: {INPUT_TOPIC}")
    print("-" * 60)

    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:
        print("\n⏳ Chargement des données...")
        df = load_from_kafka(spark)
        
        total = df.count()
        if total == 0:
            print("⚠️ Aucune donnée trouvée!")
            print("   Lancez d'abord le producteur historique:")
            print("   python work/producer/weather_history_producer.py --city Paris")
            return
        
        print(f"✅ {total} jours de données chargés")
        
        # Analyses
        detect_temperature_records(df)
        detect_wind_records(df)
        detect_precipitation_records(df)
        compute_monthly_stats(df)
        compute_yearly_evolution(df)
        detect_anomalies(df)
        
        print("\n" + "=" * 60)
        print("✅ Analyse terminée!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ Erreur: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
