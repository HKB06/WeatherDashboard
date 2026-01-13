"""
Climatologie urbaine - Profils saisonniers
Calcule les profils climatiques par saison à partir des données historiques
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, min as spark_min, max as spark_max, count, stddev,
    round as spark_round, month, when, lit, percentile_approx
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)

KAFKA_BOOTSTRAP = "kafka:9092"
INPUT_TOPIC = "weather_history"
HDFS_OUTPUT = "hdfs://namenode:9000/weather/seasonal_profiles"

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

def get_season(month_col):
    """Détermine la saison à partir du mois"""
    return when(month_col.isin([12, 1, 2]), lit("Hiver")) \
           .when(month_col.isin([3, 4, 5]), lit("Printemps")) \
           .when(month_col.isin([6, 7, 8]), lit("Été")) \
           .otherwise(lit("Automne"))

def create_spark():
    return SparkSession.builder \
        .appName("SeasonalProfileCalculator") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()

def load_data(spark):
    """Charge les données depuis Kafka"""
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

def compute_seasonal_profiles(df):
    """Calcule les profils saisonniers"""
    print("\n" + "=" * 60)
    print("🌍 PROFILS SAISONNIERS")
    print("=" * 60)
    
    # Ajouter mois et saison
    df_with_season = df \
        .withColumn("month", month(col("date"))) \
        .withColumn("season", get_season(col("month")))
    
    # Profils par saison
    profiles = df_with_season.groupBy("city", "country", "season") \
        .agg(
            # Température
            spark_round(avg("temp_mean"), 1).alias("temp_moyenne"),
            spark_round(avg("temp_max"), 1).alias("temp_max_moy"),
            spark_round(avg("temp_min"), 1).alias("temp_min_moy"),
            spark_round(spark_max("temp_max"), 1).alias("temp_record_max"),
            spark_round(spark_min("temp_min"), 1).alias("temp_record_min"),
            spark_round(stddev("temp_mean"), 2).alias("temp_ecart_type"),
            
            # Précipitations
            spark_round(avg("precipitation"), 2).alias("precip_moy_jour"),
            spark_round(spark_max("precipitation"), 1).alias("precip_record"),
            spark_round(avg("snowfall"), 2).alias("neige_moy_jour"),
            
            # Vent
            spark_round(avg("wind_max"), 1).alias("vent_moy"),
            spark_round(spark_max("wind_max"), 1).alias("vent_record"),
            spark_round(spark_max("wind_gusts_max"), 1).alias("rafale_record"),
            
            # Ensoleillement
            spark_round(avg("radiation"), 1).alias("radiation_moy"),
            
            # Comptage
            count("*").alias("nb_jours")
        ) \
        .orderBy("city", 
                 when(col("season") == "Hiver", 1)
                 .when(col("season") == "Printemps", 2)
                 .when(col("season") == "Été", 3)
                 .otherwise(4))
    
    profiles.show(20, truncate=False)
    return profiles

def compute_monthly_normals(df):
    """Calcule les normales mensuelles (moyennes sur toute la période)"""
    print("\n" + "=" * 60)
    print("📊 NORMALES MENSUELLES (moyennes climatiques)")
    print("=" * 60)
    
    normals = df.withColumn("month", month(col("date"))) \
        .groupBy("city", "country", "month") \
        .agg(
            spark_round(avg("temp_mean"), 1).alias("temp_normale"),
            spark_round(avg("temp_max"), 1).alias("temp_max_normale"),
            spark_round(avg("temp_min"), 1).alias("temp_min_normale"),
            spark_round(stddev("temp_mean"), 2).alias("ecart_type"),
            spark_round(avg("precipitation") * 30, 0).alias("precip_mensuelle_mm"),
            spark_round(avg("wind_max"), 1).alias("vent_moyen"),
            count("*").alias("nb_jours_mesures")
        ) \
        .orderBy("city", "month")
    
    normals.show(24, truncate=False)
    return normals

def compute_extreme_thresholds(df):
    """Calcule les seuils d'extrêmes (percentiles 5 et 95)"""
    print("\n" + "=" * 60)
    print("⚠️ SEUILS D'EXTRÊMES (P5 et P95)")
    print("=" * 60)
    
    df_with_month = df.withColumn("month", month(col("date")))
    
    thresholds = df_with_month.groupBy("city", "country", "month") \
        .agg(
            spark_round(percentile_approx("temp_max", 0.05), 1).alias("temp_max_P5"),
            spark_round(percentile_approx("temp_max", 0.95), 1).alias("temp_max_P95"),
            spark_round(percentile_approx("temp_min", 0.05), 1).alias("temp_min_P5"),
            spark_round(percentile_approx("temp_min", 0.95), 1).alias("temp_min_P95"),
            spark_round(percentile_approx("wind_max", 0.95), 1).alias("vent_P95"),
            spark_round(percentile_approx("precipitation", 0.95), 1).alias("precip_P95")
        ) \
        .orderBy("city", "month")
    
    thresholds.show(24, truncate=False)
    return thresholds

def save_to_hdfs(profiles, normals, thresholds):
    """Sauvegarde les profils dans HDFS"""
    print("\n💾 Sauvegarde dans HDFS...")
    
    profiles.write.mode("overwrite").parquet(f"{HDFS_OUTPUT}/seasonal")
    normals.write.mode("overwrite").parquet(f"{HDFS_OUTPUT}/monthly_normals")
    thresholds.write.mode("overwrite").parquet(f"{HDFS_OUTPUT}/thresholds")
    
    print(f"✅ Sauvegardé dans {HDFS_OUTPUT}")

def main():
    print("=" * 60)
    print("Seasonal Profile Calculator")
    print("=" * 60)
    print(f"📥 Source: {INPUT_TOPIC}")
    print(f"📁 Destination: {HDFS_OUTPUT}")
    print("-" * 60)

    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:
        print("\n⏳ Chargement des données...")
        df = load_data(spark)
        
        total = df.count()
        if total == 0:
            print("⚠️ Aucune donnée trouvée!")
            return
        
        print(f"✅ {total} jours chargés")
        
        # Calculs
        profiles = compute_seasonal_profiles(df)
        normals = compute_monthly_normals(df)
        thresholds = compute_extreme_thresholds(df)
        
        # Sauvegarder
        save_to_hdfs(profiles, normals, thresholds)
        
        print("\n" + "=" * 60)
        print("✅ Profils saisonniers calculés!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ Erreur: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
