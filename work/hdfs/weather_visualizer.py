"""
Visualisation et agrégation des logs météo depuis HDFS
Lit les données Parquet et génère des statistiques
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, min as spark_min, max as spark_max, count,
    round as spark_round, desc, sum as spark_sum, when
)
import argparse

HDFS_PATH = "hdfs://namenode:9000/weather/realtime"

def create_spark():
    return SparkSession.builder \
        .appName("WeatherVisualizer") \
        .getOrCreate()

def load_data(spark, country=None, city=None):
    """Charge les données depuis HDFS avec filtres optionnels"""
    df = spark.read.parquet(HDFS_PATH)
    
    if country:
        df = df.filter(col("country") == country)
    if city:
        df = df.filter(col("city") == city)
    
    return df

def show_summary(df):
    """Affiche un résumé global des données"""
    print("\n" + "=" * 60)
    print("📊 RÉSUMÉ GLOBAL")
    print("=" * 60)
    
    total = df.count()
    print(f"Total enregistrements: {total}")
    
    # Villes couvertes
    cities = df.select("country", "city").distinct().collect()
    print(f"Villes couvertes: {len(cities)}")
    for row in cities[:10]:
        print(f"   - {row.city}, {row.country}")
    if len(cities) > 10:
        print(f"   ... et {len(cities) - 10} autres")

def show_temperature_stats(df):
    """Statistiques de température par ville"""
    print("\n" + "=" * 60)
    print("🌡️ STATISTIQUES TEMPÉRATURE")
    print("=" * 60)
    
    stats = df.groupBy("country", "city") \
        .agg(
            spark_round(avg("temperature"), 1).alias("temp_moy"),
            spark_round(spark_min("temperature"), 1).alias("temp_min"),
            spark_round(spark_max("temperature"), 1).alias("temp_max"),
            count("*").alias("nb_mesures")
        ) \
        .orderBy(desc("nb_mesures"))
    
    stats.show(20, truncate=False)
    return stats

def show_alert_stats(df):
    """Statistiques des alertes"""
    print("\n" + "=" * 60)
    print("🚨 STATISTIQUES ALERTES")
    print("=" * 60)
    
    alerts = df.groupBy("country", "city") \
        .agg(
            spark_sum(when(col("wind_alert_level") != "level_0", 1).otherwise(0)).alias("alertes_vent"),
            spark_sum(when(col("heat_alert_level") != "level_0", 1).otherwise(0)).alias("alertes_chaleur"),
            spark_sum(when(col("wind_alert_level") == "level_2", 1).otherwise(0)).alias("alertes_vent_critiques"),
            spark_sum(when(col("heat_alert_level") == "level_2", 1).otherwise(0)).alias("alertes_chaleur_critiques"),
            count("*").alias("total")
        ) \
        .orderBy(desc("alertes_vent"))
    
    alerts.show(20, truncate=False)
    return alerts

def show_daily_evolution(df, city=None):
    """Évolution journalière"""
    print("\n" + "=" * 60)
    print("📈 ÉVOLUTION JOURNALIÈRE")
    print("=" * 60)
    
    query = df
    if city:
        query = query.filter(col("city") == city)
        print(f"Ville: {city}")
    
    daily = query.groupBy("date") \
        .agg(
            spark_round(avg("temperature"), 1).alias("temp_moy"),
            spark_round(spark_min("temperature"), 1).alias("temp_min"),
            spark_round(spark_max("temperature"), 1).alias("temp_max"),
            spark_round(avg("windspeed"), 1).alias("vent_moy"),
            count("*").alias("mesures")
        ) \
        .orderBy("date")
    
    daily.show(30, truncate=False)
    return daily

def show_extremes(df):
    """Records et extrêmes"""
    print("\n" + "=" * 60)
    print("🏆 RECORDS ET EXTRÊMES")
    print("=" * 60)
    
    # Température max
    max_temp = df.orderBy(desc("temperature")).first()
    if max_temp:
        print(f"🔥 Température max: {max_temp.temperature}°C")
        print(f"   {max_temp.city}, {max_temp.country} - {max_temp.event_time}")
    
    # Température min
    min_temp = df.orderBy("temperature").first()
    if min_temp:
        print(f"❄️ Température min: {min_temp.temperature}°C")
        print(f"   {min_temp.city}, {min_temp.country} - {min_temp.event_time}")
    
    # Vent max
    max_wind = df.orderBy(desc("windspeed")).first()
    if max_wind:
        print(f"💨 Vent max: {max_wind.windspeed} m/s")
        print(f"   {max_wind.city}, {max_wind.country} - {max_wind.event_time}")

def main():
    parser = argparse.ArgumentParser(description="Visualisation météo HDFS")
    parser.add_argument("--country", type=str, help="Filtrer par pays")
    parser.add_argument("--city", type=str, help="Filtrer par ville")
    parser.add_argument("--mode", type=str, default="all", 
                       choices=["all", "summary", "temp", "alerts", "daily", "extremes"],
                       help="Mode de visualisation")
    args = parser.parse_args()

    print("=" * 60)
    print("Weather Data Visualizer")
    print("=" * 60)
    print(f"📁 Source: {HDFS_PATH}")
    if args.country:
        print(f"🌍 Pays: {args.country}")
    if args.city:
        print(f"🏙️ Ville: {args.city}")

    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:
        df = load_data(spark, args.country, args.city)
        
        if df.count() == 0:
            print("\n⚠️ Aucune donnée trouvée!")
            return

        if args.mode in ["all", "summary"]:
            show_summary(df)
        
        if args.mode in ["all", "temp"]:
            show_temperature_stats(df)
        
        if args.mode in ["all", "alerts"]:
            show_alert_stats(df)
        
        if args.mode in ["all", "daily"]:
            show_daily_evolution(df, args.city)
        
        if args.mode in ["all", "extremes"]:
            show_extremes(df)

    except Exception as e:
        print(f"\n❌ Erreur: {e}")
        print("   Vérifiez que des données existent dans HDFS")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
