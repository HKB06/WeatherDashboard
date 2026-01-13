"""
Exporte les données HDFS (Parquet) vers un fichier JSON
À lancer une fois dans Spark pour alimenter le frontend
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc
import json

HDFS_PATH = "hdfs://namenode:9000/weather/realtime"
OUTPUT_PATH = "/home/jovyan/work/frontend/weather_data.json"

def main():
    print("=" * 60)
    print("Export HDFS → JSON")
    print("=" * 60)
    
    spark = SparkSession.builder \
        .appName("HDFS-Export") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"📂 Lecture depuis {HDFS_PATH}...")
    
    try:
        df = spark.read.parquet(HDFS_PATH)
        count = df.count()
        print(f"✅ {count} enregistrements trouvés")
        
        # Prendre les 500 derniers (triés par date)
        df_latest = df.orderBy(desc("event_time")).limit(500)
        
        # Convertir en JSON
        data = [row.asDict() for row in df_latest.collect()]
        
        # Convertir les types non-sérialisables
        for record in data:
            for key, value in record.items():
                if hasattr(value, 'isoformat'):
                    record[key] = value.isoformat()
        
        # Sauvegarder
        with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
            json.dump({
                "records": data,
                "count": count,
                "exported_at": str(spark.sparkContext.startTime)
            }, f, ensure_ascii=False, indent=2, default=str)
        
        print(f"✅ Exporté vers {OUTPUT_PATH}")
        print(f"   {len(data)} enregistrements sauvegardés")
        
        # Afficher un résumé
        cities = df.select("city", "country").distinct().collect()
        print(f"\n📍 Villes dans HDFS:")
        for c in cities:
            print(f"   - {c.city}, {c.country}")
        
    except Exception as e:
        print(f"❌ Erreur: {e}")
        print("   Vérifie que des données existent dans HDFS")
        print("   (Lance d'abord hdfs_weather_consumer.py)")
    
    spark.stop()
    print("\n✅ Export terminé!")

if __name__ == "__main__":
    main()
