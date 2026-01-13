"""
Producteur de données historiques météo (10 ans)
Utilise l'API Open-Meteo Archive pour récupérer l'historique
"""
from kafka import KafkaProducer
import requests
import json
import argparse
from datetime import datetime, timedelta
import time

KAFKA_BOOTSTRAP = "localhost:29092"
TOPIC = "weather_history"
ARCHIVE_API = "https://archive-api.open-meteo.com/v1/archive"
GEOCODING_API = "https://geocoding-api.open-meteo.com/v1/search"

def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None
    )

def geocode_city(city: str, country: str = None) -> dict:
    """Géocode une ville"""
    params = {"name": city, "count": 5, "language": "fr", "format": "json"}
    response = requests.get(GEOCODING_API, params=params)
    response.raise_for_status()
    data = response.json()
    
    if "results" not in data or not data["results"]:
        raise ValueError(f"Ville '{city}' non trouvée")
    
    results = data["results"]
    if country:
        filtered = [r for r in results if r.get("country", "").lower() == country.lower()]
        if filtered:
            results = filtered
    
    loc = results[0]
    return {
        "latitude": loc["latitude"],
        "longitude": loc["longitude"],
        "city": loc.get("name", city),
        "country": loc.get("country", "Unknown"),
        "timezone": loc.get("timezone", "UTC")
    }

def fetch_historical_data(lat: float, lon: float, start_date: str, end_date: str) -> dict:
    """Récupère les données historiques depuis Open-Meteo Archive"""
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "daily": [
            "temperature_2m_max",
            "temperature_2m_min",
            "temperature_2m_mean",
            "apparent_temperature_max",
            "apparent_temperature_min",
            "precipitation_sum",
            "rain_sum",
            "snowfall_sum",
            "precipitation_hours",
            "wind_speed_10m_max",
            "wind_gusts_10m_max",
            "wind_direction_10m_dominant",
            "shortwave_radiation_sum"
        ],
        "timezone": "auto"
    }
    response = requests.get(ARCHIVE_API, params=params, timeout=60)
    response.raise_for_status()
    return response.json()

def send_to_kafka(producer, data: dict, location: dict):
    """Envoie les données historiques jour par jour dans Kafka"""
    daily = data.get("daily", {})
    dates = daily.get("time", [])
    
    key = f"{location['country']}_{location['city']}".replace(" ", "_")
    count = 0
    
    for i, date in enumerate(dates):
        message = {
            "date": date,
            "city": location["city"],
            "country": location["country"],
            "latitude": location["latitude"],
            "longitude": location["longitude"],
            "timezone": location["timezone"],
            "temp_max": daily.get("temperature_2m_max", [None])[i],
            "temp_min": daily.get("temperature_2m_min", [None])[i],
            "temp_mean": daily.get("temperature_2m_mean", [None])[i],
            "apparent_temp_max": daily.get("apparent_temperature_max", [None])[i],
            "apparent_temp_min": daily.get("apparent_temperature_min", [None])[i],
            "precipitation": daily.get("precipitation_sum", [None])[i],
            "rain": daily.get("rain_sum", [None])[i],
            "snowfall": daily.get("snowfall_sum", [None])[i],
            "precipitation_hours": daily.get("precipitation_hours", [None])[i],
            "wind_max": daily.get("wind_speed_10m_max", [None])[i],
            "wind_gusts_max": daily.get("wind_gusts_10m_max", [None])[i],
            "wind_direction": daily.get("wind_direction_10m_dominant", [None])[i],
            "radiation": daily.get("shortwave_radiation_sum", [None])[i]
        }
        
        producer.send(TOPIC, value=message, key=key)
        count += 1
        
        if count % 365 == 0:
            print(f"   📤 {count} jours envoyés...")
    
    producer.flush()
    return count

def main():
    parser = argparse.ArgumentParser(description="Producteur données historiques météo")
    parser.add_argument("--city", "-c", type=str, required=True, help="Nom de la ville")
    parser.add_argument("--country", "-co", type=str, help="Pays (optionnel)")
    parser.add_argument("--years", "-y", type=int, default=10, help="Nombre d'années (défaut: 10)")
    args = parser.parse_args()

    print("=" * 60)
    print("Producteur Historique Météo")
    print("=" * 60)

    # Géocoder
    print(f"🔍 Recherche de '{args.city}'...")
    try:
        location = geocode_city(args.city, args.country)
    except ValueError as e:
        print(f"❌ {e}")
        return
    
    print(f"✅ {location['city']}, {location['country']}")
    print(f"   📍 {location['latitude']}, {location['longitude']}")

    # Calculer les dates
    end_date = datetime.now() - timedelta(days=5)  # API a ~5 jours de délai
    start_date = end_date - timedelta(days=365 * args.years)
    
    print(f"\n📅 Période: {start_date.strftime('%Y-%m-%d')} → {end_date.strftime('%Y-%m-%d')}")
    print(f"   ({args.years} ans = ~{args.years * 365} jours)")
    print(f"📤 Topic: {TOPIC}")
    print("-" * 60)

    # Récupérer et envoyer
    print("\n⏳ Récupération des données (peut prendre du temps)...")
    
    try:
        data = fetch_historical_data(
            location["latitude"],
            location["longitude"],
            start_date.strftime("%Y-%m-%d"),
            end_date.strftime("%Y-%m-%d")
        )
        
        producer = create_producer()
        count = send_to_kafka(producer, data, location)
        producer.close()
        
        print(f"\n✅ Terminé! {count} jours envoyés dans Kafka")
        
    except requests.RequestException as e:
        print(f"❌ Erreur API: {e}")
    except Exception as e:
        print(f"❌ Erreur: {e}")

if __name__ == "__main__":
    main()
