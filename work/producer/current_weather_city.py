"""
Producteur météo avec géocodage ville/pays
Utilise l'API Open-Meteo Geocoding pour convertir ville → coordonnées
"""
from kafka import KafkaProducer
import requests
import json
import argparse
import time
from datetime import datetime, timezone

KAFKA_BOOTSTRAP = "localhost:29092"
TOPIC = "weather_stream"
GEOCODING_API = "https://geocoding-api.open-meteo.com/v1/search"
WEATHER_API = "https://api.open-meteo.com/v1/forecast"

def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None
    )

def geocode_city(city: str, country: str = None) -> dict:
    """Géocode une ville via l'API Open-Meteo"""
    params = {
        "name": city,
        "count": 10,
        "language": "fr",
        "format": "json"
    }
    
    response = requests.get(GEOCODING_API, params=params)
    response.raise_for_status()
    data = response.json()
    
    if "results" not in data or not data["results"]:
        raise ValueError(f"Ville '{city}' non trouvée")
    
    results = data["results"]
    
    # Filtrer par pays si spécifié
    if country:
        filtered = [r for r in results if r.get("country", "").lower() == country.lower()]
        if filtered:
            results = filtered
    
    loc = results[0]
    return {
        "latitude": loc["latitude"],
        "longitude": loc["longitude"],
        "city": loc.get("name", city),
        "country": loc.get("country", country or "Unknown"),
        "country_code": loc.get("country_code", ""),
        "timezone": loc.get("timezone", "UTC"),
        "elevation": loc.get("elevation", 0),
        "admin1": loc.get("admin1", ""),
        "admin2": loc.get("admin2", "")
    }

def get_weather(lat: float, lon: float) -> dict:
    """Récupère les données météo actuelles"""
    params = {
        "latitude": lat,
        "longitude": lon,
        "current": [
            "temperature_2m", "relative_humidity_2m", "apparent_temperature",
            "precipitation", "weather_code", "cloud_cover", "pressure_msl",
            "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m"
        ],
        "timezone": "auto"
    }
    response = requests.get(WEATHER_API, params=params)
    response.raise_for_status()
    return response.json()

def format_message(api_data: dict, location: dict) -> dict:
    """Formate les données avec ville/pays inclus"""
    current = api_data.get("current", {})
    return {
        "event_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "latitude": location["latitude"],
        "longitude": location["longitude"],
        "city": location["city"],
        "country": location["country"],
        "country_code": location["country_code"],
        "timezone": location["timezone"],
        "elevation": location["elevation"],
        "admin1": location["admin1"],
        "temperature": current.get("temperature_2m"),
        "apparent_temperature": current.get("apparent_temperature"),
        "humidity": current.get("relative_humidity_2m"),
        "precipitation": current.get("precipitation"),
        "weather_code": current.get("weather_code"),
        "cloud_cover": current.get("cloud_cover"),
        "pressure_msl": current.get("pressure_msl"),
        "wind_speed": current.get("wind_speed_10m"),
        "wind_direction": current.get("wind_direction_10m"),
        "wind_gusts": current.get("wind_gusts_10m")
    }

def main():
    parser = argparse.ArgumentParser(description="Producteur météo par ville")
    parser.add_argument("--city", "-c", type=str, required=True, help="Nom de la ville")
    parser.add_argument("--country", "-co", type=str, default=None, help="Nom du pays (optionnel)")
    parser.add_argument("--interval", "-i", type=int, default=60, help="Intervalle en secondes")
    args = parser.parse_args()

    print("=" * 60)
    print("Producteur Météo (Ville)")
    print("=" * 60)

    print(f"🔍 Recherche de '{args.city}'...")
    try:
        location = geocode_city(args.city, args.country)
    except ValueError as e:
        print(f"❌ {e}")
        return
    
    print(f"✅ Trouvé: {location['city']}, {location['country']}")
    print(f"   📍 Lat: {location['latitude']}, Lon: {location['longitude']}")
    print(f"   🌍 Région: {location['admin1']}")
    print(f"   ⏰ Timezone: {location['timezone']}")
    print(f"   📤 Topic: {TOPIC}")
    print("-" * 60)

    producer = create_producer()
    key = f"{location['country']}_{location['city']}".replace(" ", "_")
    count = 0

    try:
        while True:
            try:
                data = get_weather(location["latitude"], location["longitude"])
                message = format_message(data, location)
                
                producer.send(TOPIC, value=message, key=key)
                producer.flush()
                
                count += 1
                print(f"📤 #{count} {location['city']} | 🌡️ {message['temperature']}°C | 💨 {message['wind_speed']} m/s")
                
            except requests.RequestException as e:
                print(f"⚠️ Erreur API: {e}")
            
            time.sleep(args.interval)
            
    except KeyboardInterrupt:
        print(f"\n\n⏹️ Arrêté après {count} messages")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
