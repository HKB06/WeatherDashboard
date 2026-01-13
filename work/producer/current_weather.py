"""
Producteur météo temps réel (coordonnées GPS)
Interroge l'API Open-Meteo avec lat/lon et publie dans Kafka
"""
from kafka import KafkaProducer
import requests
import json
import argparse
import time
from datetime import datetime, timezone

KAFKA_BOOTSTRAP = "localhost:29092"
TOPIC = "weather_stream"
OPEN_METEO_API = "https://api.open-meteo.com/v1/forecast"

def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None
    )

def get_weather(lat: float, lon: float) -> dict:
    """Récupère les données météo depuis Open-Meteo"""
    params = {
        "latitude": lat,
        "longitude": lon,
        "current": [
            "temperature_2m",
            "relative_humidity_2m",
            "apparent_temperature",
            "precipitation",
            "weather_code",
            "cloud_cover",
            "pressure_msl",
            "wind_speed_10m",
            "wind_direction_10m",
            "wind_gusts_10m"
        ],
        "timezone": "auto"
    }
    response = requests.get(OPEN_METEO_API, params=params)
    response.raise_for_status()
    return response.json()

def format_message(api_data: dict, lat: float, lon: float) -> dict:
    """Formate les données pour Kafka"""
    current = api_data.get("current", {})
    return {
        "event_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "latitude": lat,
        "longitude": lon,
        "timezone": api_data.get("timezone", "UTC"),
        "elevation": api_data.get("elevation", 0),
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
    parser = argparse.ArgumentParser(description="Producteur météo temps réel")
    parser.add_argument("--lat", type=float, required=True, help="Latitude")
    parser.add_argument("--lon", type=float, required=True, help="Longitude")
    parser.add_argument("--interval", type=int, default=60, help="Intervalle en secondes")
    args = parser.parse_args()

    print("=" * 50)
    print("Producteur Météo (GPS)")
    print("=" * 50)
    print(f"📍 Lat: {args.lat}, Lon: {args.lon}")
    print(f"⏱️  Intervalle: {args.interval}s")
    print(f"📤 Topic: {TOPIC}")
    print("-" * 50)

    producer = create_producer()
    key = f"{args.lat}_{args.lon}"
    count = 0

    try:
        while True:
            try:
                data = get_weather(args.lat, args.lon)
                message = format_message(data, args.lat, args.lon)
                
                producer.send(TOPIC, value=message, key=key)
                producer.flush()
                
                count += 1
                temp = message['temperature']
                wind = message['wind_speed']
                print(f"📤 #{count} | 🌡️ {temp}°C | 💨 {wind} m/s | ☁️ {message['cloud_cover']}%")
                
            except requests.RequestException as e:
                print(f"⚠️ Erreur API: {e}")
            except Exception as e:
                print(f"⚠️ Erreur: {e}")
            
            time.sleep(args.interval)
            
    except KeyboardInterrupt:
        print(f"\n\n⏹️ Arrêté après {count} messages")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
