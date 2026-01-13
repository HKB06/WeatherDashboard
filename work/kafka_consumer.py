"""
Consommateur Kafka générique
Lit les messages depuis un topic passé en argument
"""
from kafka import KafkaConsumer
import json
import argparse
from datetime import datetime

KAFKA_BOOTSTRAP = "localhost:29092"

def consume(topic: str):
    """Consomme les messages d'un topic en temps réel"""
    print(f"🎧 Écoute du topic '{topic}'...")
    print(f"   Serveur: {KAFKA_BOOTSTRAP}")
    print("   (Ctrl+C pour arrêter)")
    print("-" * 50)
    
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="weather-consumer",
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    
    try:
        for msg in consumer:
            ts = datetime.fromtimestamp(msg.timestamp / 1000).strftime('%H:%M:%S')
            print(f"\n📩 [{ts}] Partition {msg.partition} | Offset {msg.offset}")
            print(f"   {json.dumps(msg.value, indent=2, ensure_ascii=False)}")
    except KeyboardInterrupt:
        print("\n\n⏹️ Arrêt du consommateur")
    finally:
        consumer.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Consommateur Kafka")
    parser.add_argument("topic", type=str, help="Nom du topic à consommer")
    args = parser.parse_args()
    
    print("=" * 40)
    print("Consommateur Kafka")
    print("=" * 40)
    
    consume(args.topic)
