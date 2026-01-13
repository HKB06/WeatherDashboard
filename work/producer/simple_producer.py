"""
Producteur Kafka simple
- Crée le topic weather_stream
- Envoie un message test
"""
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import json

KAFKA_BOOTSTRAP = "localhost:29092"
TOPIC = "weather_stream"

def create_topic():
    """Crée le topic weather_stream s'il n'existe pas"""
    try:
        admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP)
        topic = NewTopic(name=TOPIC, num_partitions=3, replication_factor=1)
        admin.create_topics([topic])
        print(f"✅ Topic '{TOPIC}' créé!")
    except TopicAlreadyExistsError:
        print(f"ℹ️ Topic '{TOPIC}' existe déjà")
    except Exception as e:
        print(f"⚠️ Erreur création topic: {e}")

def send_message():
    """Envoie un message test"""
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    message = {"msg": "Hello Kafka"}
    
    print(f"\n📤 Envoi: {message}")
    future = producer.send(TOPIC, value=message)
    result = future.get(timeout=10)
    
    print(f"✅ Message envoyé!")
    print(f"   Topic: {result.topic}")
    print(f"   Partition: {result.partition}")
    print(f"   Offset: {result.offset}")
    
    producer.close()

if __name__ == "__main__":
    print("=" * 40)
    print("Producteur Kafka Simple")
    print("=" * 40)
    
    print("\n📋 Création du topic...")
    create_topic()
    
    print("\n📨 Envoi du message...")
    send_message()
    
    print("\n🎉 Terminé!")
