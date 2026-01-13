"""
Frontend de visualisation météo
- Données historiques depuis HDFS (exporté en JSON)
- Données temps réel depuis Kafka
"""
from flask import Flask, render_template, jsonify
from kafka import KafkaConsumer
import json
import threading
import os
from collections import deque
from datetime import datetime
import time

app = Flask(__name__)

# Configuration
KAFKA_BOOTSTRAP = "localhost:9092"
DATA_FILE = os.path.join(os.path.dirname(__file__), "weather_data.json")
TOPICS = ["weather_transformed", "weather_anomalies"]

# Buffers
MAX_MESSAGES = 100
data_buffer = {
    "realtime": deque(maxlen=MAX_MESSAGES),
    "anomalies": deque(maxlen=MAX_MESSAGES),
    "hdfs_data": [],
    "stats": {
        "total_messages": 0,
        "total_anomalies": 0,
        "hdfs_records": 0,
        "last_update": None,
        "cities": {},
        "kafka_status": "connecting",
        "hdfs_status": "not loaded"
    }
}

def load_hdfs_data():
    """Charge les données exportées depuis HDFS"""
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, 'r', encoding='utf-8') as f:
                exported = json.load(f)
            
            data_buffer["hdfs_data"] = exported.get("records", [])
            data_buffer["stats"]["hdfs_records"] = exported.get("count", 0)
            data_buffer["stats"]["hdfs_status"] = "loaded"
            
            for record in data_buffer["hdfs_data"]:
                city = record.get("city")
                if city:
                    data_buffer["stats"]["cities"][city] = {
                        "count": 1,
                        "last_temp": record.get("temperature"),
                        "last_wind": record.get("windspeed"),
                        "country": record.get("country", "")
                    }
            
            print(f"✅ HDFS: {len(data_buffer['hdfs_data'])} enregistrements chargés")
            return True
        except Exception as e:
            print(f"⚠️ Erreur: {e}")
    else:
        print(f"⚠️ {DATA_FILE} non trouvé")
        print("   Lance: docker exec -it pyspark_notebook spark-submit /home/jovyan/work/hdfs/export_to_json.py")
    
    data_buffer["stats"]["hdfs_status"] = "no data"
    return False

def consume_kafka():
    """Thread consommateur Kafka"""
    retries = 0
    
    while retries < 3:
        try:
            print(f"🔌 Connexion Kafka...")
            consumer = KafkaConsumer(
                *TOPICS,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id=f"frontend-{int(time.time())}",
                auto_offset_reset='earliest',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                consumer_timeout_ms=10000
            )
            
            data_buffer["stats"]["kafka_status"] = "connected"
            print("✅ Kafka connecté!")
            
            while True:
                messages = consumer.poll(timeout_ms=1000)
                for tp, msgs in messages.items():
                    for msg in msgs:
                        try:
                            data = msg.value
                            data["_timestamp"] = datetime.now().isoformat()
                            
                            if msg.topic == "weather_transformed":
                                data_buffer["realtime"].append(data)
                                data_buffer["stats"]["total_messages"] += 1
                                city = data.get("city")
                                if city:
                                    if city not in data_buffer["stats"]["cities"]:
                                        data_buffer["stats"]["cities"][city] = {
                                            "count": 0, "last_temp": None,
                                            "last_wind": None, "country": data.get("country", "")
                                        }
                                    data_buffer["stats"]["cities"][city]["count"] += 1
                                    data_buffer["stats"]["cities"][city]["last_temp"] = data.get("temperature")
                                    data_buffer["stats"]["cities"][city]["last_wind"] = data.get("windspeed")
                                print(f"📥 {city} | {data.get('temperature')}°C")
                                
                            elif msg.topic == "weather_anomalies":
                                data_buffer["anomalies"].append(data)
                                data_buffer["stats"]["total_anomalies"] += 1
                            
                            data_buffer["stats"]["last_update"] = datetime.now().isoformat()
                        except Exception as e:
                            print(f"⚠️ {e}")
                            
        except Exception as e:
            retries += 1
            data_buffer["stats"]["kafka_status"] = f"retry {retries}"
            print(f"❌ Kafka: {e}")
            time.sleep(3)
    
    data_buffer["stats"]["kafka_status"] = "offline"

# Routes API
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/realtime')
def api_realtime():
    return jsonify(list(data_buffer["realtime"]))

@app.route('/api/hdfs')
def api_hdfs():
    return jsonify(data_buffer["hdfs_data"])

@app.route('/api/anomalies')
def api_anomalies():
    return jsonify(list(data_buffer["anomalies"]))

@app.route('/api/stats')
def api_stats():
    return jsonify(data_buffer["stats"])

@app.route('/api/latest')
def api_latest():
    latest = {}
    for record in data_buffer["hdfs_data"]:
        city = record.get("city")
        if city and city not in latest:
            latest[city] = record
    for msg in list(data_buffer["realtime"]):
        city = msg.get("city")
        if city:
            latest[city] = msg
    return jsonify(latest)

@app.route('/api/reload')
def api_reload():
    success = load_hdfs_data()
    return jsonify({"success": success, "records": len(data_buffer["hdfs_data"])})

@app.route('/api/status')
def api_status():
    return jsonify({
        "kafka": data_buffer["stats"]["kafka_status"],
        "hdfs": data_buffer["stats"]["hdfs_status"],
        "hdfs_records": data_buffer["stats"]["hdfs_records"],
        "realtime_messages": data_buffer["stats"]["total_messages"],
        "anomalies": data_buffer["stats"]["total_anomalies"]
    })

if __name__ == "__main__":
    print("=" * 60)
    print("🌤️  Weather Dashboard")
    print("=" * 60)
    
    load_hdfs_data()
    
    print(f"📡 Kafka: {KAFKA_BOOTSTRAP}")
    print(f"🌐 http://localhost:5000")
    print("-" * 60)
    
    kafka_thread = threading.Thread(target=consume_kafka, daemon=True)
    kafka_thread.start()
    
    print("🚀 Serveur démarré!")
    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)
