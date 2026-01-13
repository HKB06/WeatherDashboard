# 🌤️ Kafka Weather Project

Projet de streaming météo temps réel utilisant **Apache Kafka**, **Spark Streaming**, **HDFS** et **Flask**.

## 📋 Description

Ce projet implémente une pipeline de données météo complète :
- **Collecte** : Récupération des données météo via l'API Open-Meteo
- **Streaming** : Transmission en temps réel via Apache Kafka
- **Traitement** : Transformations et détection d'alertes avec Spark Streaming
- **Stockage** : Persistance dans HDFS au format Parquet
- **Visualisation** : Dashboard web avec historique et temps réel

## 🏗️ Architecture

```
┌─────────────────┐     ┌─────────────┐     ┌─────────────────┐
│   Open-Meteo    │────▶│    Kafka    │────▶│  Spark Stream   │
│      API        │     │             │     │  (Transform)    │
└─────────────────┘     └─────────────┘     └────────┬────────┘
                                                     │
                        ┌─────────────┐              │
                        │   Frontend  │◀─────────────┤
                        │   (Flask)   │              │
                        └─────────────┘              ▼
                                            ┌─────────────────┐
                                            │      HDFS       │
                                            │   (Parquet)     │
                                            └─────────────────┘
```

## 📦 Prérequis

- **Docker** et **Docker Compose**
- **Python 3.10+**
- **pip**

## 🚀 Installation

### 1. Cloner le projet
```bash
git clone <repo-url>
cd Sem15Kafka
```

### 2. Lancer les conteneurs Docker
```bash
docker-compose up -d
```

Services démarrés :
| Service | Port | Description |
|---------|------|-------------|
| Kafka | 9092 | Broker de messages |
| Zookeeper | 2181 | Coordination Kafka |
| Spark Master | 8080 | Interface Spark |
| Spark Worker | - | Exécution des jobs |
| HDFS Namenode | 9870 | Interface HDFS |
| HDFS Datanode | 9864 | Stockage HDFS |
| PySpark Notebook | 8888 | Jupyter + Spark |

### 3. Installer les dépendances Python (local)
```bash
cd work
pip install -r requirements.txt
```

## 🎯 Lancement

### Étape 1 : Créer le topic et tester Kafka

```bash
# Terminal 1 - Producteur simple
python producer/simple_producer.py

# Terminal 2 - Consumer
python kafka_consumer.py weather_stream
```

### Étape 2 : Lancer le producteur météo temps réel

```bash
# Météo pour Paris (toutes les 30 secondes)
python producer/current_weather_city.py --city Paris --interval 30
```

### Étape 3 : Lancer Spark Streaming (transformation + alertes)

```bash
docker exec -it pyspark_notebook spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /home/jovyan/work/spark/spark_weather_stream.py
```

### Étape 4 : Stocker dans HDFS

```bash
docker exec -it pyspark_notebook spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /home/jovyan/work/hdfs/hdfs_weather_consumer.py
```

### Étape 5 : Lancer le Dashboard

```bash
# Exporter les données HDFS vers JSON
docker exec -it pyspark_notebook spark-submit \
  /home/jovyan/work/hdfs/export_to_json.py

# Lancer le frontend
python frontend/app.py
```

Ouvrir **http://localhost:5000** dans le navigateur.

## 📁 Structure du Projet

```
work/
├── kafka_consumer.py              # Consumer Kafka générique
├── requirements.txt               # Dépendances Python
│
├── producer/
│   ├── simple_producer.py         # Test Kafka basique
│   ├── current_weather.py         # Météo par coordonnées
│   ├── current_weather_city.py    # Météo par ville
│   └── weather_history_producer.py # Historique 10 ans
│
├── spark/
│   ├── spark_weather_stream.py    # Transformation + alertes
│   ├── spark_weather_aggregates.py # Agrégats sliding windows
│   ├── climate_records_detector.py # Détection records
│   ├── seasonal_profile.py        # Calcul normales saisonnières
│   ├── seasonal_enricher.py       # Enrichissement temps réel
│   └── anomaly_detector.py        # Détection anomalies
│
├── hdfs/
│   ├── hdfs_weather_consumer.py   # Stockage HDFS
│   ├── weather_visualizer.py      # Visualisation batch
│   └── export_to_json.py          # Export HDFS → JSON
│
└── frontend/
    ├── app.py                     # Serveur Flask
    ├── templates/index.html       # Dashboard HTML
    └── weather_data.json          # Cache données HDFS
```

## 📊 Topics Kafka

| Topic | Description |
|-------|-------------|
| `weather_stream` | Données brutes Open-Meteo |
| `weather_transformed` | Données transformées + alertes |
| `weather_history` | Données historiques (10 ans) |
| `weather_enriched` | Données enrichies avec normales |
| `weather_anomalies` | Anomalies détectées |

## 🔔 Niveaux d'Alertes

### Alertes Vent
| Niveau | Condition | Description |
|--------|-----------|-------------|
| level_0 | ≤ 10 m/s | Vent faible |
| level_1 | 10-20 m/s | Vent modéré |
| level_2 | > 20 m/s | Vent fort |

### Alertes Chaleur
| Niveau | Condition | Description |
|--------|-----------|-------------|
| level_0 | ≤ 25°C | Normal |
| level_1 | 25-35°C | Chaleur modérée |
| level_2 | > 35°C | Canicule |

## 🌐 API Frontend

| Endpoint | Description |
|----------|-------------|
| `GET /` | Dashboard principal |
| `GET /api/realtime` | Données temps réel Kafka |
| `GET /api/hdfs` | Données historiques HDFS |
| `GET /api/latest` | Dernière mesure par ville |
| `GET /api/anomalies` | Anomalies détectées |
| `GET /api/stats` | Statistiques globales |
| `GET /api/status` | Statut Kafka/HDFS |
| `GET /api/reload` | Recharger données HDFS |

## 🛠️ Commandes Utiles

```bash
# Voir les logs Kafka
docker logs kafka

# Lister les topics Kafka
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Vérifier HDFS
docker exec namenode hdfs dfs -ls /weather/realtime

# Arrêter tous les conteneurs
docker-compose down

# Supprimer les volumes (reset complet)
docker-compose down -v
```

## 📈 Exercices Implémentés

| # | Exercice | Fichier |
|---|----------|---------|
| 1 | Setup Kafka + producteur simple | `producer/simple_producer.py` |
| 2 | Consumer Kafka Python | `kafka_consumer.py` |
| 3 | Producteur météo temps réel | `producer/current_weather.py` |
| 4 | Transformation Spark + alertes | `spark/spark_weather_stream.py` |
| 5 | Agrégats sliding windows | `spark/spark_weather_aggregates.py` |
| 6 | Géocodage ville/pays | `producer/current_weather_city.py` |
| 7 | Stockage HDFS partitionné | `hdfs/hdfs_weather_consumer.py` |
| 8 | Visualisation logs HDFS | `hdfs/weather_visualizer.py` |
| 9 | Historique 10 ans | `producer/weather_history_producer.py` |
| 10 | Détection records climatiques | `spark/climate_records_detector.py` |
| 11 | Profils saisonniers | `spark/seasonal_profile.py` |
| 12 | Enrichissement saisonnier | `spark/seasonal_enricher.py` |
| 13 | Détection anomalies | `spark/anomaly_detector.py` |
| 14 | Frontend Dashboard | `frontend/app.py` |

## 🔗 Liens Utiles

- [Apache Kafka](https://kafka.apache.org/)
- [Apache Spark](https://spark.apache.org/)
- [Open-Meteo API](https://open-meteo.com/)
- [HDFS Documentation](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsUserGuide.html)

## 📝 License

Projet éducatif - IPSSI 2026
