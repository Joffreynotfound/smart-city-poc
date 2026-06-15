# Smart City — POC Monitoring du Trafic Urbain

Pipeline de traitement de données temps réel pour la détection et l'analyse du trafic urbain, basé sur une architecture **Medallion (Bronze → Silver → Gold)**.

```
Vidéo MP4
   │
   ▼
[Edge] YOLOv8 (détection véhicules)
   │  producer_yolo.py
   ▼
[Broker] Apache Kafka
   │  topic: urban.traffic
   ▼
[Traitement] Apache Spark Structured Streaming
   │  consumer_spark.py
   │  Bronze → Silver → Gold
   ▼
[Persistance] Fichiers Parquet
   │  data/gold/traffic_aggregated/
   ▼
[Visualisation] Dashboard Streamlit
   dashboard/app.py
```

---

## Prérequis

| Outil | Version testée |
|---|---|
| Python | 3.12 |
| Docker Desktop | ≥ 4.x |
| Java (JDK) | 11 ou 17 (requis par Spark) |

Vérification rapide :
```bash
python3 --version
docker --version
java -version
```

---

## Installation

```bash
# 1. Cloner le repo
git clone <url-du-repo>
cd smart-city-poc

# 2. Créer et activer l'environnement virtuel
python3 -m venv venv
source venv/bin/activate

# 3. Installer les dépendances
pip install kafka-python ultralytics opencv-python pyspark streamlit pyarrow
```

---

## Lancement — dans l'ordre exact

### Étape 1 — Démarrer l'infrastructure Kafka

```bash
docker compose -f infrastructure/docker-compose.yml up -d
```

Vérification : ouvrir [http://localhost:9000](http://localhost:9000) (Kafdrop) — le broker doit apparaître.

---

### Étape 2 — Démarrer le consumer Spark

```bash
cd spark_processing
python consumer_spark.py
```

Spark se connecte à Kafka et attend les messages. Les résultats agrégés sont écrits dans `spark_processing/data/gold/traffic_aggregated/` au format Parquet, partitionnés par `date` et `camera_id`.

---

### Étape 3 — Démarrer le(s) producer(s) YOLOv8

```bash
cd edge_ingestion

# Caméra 1 (terminal séparé)
python producer_yolo.py --camera-id CAM_ESIEE_01 --video traffic_highway_3.mp4

# Caméra 2 (terminal séparé) — optionnel
python producer_yolo.py --camera-id CAM_ESIEE_02 --video traffic_highway_flowing.mp4

# Caméra 3 (terminal séparé) — optionnel
python producer_yolo.py --camera-id CAM_ESIEE_03 --video traffic_highway.mp4
```

Une fenêtre OpenCV s'ouvre pour chaque caméra avec les détections annotées en temps réel. Appuyer sur `q` pour arrêter une instance.

---

### Étape 4 — Démarrer le dashboard Streamlit

```bash
cd smart-city-poc
venv/bin/streamlit run dashboard/app.py
```

Ouvrir [http://localhost:8501](http://localhost:8501).

---

## Structure du projet

```
smart-city-poc/
├── edge_ingestion/
│   ├── producer_yolo.py        # Détection YOLOv8 + envoi Kafka
│   ├── yolov8n.pt              # Modèle pré-entraîné (COCO)
│   └── *.mp4                   # Vidéos sources
│
├── spark_processing/
│   ├── consumer_spark.py       # Pipeline Spark Bronze → Silver → Gold
│   └── data/
│       ├── gold/               # Parquet partitionnés (générés à l'exécution)
│       └── checkpoints/        # Checkpoints Spark (générés à l'exécution)
│
├── dashboard/
│   └── app.py                  # Dashboard Streamlit (lecture Parquet)
│
└── infrastructure/
    └── docker-compose.yml      # Kafka (KRaft) + Kafdrop
```

---

## Architecture Medallion

| Couche | Description | Format |
|---|---|---|
| **Bronze** | Messages bruts lus depuis Kafka | Binaire (Kafka offset) |
| **Silver** | JSON parsé, typé, horodaté | DataFrame Spark en mémoire |
| **Gold** | Agrégation par fenêtre de 1 min, statut de congestion | Parquet partitionné sur disque |

### Règles de congestion

| Statut | Condition |
|---|---|
| 🟢 FLUIDE | Moyenne ≤ 8 véhicules / fenêtre |
| 🟠 EMBOUTEILLAGE | Moyenne entre 8 et 15 véhicules / fenêtre |
| 🔴 CRITIQUE | Moyenne > 15 véhicules / fenêtre |

---

## Arrêt propre

```bash
# Arrêter les producers : appuyer sur 'q' dans chaque fenêtre OpenCV

# Arrêter Spark : Ctrl+C dans le terminal consumer

# Arrêter le dashboard : Ctrl+C dans le terminal Streamlit

# Arrêter Kafka
docker compose -f infrastructure/docker-compose.yml down
```
