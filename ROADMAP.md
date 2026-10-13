# 🗺️ Smart City Traffic Roadmap : Du Prototype au Système Réactif

Ce document trace les lignes directrices pour faire évoluer notre architecture analytique (**YOLOv8 ➔ Kafka ➔ Spark ➔ Parquet ➔ Streamlit**) vers un système Smart City temps réel, bouclé, prédictif et interconnecté.

---

## 🏗️ Architecture Cible Globale

L'objectif principal est de briser le flux unidirectionnel actuel pour introduire une **boucle de rétroaction (Feedback Loop)** et un **moteur prédictif** au cœur du pipeline.

```text
[Génération] Flux Vidéo Réel OR Simulateur Urbain (SUMO)
   │
   ▼
[Edge / Ingestion] YOLOv8 ➔ `producer_yolo.py` ➔ Kafka (`urban.traffic`)
   │
   ▼
[Traitement] Spark Structured Streaming 
   │  ├─ Bronze : Ingestion brute des métriques YOLO
   │  ├─ Silver : Nettoyage, structuration & Inférence IA Prédictive (T+15min)
   │  └─ Gold   : Agrégations spatiales (Graphes) & Déclenchement d'alertes
   │
   ├──► [Persistance] Fichiers Parquet ➔ Stockage & Entraînement continu des modèles
   │
   └──► [Actionneur] Émission d'alertes/ordres ➔ Kafka (`urban.control`)
            │
            ▼
   [Régulation] Ajustement des feux (SUMO TraCI) OR Panneaux à Message Variable (PMV)
```

---

## 🛠️ Axes d'Amélioration & Chantiers Techniques

### 🛑 Constat Actuel
Le prototype actuel est **descriptif** : il capte parfaitement le passé et le présent, mais n'a aucun impact sur l'environnement. Les chantiers ci-dessous visent à rendre le système **prescriptif et réactif**.

### 🔹 Chantier 1 : Passer de la Vidéo Figée à la Simulation Dynamique (SUMO)
* **Objectif :** Remplacer le fichier MP4 statique par un environnement urbain virtuel interactif et réactif.
* **Tâches clés :**
  * Modéliser un réseau routier réaliste (intersections, feux tricolores, voies de circulation) avec **SUMO (Simulation of Urban MObility)**.
  * Utiliser la bibliothèque Python **TraCI (Traffic Control Interface)** pour piloter la simulation et simuler des caméras virtuelles.
  * Connecter ce flux de données synthétiques dynamique à notre script `producer_yolo.py`.

### 🔹 Chantier 2 : Implémenter la Boucle de Rétroaction (Feedback Loop)
* **Objectif :** Permettre à l'architecture de données de modifier l'état de la ville en fonction du niveau de congestion.
* **Tâches clés :**
  * Créer un nouveau topic Kafka dédié aux commandes : `urban.control`.
  * Configurer **Spark Structured Streaming** pour publier un message d'alerte (JSON) sur ce topic dès que la densité de véhicules de la couche *Gold* dépasse un seuil critique pendant plus de 3 minutes.
  * Développer un script `consumer_control.py` qui consomme ce topic et ajuste dynamiquement le rythme des feux de signalisation dans SUMO (via TraCI) pour résorber le bouchon.

### 🔹 Chantier 3 : Intégrer l'Intelligence Artificielle Prédictive
* **Objectif :** Anticiper les vagues d'embouteillages 15 à 30 minutes avant qu'elles ne se forment à l'écran.
* **Tâches clés :**
  * Exploiter l'historique des fichiers **Parquet (Gold)** accumulés pour entraîner un modèle de série temporelle (ex: *XGBoost*, *LSTM* ou *Prophet*).
  * Exporter et sérialiser le modèle entraîné (format *ONNX* ou via *MLflow*).
  * Charger ce modèle directement dans le flux **Spark Streaming (couche Silver)** pour enrichir chaque ligne de données en temps réel avec une métrique prédictive : `predicted_congestion_t_plus_15`.

### 🔹 Chantier 4 : Évoluer vers une Analyse Spatiale (Structure en Graphe)
* **Objectif :** Prendre en compte la topologie de la ville (comprendre l'effet domino d'un carrefour bloqué sur les rues adjacentes).
* **Tâches clés :**
  * Modéliser le réseau de caméras YOLO sous forme de graphe mathématique (Nœuds = Intersections, Arêtes = Rues).
  * Implémenter des algorithmes de calcul de coût (ex: *Dijkstra dynamique*) où le poids des arêtes varie selon le volume de véhicules mesuré en temps réel par YOLO.
  * Générer des recommandations d'itinéraires de délestage alternatifs à envoyer aux usagers ou au dashboard.

### 🔹 Chantier 5 : Refonte du Dashboard Streamlit (Pilotage Actif)
* **Objectif :** Transformer l'interface de visualisation passive en un véritable centre de supervision et de contrôle.
* **Tâches clés :**
  * Intégrer une carte interactive (ex: *Folium* ou *pydeck*) affichant les congestions actuelles combinées aux prédictions à court terme.
  * Ajouter un commutateur "Mode Automatique / Mode Manuel".
  * Implémenter un bouton d'action d'urgence permettant à l'opérateur humain de forcer le passage au vert d'un axe routier en publiant directement un message personnalisé dans le topic `urban.control`.

---

## 📅 Jalons de Développement Proposés

- [ ] **Étape 1 :** Création du topic `urban.control` et écriture du script d'actionnement de base (Looping).
- [ ] **Étape 2 :** Prise en main de SUMO/TraCI et remplacement du fichier MP4 par la simulation urbaine.
- [ ] **Étape 3 :** Phase Data Science : Entraînement et intégration du modèle prédictif dans le flux Spark.
- [ ] **Étape 4 :** Refonte de l'interface Streamlit en mode "Control Room".
