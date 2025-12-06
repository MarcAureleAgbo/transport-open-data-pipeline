# 🚲 Projet Data Engineering — Pipeline Vélib (Airflow + Postgres)

Ce projet met en place un pipeline ETL robuste permettant de collecter automatiquement 
les données publiques des stations Vélib (localisation + disponibilité temps réel), 
de les transformer et de les stocker dans une base Postgres pour permettre des analyses 
de mobilité urbaine.

Ce pipeline est orchestré avec **Apache Airflow** et s’exécute automatiquement 
chaque jour (ou manuellement à la demande).

---

## 📌 Objectifs du projet

- Collecter les données Open Data Paris concernant :
  - l’emplacement et les caractéristiques des stations Vélib
  - la disponibilité en temps réel (vélos mécaniques, électriques, docks)
- Nettoyer et fusionner ces données pour obtenir un snapshot complet par station.
- Stocker ce snapshot dans une base **Postgres**.
- Automatiser l’exécution quotidienne avec Airflow.
- Produire une base prête pour :
  - analyses statistiques
  - visualisations
  - machine learning
  - études de mobilité urbaine

---

## 🏗️ Architecture générale

Le projet repose sur :

🎛️ **Apache Airflow** — orchestration  
🐘 **PostgreSQL** — stockage des données  
🐳 **Docker / Docker Compose** — déploiement local  
📡 **API Open Data Paris** — source des données Vélib  
🐍 **Python 3.12** — transformations

---

## 🔄 Pipeline ETL (DAG Airflow)

Le DAG `velib_daily_pipeline` suit la structure ci-dessous :

extract_velib_data
↓
transform_velib_data
↓
load_velib_data


### 1️⃣ EXTRACT — `extract_velib_data`
Récupération des données brutes depuis les endpoints Open Data Paris (format JSON) :

- `velib-emplacement-des-stations`
- `velib-disponibilite-en-temps-reel`

La tâche :
- appelle les API
- valide les statuts HTTP
- charge les données JSON
- retourne un dictionnaire `{stations, status}`

### 2️⃣ TRANSFORM — `transform_velib_data`
Nettoyage et fusion :

- association station ↔ disponibilité via `stationcode`
- extraction des champs utiles
- formatage d’un enregistrement par station
- ajout d’un `snapshot_date` (date d'exécution)

Retourne une liste propre, directement insérable.

### 3️⃣ LOAD — `load_velib_data`
Chargement dans Postgres :

- création automatique de la table `velib_station_status` si nécessaire
- insertion ligne par ligne
- gestion des transactions (commit / rollback)
- fermeture propre des connexions

---

## 🗄️ Structure de la table Postgres

```sql
CREATE TABLE velib_station_status (
    id SERIAL PRIMARY KEY,
    snapshot_date DATE NOT NULL,
    stationcode VARCHAR(20),
    name TEXT,
    capacity INTEGER,
    lon DOUBLE PRECISION,
    lat DOUBLE PRECISION,
    is_installed TEXT,
    is_renting TEXT,
    is_returning TEXT,
    num_bikes_available INTEGER,
    num_docks_available INTEGER,
    mechanical INTEGER,
    ebike INTEGER
);

🐳 Démarrer le projet avec Docker
1. Lancer Airflow et Postgres
docker compose up -d


Airflow sera accessible sur :

👉 http://localhost:8080

user: airflow
password: airflow

2. Vérifier la connexion à Postgres
docker exec -it transport_postgres psql -U airflow -d airflow

▶️ Exécuter le pipeline

Depuis linterface Airflow :

Aller dans DAGs

Activer velib_daily_pipeline

Cliquer sur Trigger DAG

Les données apparaîtront dans :

SELECT COUNT(*) FROM velib_station_status;
SELECT * FROM velib_station_status LIMIT 10;

📁 Arborescence du projet
transport-open-data-pipeline/
│
├── dags/
│   └── velib_daily_pipeline.py
│
├── docker-compose.yml
├── README.md
├── .gitignore
└── requirements.txt (optionnel)
