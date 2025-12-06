# 🚲 Pipeline Vélib — Projet Data Engineering (Airflow + Postgres)

Ce projet met en place un pipeline ETL complet pour collecter automatiquement
les données publiques Vélib (stations + disponibilité temps réel), les transformer
et les stocker dans une base PostgreSQL.  
L’orchestration est gérée par **Apache Airflow** dans un environnement Docker.

---

## 🎯 Objectifs

- Récupérer les données open data Vélib depuis l’API de la Ville de Paris.
- Combiner :
  - les **métadonnées de station** (emplacement, capacité…)
  - la **disponibilité en temps réel** (vélos, bornes libres, mécaniques/électriques).
- Construire un **snapshot quotidien** de l’état du réseau Vélib.
- Sauvegarder ce snapshot dans une table PostgreSQL pour :
  - analyses de mobilité
  - visualisations / dashboards
  - futurs modèles prédictifs.

---

## 🏗️ Architecture

**Technologies principales :**

- Apache Airflow (orchestration)
- Python 3.12
- PostgreSQL
- Docker & Docker Compose
- API Open Data Paris

**Schéma global :**

```text
Open Data API → Airflow DAG → Postgres → (SQL / BI / ML)
🔄 Le DAG velib_daily_pipeline
Le pipeline comporte 3 tâches :

text
Copy code
extract_velib_data → transform_velib_data → load_velib_data
1. extract_velib_data (EXTRACT)
Appelle les endpoints JSON d’export :

velib-emplacement-des-stations

velib-disponibilite-en-temps-reel

Utilise requests.get(...) avec gestion des erreurs HTTP.

Retourne les données brutes dans un dictionnaire :

python
Copy code
{
    "stations": [...],
    "status": [...]
}
2. transform_velib_data (TRANSFORM)
Joint les deux jeux de données via stationcode.

Extrait les champs pertinents :

position (lat/lon)

capacité

nombre de vélos disponibles

mécaniques / électriques

statut de la station.

Ajoute un snapshot_date (date logique Airflow).

Retourne une liste de lignes prêtes à être insérées en base.

3. load_velib_data (LOAD)
Se connecte à PostgreSQL.

Crée la table cible si elle n’existe pas :

velib_station_status

Insère les lignes transformées dans la table.

Gère la transaction (commit / rollback) et les erreurs éventuelles.

🗄️ Schéma de la table PostgreSQL
sql
Copy code
CREATE TABLE velib_station_status (
    id SERIAL PRIMARY KEY,
    snapshot_date DATE NOT NULL,
    stationcode VARCHAR(20) NOT NULL,
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
🐳 Lancer le projet en local
1. Démarrer l’environnement Docker
bash
Copy code
docker compose up -d
2. Accéder à l’interface Airflow
URL : http://localhost:8080

Utilisateur : airflow (à adapter selon la conf)

Mot de passe : airflow

3. Exécuter le DAG
Activer velib_daily_pipeline dans la liste des DAGs.

Cliquer sur Trigger DAG pour lancer un run manuel.

Vérifier que les 3 tâches passent au vert.

4. Vérifier les données dans Postgres
bash
Copy code
docker exec -it transport_postgres \
  psql -U airflow -d airflow -c "SELECT COUNT(*) FROM velib_station_status;"

docker exec -it transport_postgres \
  psql -U airflow -d airflow -c "SELECT * FROM velib_station_status LIMIT 5;"
📊 Exemples d’usages possibles
Taux d’occupation moyen des stations par arrondissement.

Identification des stations saturées ou fréquemment vides.

Profil temporel d’utilisation (heures de pointe, saisonnalité).

Préparation d’un dataset pour un modèle prédictif de disponibilité.

🚀 Pistes d’amélioration
Passage à une fréquence horaire ou infra-horaire.

Ajout d’un snapshot_timestamp (datetime complet).

Création de vues matérialisées pour l’analyse.

Intégration à un outil de BI (Metabase, Grafana).

Export vers Parquet / Data Lake.

👤 Auteur
MAA
Projet personnel Data Engineering — 2025

sql
Copy code

👉 Une fois collé dans `README.md` :  
```bash
git add README.md
git commit -m "Improve README with detailed project documentation"
git push
