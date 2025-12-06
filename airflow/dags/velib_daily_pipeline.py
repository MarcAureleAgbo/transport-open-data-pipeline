from datetime import datetime, timedelta
import logging

import requests
import psycopg2

from airflow import DAG
from airflow.decorators import task


# -------------------------
# Config générale du DAG
# -------------------------

default_args = {
    "owner": "maa",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Endpoints Open Data Vélib (format exports/json)
STATIONS_URL = (
    "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/"
    "velib-emplacement-des-stations/exports/json?lang=fr&timezone=Europe%2FParis"
)

STATUS_URL = (
    "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/"
    "velib-disponibilite-en-temps-reel/exports/json?lang=fr&timezone=Europe%2FParis"
)

# Config Postgres (dans le docker-compose : service "postgres")
PG_HOST = "postgres"
PG_PORT = 5432
PG_DB = "airflow"
PG_USER = "airflow"
PG_PASSWORD = "airflow"


with DAG(
    dag_id="velib_daily_pipeline",
    description="Pipeline quotidien pour récupérer et stocker les données Vélib (stations + disponibilité)",
    default_args=default_args,
    schedule_interval="@daily",   # tu pourras passer à de l’horaire plus tard
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["velib", "transport", "open-data"],
):

    # -------------------------
    # 1. EXTRACT : télécharger les JSON exports
    # -------------------------
    @task
    def extract_velib_data():
        logging.info("Téléchargement des données Vélib via exports/json...")

        stations_resp = requests.get(STATIONS_URL, timeout=60)
        status_resp = requests.get(STATUS_URL, timeout=60)

        stations_resp.raise_for_status()
        status_resp.raise_for_status()

        stations = stations_resp.json()  # liste de dicts
        status = status_resp.json()      # liste de dicts

        logging.info("Stations reçues : %d", len(stations))
        logging.info("Status reçus   : %d", len(status))

        return {"stations": stations, "status": status}

    # -------------------------
    # 2. TRANSFORM : joindre stations + status, préparer les lignes
    # -------------------------
    @task
    def transform_velib_data(raw_data: dict, ds=None):
        # ds = date logique Airflow ('YYYY-MM-DD'). Si jamais None, on prend la date du jour.
        execution_date = ds or datetime.today().date().isoformat()

        stations = raw_data["stations"]
        status = raw_data["status"]

        # stations = [{ "stationcode": "...", "name": "...", ... }]
        stations_by_code = {
            s.get("stationcode"): s
            for s in stations
            if s.get("stationcode")
        }

        rows = []
        for st in status:
            code = st.get("stationcode")
            if not code:
                continue

            station_info = stations_by_code.get(code, {})

            row = {
                "snapshot_date": execution_date,
                "stationcode": code,
                "name": station_info.get("name"),
                "capacity": station_info.get("capacity"),
                "lon": (station_info.get("coordonnees_geo") or {}).get("lon"),
                "lat": (station_info.get("coordonnees_geo") or {}).get("lat"),
                "is_installed": st.get("is_installed"),
                "is_renting": st.get("is_renting"),
                "is_returning": st.get("is_returning"),
                "num_bikes_available": st.get("numbikesavailable"),
                "num_docks_available": st.get("numdocksavailable"),
                "mechanical": st.get("mechanical"),
                "ebike": st.get("ebike"),
            }

            rows.append(row)

        logging.info("Nombre de lignes transformées : %d", len(rows))
        if not rows:
            logging.warning("Aucune ligne transformée – vérifier le format JSON.")
        return rows

    # -------------------------
    # 3. LOAD : créer la table si besoin + insérer les données
    # -------------------------
    @task
    def load_velib_data(rows: list):
        logging.info("Connexion à Postgres pour insérer les données Vélib...")

        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            dbname=PG_DB,
            user=PG_USER,
            password=PG_PASSWORD,
        )
        conn.autocommit = False

        create_table_sql = """
        CREATE TABLE IF NOT EXISTS velib_station_status (
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
        """

        insert_sql = """
        INSERT INTO velib_station_status (
            snapshot_date,
            stationcode,
            name,
            capacity,
            lon,
            lat,
            is_installed,
            is_renting,
            is_returning,
            num_bikes_available,
            num_docks_available,
            mechanical,
            ebike
        )
        VALUES (
            %(snapshot_date)s,
            %(stationcode)s,
            %(name)s,
            %(capacity)s,
            %(lon)s,
            %(lat)s,
            %(is_installed)s,
            %(is_renting)s,
            %(is_returning)s,
            %(num_bikes_available)s,
            %(num_docks_available)s,
            %(mechanical)s,
            %(ebike)s
        );
        """

        try:
            with conn.cursor() as cur:
                cur.execute(create_table_sql)
                logging.info("Table velib_station_status OK (créée ou déjà existante).")

                if not rows:
                    logging.warning("0 lignes à insérer dans velib_station_status.")
                    conn.commit()
                    return

                for row in rows:
                    cur.execute(insert_sql, row)

            conn.commit()
            logging.info("Insertion terminée avec succès (%d lignes).", len(rows))
        except Exception as e:
            conn.rollback()
            logging.error("Erreur lors du chargement des données Vélib : %s", e)
            raise
        finally:
            conn.close()

    # Définition du flow
    raw = extract_velib_data()
    transformed = transform_velib_data(raw)
    load_velib_data(transformed)
