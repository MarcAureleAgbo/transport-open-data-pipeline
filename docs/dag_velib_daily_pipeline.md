# DAG `velib_daily_pipeline` — Documentation technique

## 1. Rôle du DAG

Ce DAG orchestre un pipeline ETL quotidien pour collecter les données Vélib,
les transformer puis les charger dans PostgreSQL.  
Il s'exécute actuellement avec une planification `@daily`.

---

## 2. Paramètres généraux

- `dag_id` : `velib_daily_pipeline`
- `schedule_interval` : `@daily`
- `start_date` : `2024-01-01`
- `catchup` : `False`
- `max_active_runs` : `1`
- `owner` par défaut : `maa`
- `retries` : `1`
- `retry_delay` : 5 minutes

---

## 3. Tâches

### 3.1 `extract_velib_data`

**Type** : `@task` (PythonDecoratedOperator)  
**Responsabilité** :  
- Appeler les endpoints JSON d’export Vélib.
- Valider les réponses HTTP.
- Retourner les données brutes sous forme de dictionnaire.

**Entrées** : aucune (tâche de début de pipeline)  
**Sorties** : `raw_data = {"stations": [...], "status": [...]}` (XCom)

---

### 3.2 `transform_velib_data`

**Type** : `@task`  
**Responsabilité** :
- Lire `raw_data` depuis l’XCom.
- Indexer les stations par `stationcode`.
- Joindre stations + statuts.
- Construire une liste de dictionnaires structurés, un par station.

**Champs produits** (par ligne) :

- `snapshot_date`
- `stationcode`
- `name`
- `capacity`
- `lon`
- `lat`
- `is_installed`
- `is_renting`
- `is_returning`
- `num_bikes_available`
- `num_docks_available`
- `mechanical`
- `ebike`

**Entrées** : `raw_data`  
**Sorties** : `rows` (liste de dictionnaires)

---

### 3.3 `load_velib_data`

**Type** : `@task`  
**Responsabilité** :
- Se connecter à PostgreSQL.
- Créer la table `velib_station_status` si elle n’existe pas.
- Insérer chaque ligne de `rows` avec des requêtes `INSERT`.
- Gérer la transaction et les erreurs.

**Entrées** : `rows` (liste)  
**Sorties** : aucune (effet de bord en base)

---

## 4. Dépendances

```text
extract_velib_data → transform_velib_data → load_velib_data
