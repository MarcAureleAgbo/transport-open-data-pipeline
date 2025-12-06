from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator

default_args = {
    "owner": "maa",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="check_env",
    default_args=default_args,
    description="DAG de test pour vérifier l'environnement",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    start >> end
