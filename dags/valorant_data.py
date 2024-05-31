"""DAG CREATED TO GATHER MULTIPLE DIFFERENT TYPES OF VALORANT DATA FOR EACH PLAYER"""
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from datetime import datetime


PLAYERS = [{'username': 'AnimeWatcher', 'tag': 'BP3', 'region': 'NA'}]

with DAG(
        dag_id='Valorant_Data_Pipeline',
        concurrency=2,
        schedule_interval='0 */4 * * *',
        start_date=datetime(2023, 5, 30),
        catchup=False,
        max_active_runs=1,
        orientation='LR',
) as dag:

    for player in PLAYERS:
        extract_data = SimpleHttpOperator(
            task_id=f'extra_{player}_data',
            method='GET',
            http_conn_id='http_default',
            endpoint='api/v1/resource',
            headers={'Content-Type': 'application/json'},
            xcom_push=True
        )
