"""DAG CREATED TO GATHER MULTIPLE DIFFERENT TYPES OF VALORANT DATA FOR EACH PLAYER"""
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.empty import EmptyOperator
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

    start = EmptyOperator(
        task_id='start'
    )

    end = EmptyOperator(
        task_id='end'
    )

    for player in PLAYERS:

        conn_id = 'valorant'
        http_conn = BaseHook.get_connection(conn_id)
        api_key = http_conn.extra_dejson.get('Authorization')

        extract_data = SimpleHttpOperator(
            task_id=f"extra_{player['username']}_data",
            method='GET',
            http_conn_id='valorant',
            endpoint=f"valorant/v1/lifetime/matches/{player['region']}/{player['username']}/{player['tag']}?page=1&size=10",
            headers={'Authorization': api_key}
        )

        start >> extract_data >> end
