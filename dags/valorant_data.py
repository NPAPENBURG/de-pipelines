"""DAG CREATED TO GATHER MULTIPLE DIFFERENT TYPES OF VALORANT DATA FOR EACH PLAYER"""
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from datetime import datetime

from helpers.val_helpers import clean_valorant_data

PLAYERS = [{'username': 'animewatcher', 'tag': 'bp3', 'region': 'na'},
           {'username': 'zomama3k', 'tag': 'mama', 'region': 'na'}]

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
        extract_data = SimpleHttpOperator(
            task_id=f"extract_{player['username']}_data",
            method='GET',
            http_conn_id='valorant',
            endpoint=f"valorant/v1/lifetime/matches/{player['region']}/{player['username']}/{player['tag']}?page=1&size=10"
        )

        clean_data = PythonOperator(
            task_id=f"clean_{player['username']}_data",
            python_callable=clean_valorant_data,
            op_kwargs={'task_id': f"extract_{player['username']}_data"}
        )

        start >> extract_data >> clean_data >> end
