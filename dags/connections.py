"""
DAG to Create Airflow Connections from a YAML File

This DAG reads connection details from a `connections.yaml` file (not included due to containing sensitive information)
and adds them to the Airflow database as connections.
"""
import os

from airflow import DAG
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from helpers.connection_helpers import create_connections_from_yaml

dag_directory = os.path.dirname(os.path.abspath(__file__))
parent_directory = os.path.abspath(os.path.join(dag_directory, os.pardir))
yaml_file_path = os.path.join(parent_directory, 'connections.yaml')

with DAG(
        dag_id='import_connections',
        concurrency=1,
        schedule_interval=None,
        start_date=datetime(2023, 5, 30),
        catchup=True,
        max_active_runs=1,
) as dag:
    start = EmptyOperator(
        task_id='start'
    )

    end = EmptyOperator(
        task_id='end'
    )

    create_connections = PythonOperator(
        task_id='create_connections_task',
        python_callable=create_connections_from_yaml,
        op_kwargs={'yaml_file': yaml_file_path},
        dag=dag,
    )

    start >> create_connections >> end
