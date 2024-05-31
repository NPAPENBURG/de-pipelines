from airflow import DAG

from datetime import datetime

import yaml
from airflow import settings
from airflow.models import Connection
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 5, 30),
    'retries': 1,
}


def create_connections_from_yaml(yaml_file):
    with open(yaml_file, 'r') as f:
        connections_data = yaml.safe_load(f)

    for connection_name, connection_params in connections_data.items():
        conn = Connection(
            conn_id=connection_name,
            conn_type=connection_params['conn_type'],
            host=connection_params['host'],
            login=connection_params.get('login', None),
            password=connection_params.get('password', None),
            schema=connection_params.get('schema', None),
            port=connection_params.get('port', None),
            extra=connection_params.get('extra', None)
        )
        session = settings.Session()
        session.add(conn)
        session.commit()


with DAG(
        dag_id='import_connections',
        concurrency=1,
        schedule_interval=None,
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

    create_connections = PythonOperator(
        task_id='create_connections_task',
        python_callable=create_connections_from_yaml,
        op_kwargs={'yaml_file': 'connections.yaml'},
        dag=dag,
    )

    start >> create_connections >> end
