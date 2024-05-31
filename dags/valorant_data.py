"""
DAG to extract, clean, and load Valorant player data into a PostgreSQL database.

This DAG performs the following steps:
1. Extracts player match data from the Valorant API.
2. Cleans the extracted data using a Python function.
3. Loads the cleaned data into a temporary PostgreSQL table.
4. Inserts the data from the temporary table into the main player table.
5. Drops the temporary table.
"""

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

from operators.json_to_postgres import JsonToPostgresOperator
from helpers.val_helpers import clean_valorant_data

# List of players to process
PLAYERS = [{'username': 'animewatcher', 'tag': 'bp3', 'region': 'na'},
           {'username': 'zomama3k', 'tag': 'mama', 'region': 'na'},
           {'username': 'puckgoat', 'tag': 'bardo', 'region': 'na'}]

with DAG(
        dag_id='Valorant_Data_Pipeline',
        concurrency=2,
        schedule_interval='0 */4 * * *',
        start_date=datetime(2023, 5, 30),
        catchup=False,
        max_active_runs=1,
        orientation='LR',
) as dag:
    # Define start and end points of the DAG
    start = EmptyOperator(
        task_id='start'
    )

    end = EmptyOperator(
        task_id='end'
    )

    finished_extract = EmptyOperator(
        task_id='finished_extract'
    )

    # Create the main players table in PostgreSQL
    create_table = PostgresOperator(
        task_id="create_valorant_players_table",
        sql='sql/valorant/create_player_table.sql',
        postgres_conn_id='de_database',
    )

    # Create a temporary table in PostgreSQL for intermediate data
    create_temp_table = PostgresOperator(
        task_id="create_temp_players_table",
        sql='sql/valorant/create_temp_player_table.sql',
        postgres_conn_id='de_database',
    )

    # Insert data from the temporary table into the main players table
    insert_player_data = PostgresOperator(
        task_id="insert_players_data",
        sql='sql/valorant/insert_player_data.sql',
        postgres_conn_id='de_database',
    )

    # Drop the temporary table after data insertion
    drop_temp_table = PostgresOperator(
        task_id="drop_temp_table",
        sql='sql/valorant/drop_temp_table.sql',
        postgres_conn_id='de_database',
    )

    # Loop through each player and define tasks for data extraction, cleaning, and insertion
    for player in PLAYERS:
        # Extract player data from the Valorant API
        extract_data = SimpleHttpOperator(
            task_id=f"extract_{player['username']}_data",
            method='GET',
            http_conn_id='valorant',
            endpoint=f"valorant/v1/lifetime/matches/{player['region']}/{player['username']}/{player['tag']}?page=1&size=10"
        )

        # Clean the extracted data
        clean_data = PythonOperator(
            task_id=f"clean_{player['username']}_data",
            python_callable=clean_valorant_data,
            op_kwargs={'task_id': f"extract_{player['username']}_data",
                       'player': player['username']}
        )

        # Insert cleaned data into the temporary PostgreSQL table
        insert_temp_data = JsonToPostgresOperator(
            task_id=f"temp_{player['username']}_data",
            conn_id='de_database',
            table='temp_valorant_player_data',
            xcom_task_id=f'clean_{player["username"]}_data',
        )

        # Define task dependencies
        start >> extract_data >> clean_data >> finished_extract
        finished_extract >> create_table >> create_temp_table >> insert_temp_data
        insert_temp_data >> insert_player_data >> drop_temp_table >> end
