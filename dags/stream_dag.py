import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
import boto3
import json
import random
import time

first_names = [
    "Emma", "Liam", "Olivia", "Noah", "Ava", "Elijah", "Sophia", "Oliver", "Isabella", "Lucas",
    "Mia", "Alexander", "Charlotte", "James", "Amelia", "Benjamin", "Harper", "Henry", "Evelyn", "William",
    "Abigail", "Michael", "Emily", "Ethan", "Elizabeth", "Daniel", "Avery", "Matthew", "Sofia", "Jackson",
    "Ella", "Sebastian", "Madison", "David", "Scarlett", "Carter", "Victoria", "Jayden", "Aria", "Wyatt",
    "Grace", "John", "Chloe", "Owen", "Camila", "Dylan", "Penelope", "Luke", "Luna", "Gabriel",
    "Layla", "Anthony", "Zoe", "Isaac", "Mila", "Joseph", "Aubrey", "Levi", "Hannah", "Jack",
    "Stella", "Christopher", "Nora", "Joshua", "Addison", "Andrew", "Brooklyn", "Lincoln", "Ellie", "Mateo",
    "Paisley", "Ryan", "Audrey", "Jaxon", "Savannah", "Nathan", "Claire", "Aaron", "Skylar", "Isaiah",
    "Lucy", "Thomas", "Bella", "Charles", "Aurora", "Caleb", "Anna", "Josiah", "Leah", "Christian",
    "Maya", "Hunter", "Abby", "Eli", "Julia", "Ezra", "Makayla", "Jonathan", "Elena", "Connor"
]
top_10_anime = [
    "Attack on Titan",
    "Naruto (and Naruto Shippuden)",
    "One Piece",
    "Death Note",
    "Fullmetal Alchemist: Brotherhood",
    "My Hero Academia",
    "Dragon Ball Z",
    "Sword Art Online",
    "Demon Slayer: Kimetsu no Yaiba",
    "Steins;Gate"
]


def generate_random_data(names, anime):
    aws_hook = AwsBaseHook('aws')
    credentials = aws_hook.get_credentials()
    kinesis_client = boto3.client(
        "kinesis",
        aws_access_key_id=credentials.access_key,
        aws_secret_access_key=credentials.secret_key,
        region_name='us-east-1',
    )

    stream_name = "fav-anime"

    for i in range(50):
        date_string = datetime.now().strftime('%Y-%m-%d')
        message_data = {"user": random.choice(names), "vote": random.choice(anime), "date": date_string}
        encoded_data = json.dumps(message_data)
        response2 = kinesis_client.put_record(
            StreamName=stream_name, Data=encoded_data, PartitionKey="date"
        )
        time.sleep(1)
        logging.info(response2)


with DAG(
        dag_id='anime_stream_data',
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

    generate_data_task = PythonOperator(
        task_id='generate_random_data',
        python_callable=generate_random_data,
        op_kwargs={'names': first_names, 'anime': top_10_anime}
    )

    start >> generate_data_task >> end
