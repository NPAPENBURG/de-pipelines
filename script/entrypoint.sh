#!/bin/bash
set -e

# Source environment variables
if [ -f ".env" ]; then
  source .env
fi

# Upgrade pip and install requirements if the requirements file exists
if [ -e "/opt/airflow/requirements.txt" ]; then
  python -m pip install --upgrade pip
  pip install --user -r /opt/airflow/requirements.txt
fi

# Initialize the database and create an admin user if it hasn't been done yet
if [ ! -f "/opt/airflow/airflow.db" ]; then
  airflow db init
  airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@example.com \
    --password admin
  touch /opt/airflow/airflow.db
fi

# Upgrade the database
airflow db upgrade

# Start the Airflow webserver
exec airflow webserver

