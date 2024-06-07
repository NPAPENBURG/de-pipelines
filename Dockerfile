# Base image for Airflow
FROM apache/airflow:2.9.1

# Copy the requirements.txt file to the image
COPY requirements.txt /requirements.txt

# Install the requirements specified in requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
