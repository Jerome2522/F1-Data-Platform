from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from docker.types import Mount

import os

# Use HOST_DATA_PATH from environment variable (set in docker-compose)
# This allows portability across environments
HOST_DATA_PATH = os.environ.get("HOST_DATA_PATH", "/data")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
    'catchup': False
}

with DAG(
    'f1_pipeline_dag',
    default_args=default_args,
    schedule_interval=None,
    description='A simple pipeline to ingest and process F1 data',
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    ingest_task = DockerOperator(
        task_id='ingest_f1_data',
        image='f1-data-platform-ingestion:latest',
        container_name='f1_ingestion_job',
        api_version='auto',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        network_mode='f1-network',
        mounts=[
            Mount(source=HOST_DATA_PATH, target='/data', type='bind'),
        ],
        command='python ingest_f1_data.py'
    )

    process_task = DockerOperator(
        task_id='process_f1_data',
        image='f1-data-platform-spark:latest',
        container_name='f1_spark_job',
        api_version='auto',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        network_mode='f1-network',
        mounts=[
            Mount(source=HOST_DATA_PATH, target='/data', type='bind'),
        ],
        command='python process_f1_data.py'
    )

    start >> ingest_task >> process_task >> end
