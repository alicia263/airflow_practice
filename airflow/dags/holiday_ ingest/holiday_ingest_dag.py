
from airflow import DAG
from airflow.models import Variable
from airflow.settings import AIRFLOW_HOME
from airflow.operators.python_operator import PythonOperator

import os
from datetime import datetime, timedelta

from operators.holiday_api_plugin import HolidayAPIIngestOperator

# Define default arguments
default_args = {
    'owner': 'Ajay',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 22),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


# Instantiate a DAG object
with DAG(
    dag_id='holiday_ingest',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
) as dag:

    filename_json = f"holiday_brazil.json"
    task = HolidayAPIIngestOperator(
        task_id="holiday_api_ingestion",
        filename=filename_json,
        secret_key="e7d52583-2982-491b-a60c-4702b6518ec9",
        country="BR",
        year=2024
    )


task
