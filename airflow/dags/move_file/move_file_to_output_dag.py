from airflow import DAG
from airflow.settings import AIRFLOW_HOME
from airflow.operators.bash import BashOperator
from airflow.sensors.weekday import DayOfWeekSensor
from airflow.utils.weekday import WeekDay
from datetime import datetime, timedelta
from slack_notifications import send_failure_alert  # Updated import

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 11),
    'email': ['junioralexio607@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': send_failure_alert
}

# Instantiate a DAG object
with DAG(
    dag_id='sensors_move_file1',
    default_args=default_args,
    schedule_interval="@once",
) as dag:
    
    move_file_on_saturdays = DayOfWeekSensor(
        task_id="move_file_on_fridays",
        timeout=3,
        soft_fail=True,
        week_day=WeekDay.FRIDAY
    )
    
    move_file_task = BashOperator(
        task_id="move_file_task",
        bash_command="mv $AIRFLOW_HOME/files_to_test/sensors_files/*.json $AIRFLOW_HOME/files_to_test/output_files/",
    )

move_file_on_saturdays.set_downstream(move_file_task)