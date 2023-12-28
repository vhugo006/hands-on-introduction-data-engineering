"""One Task DAG"""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'Victor',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'catchup': False,
    'start_date': datetime(2023, 12, 23)
}

with DAG(
        dag_id='my_first_dag',
        description='My first DAG in Airflow',
        schedule_interval=None,
        default_args=default_args
) as dag:
    task1 = BashOperator(
        task_id='my_first_task',
        bash_command='echo "hello to my first DAG" > ~/continuous-learning/airflow/createthisfile.txt'
    )
