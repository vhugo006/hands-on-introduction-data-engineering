"""Two Task DAG"""
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
        dag_id='two_task_dag',
        description='A Two task Airflow DAG',
        schedule_interval=None,
        default_args=default_args
) as dag:
    task0 = BashOperator(
        task_id='task0',
        bash_command='echo "task0 run"'
    )

    task1 = BashOperator(
        task_id='task1',
        bash_command='echo "Sleeping" && sleep 5s && echo "task1 run"'
    )

    task0 >> task1
