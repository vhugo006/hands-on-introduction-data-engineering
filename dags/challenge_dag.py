from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
        dag_id='basic_etl_dag',
        description='Basic ETL DAG',
        start_date=datetime(2023, 12, 23),
        catchup=False
) as dag:
    extract_task = BashOperator(
        task_id='extract_task',
        bash_command='wget -c https://datahub.io/core/s-and-p-500-companies/r/contituents.csv '
                     '-O /home/victoralmeida/continuous-learning/airflow/lab/challenge'
                     '/s-and-p-extract-data.csv')
