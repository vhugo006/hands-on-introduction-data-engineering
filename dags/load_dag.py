from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
        dag_id='load_dag',
        description='Load DAG',
        schedule_interval=None,
        start_date=datetime(2023, 12, 26),
        catchup=False
) as dag:
    load_task = BashOperator(
        task_id='load_task',
        bash_command='echo -e ".separator ","\n.import --skip 1 /home/victoralmeida/continuous-learning/'
                     'airflow/lab/etl-handson/manual-extract-data.csv top_level_domains" |'
                     'sqlite3 /home/victoralmeida/continuous-learning/airflow/airflow-load-db.db',
        dag=dag
    )
