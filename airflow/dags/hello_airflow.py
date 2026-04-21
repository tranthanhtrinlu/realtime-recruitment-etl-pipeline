from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="hello_airflow",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test"],
) as dag:

    t1 = BashOperator(
        task_id="say_hello",
        bash_command="echo 'Hello from Airflow in WSL'",
    )