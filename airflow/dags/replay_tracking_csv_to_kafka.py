from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="replay_tracking_csv_to_kafka",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["kafka", "producer", "demo"],
) as dag:
    replay = BashOperator(
        task_id="replay_tracking_csv",
        bash_command="""
        cd /mnt/e/Project_ETL/ETL_Docker_cach3 && python3 replay_tracking_to_kafka.py
        """,
    )