from datetime import datetime
import subprocess

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator


def decide_start_or_skip():
    result = subprocess.run(
        [
            "docker",
            "exec",
            "spark-master",
            "bash",
            "-lc",
            "pgrep -f kafka_to_cassandra_mysql_stream_upsert.py >/dev/null",
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    if result.returncode == 0:
        return "stream_already_running"
    return "start_stream_job"


with DAG(
    dag_id="stream_guard_kafka_spark",
    start_date=datetime(2026, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    tags=["kafka", "spark", "monitoring"],
) as dag:
    decide = BranchPythonOperator(
        task_id="decide_start_or_skip",
        python_callable=decide_start_or_skip,
    )

    stream_already_running = EmptyOperator(
        task_id="stream_already_running"
    )

    start_stream_job = BashOperator(
        task_id="start_stream_job",
        bash_command="""
        docker exec spark-master bash -lc '
        pgrep -f kafka_to_cassandra_mysql_stream_upsert.py >/dev/null || \
        nohup /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3,com.datastax.spark:spark-cassandra-connector_2.12:3.1.0 \
        /opt/project/jobs/kafka_to_cassandra_mysql_stream_upsert.py \
        >/opt/project/airflow_stream_upsert.log 2>&1 &
        '
        """,
    )

    decide >> [stream_already_running, start_stream_job]