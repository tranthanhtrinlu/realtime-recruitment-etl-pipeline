from datetime import datetime
import subprocess

import mysql.connector
from cassandra.cluster import Cluster
from airflow import DAG
from airflow.operators.python import PythonOperator


def check_stream_process():
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

    if result.returncode != 0:
        raise Exception("Spark streaming job is not running")


def check_mysql_final():
    conn = mysql.connector.connect(
        host="127.0.0.1",
        port=3307,
        user="root",
        password="1",
        database="etl_dw",
    )
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM fact_events_hourly_kafka_final")
        count = cur.fetchone()[0]
        cur.close()
    finally:
        conn.close()

    if count <= 0:
        raise Exception("fact_events_hourly_kafka_final is empty")


def check_cassandra_raw():
    cluster = Cluster(["127.0.0.1"], port=19042)
    try:
        session = cluster.connect("logs")
        row = session.execute("SELECT * FROM tracking_raw_kafka LIMIT 1").one()
    finally:
        cluster.shutdown()

    if row is None:
        raise Exception("tracking_raw is empty")

with DAG(
    dag_id="quality_checks_kafka_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="*/10 * * * *",
    catchup=False,
    tags=["quality", "monitoring"],
) as dag:
    t1 = PythonOperator(
        task_id="check_stream_process",
        python_callable=check_stream_process,
    )

    t2 = PythonOperator(
        task_id="check_mysql_final",
        python_callable=check_mysql_final,
    )

    t3 = PythonOperator(
        task_id="check_cassandra_raw",
        python_callable=check_cassandra_raw,
    )

    t1 >> t2 >> t3