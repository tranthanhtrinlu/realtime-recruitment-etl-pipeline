#!/usr/bin/env bash
set -e

ROLE="${SPARK_ROLE:-submit}"

if [ "$ROLE" = "master" ]; then
  exec /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master \
    --host spark-master \
    --port 7077 \
    --webui-port 8080
elif [ "$ROLE" = "worker" ]; then
  exec /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
    --webui-port 8081 \
    spark://spark-master:7077
else
  exec bash
fi