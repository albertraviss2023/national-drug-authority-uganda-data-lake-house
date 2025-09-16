#!/bin/bash
set -e

ROLE="$1"

if [[ "$ROLE" == "master" ]]; then
  echo "Starting Spark Master..."
  /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master \
    --host spark-master --port 7077 --webui-port 8080 > /tmp/spark-master.log 2>&1 &

  echo "Starting Jupyter Lab..."
  exec jupyter lab \
    --config=/root/.jupyter/jupyter_server_config.py \
    --no-browser --allow-root \
    --notebook-dir=/home/iceberg/notebooks/notebooks \
    --ip=0.0.0.0 --port=8888

elif [[ "$ROLE" == "worker" ]]; then
  echo "Starting Spark Worker..."
  exec /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
    spark://spark-master:7077 --port 7078 --webui-port 8081
else
  echo "Invalid role: $ROLE. Use 'master' or 'worker'"
  exit 1
fi
