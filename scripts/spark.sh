#!/bin/sh
set -eu
# Match the Scala binary and Spark versions to requirements.txt.
exec spark-submit --master 'local[3]' --driver-memory 1g \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7,org.apache.spark:spark-avro_2.12:3.5.7 \
  /app/spark_consumer.py "$@"
