#!/usr/bin/env bash
set -euo pipefail
spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  src/ratings/streaming/msk_to_bronze.py
