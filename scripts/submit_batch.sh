#!/usr/bin/env bash
set -euo pipefail
ICEBERG_PACKAGE="${ICEBERG_PACKAGE:-org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1}"
spark-submit --packages "$ICEBERG_PACKAGE" src/ratings/jobs/bronze_to_silver.py
spark-submit --packages "$ICEBERG_PACKAGE" src/ratings/jobs/silver_to_gold.py
