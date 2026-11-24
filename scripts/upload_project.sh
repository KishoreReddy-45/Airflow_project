#!/usr/bin/env bash
set -euo pipefail

: "${RATINGS_CODE_S3_BUCKET:?Set RATINGS_CODE_S3_BUCKET}"

aws s3 sync src/ratings "s3://${RATINGS_CODE_S3_BUCKET}/src/ratings" \
  --exclude "__pycache__/*" \
  --exclude "*.pyc"

echo "Uploaded Ratings Spark jobs to s3://${RATINGS_CODE_S3_BUCKET}/src/ratings"
