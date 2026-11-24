#!/usr/bin/env bash
set -euo pipefail
: "${MSK_BOOTSTRAP_SERVERS:?Set MSK_BOOTSTRAP_SERVERS}"
kafka-topics.sh --bootstrap-server "$MSK_BOOTSTRAP_SERVERS" \
  --create --if-not-exists --topic "${KAFKA_TOPIC:-movie-ratings}" \
  --partitions 3 --replication-factor 3
