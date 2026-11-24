# Ratings Project — AWS MSK + PySpark Medallion ETL

An end-to-end streaming data engineering reference project for movie-rating events.

## Architecture

`Python producer -> Amazon MSK (Kafka) -> PySpark Structured Streaming -> Bronze Iceberg on S3 -> Silver Iceberg -> Gold Iceberg -> Athena`

- **Bronze:** append-only raw events with Kafka and ingestion metadata.
- **Silver:** schema enforcement, validation, timestamp parsing and event-id deduplication.
- **Gold:** movie-level, user-level and daily rating aggregates.
- **Glue Data Catalog:** Iceberg table catalog.
- **MWAA/Airflow:** orchestrates the Silver and Gold batch transformations.
- **Compute:** a long-running Spark application (for example, EMR on EC2) consumes MSK continuously; EMR Serverless can run the scheduled Silver/Gold batch jobs.

> Important deployment distinction: the MSK-to-Bronze consumer is a continuously running Structured Streaming application. Do not treat a short-lived batch job as a permanent streaming service. Run it on a suitable long-running Spark environment. The included batch jobs are suitable for EMR Serverless.

## Repository layout

```text
Ratings_project/
├── config/
│   ├── app.yaml
│   └── kafka.properties.example
├── dags/
│   └── ratings_medallion_pipeline.py
├── scripts/
│   ├── create_topics.sh
│   ├── submit_streaming.sh
│   └── submit_batch.sh
├── sql/
│   └── athena_queries.sql
├── src/ratings/
│   ├── producer/
│   │   └── rating_producer.py
│   ├── streaming/
│   │   └── msk_to_bronze.py
│   ├── jobs/
│   │   ├── bronze_to_silver.py
│   │   └── silver_to_gold.py
│   └── common/
│       ├── config.py
│       └── schemas.py
├── tests/
│   ├── test_transformations.py
│   └── test_validation.py
├── docker-compose.yml
├── requirements.txt
└── pyproject.toml
```

## Event contract

Kafka topic: `movie-ratings`

Example JSON message:

```json
{
  "event_id": "evt-10001",
  "user_id": 501,
  "movie_id": 42,
  "rating": 4.5,
  "event_ts": "2026-11-24T10:15:00Z"
}
```

`event_id` is the idempotency key. Ratings must be between 0.5 and 5.0.

## 1. Local development

Requirements: Python 3.10+, Java 17, Docker Desktop and Git.

```powershell
git clone https://github.com/KishoreReddy-45/Ratings_project.git
cd Ratings_project
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
docker compose up -d
```

Create the topic:

```powershell
docker exec ratings-kafka kafka-topics --bootstrap-server localhost:9092 --create --topic movie-ratings --partitions 3 --replication-factor 1
```

Start the producer:

```powershell
$env:KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
python -m ratings.producer.rating_producer
```

The producer emits synthetic events. The Spark streaming job expects an Iceberg-enabled Spark runtime and a configured Glue or Hadoop catalog. For local Spark/Iceberg development, configure Spark with the matching Iceberg runtime JAR and a local Hadoop catalog; the AWS deployment uses Glue.

## 2. AWS deployment configuration

Edit `config/app.yaml` and set:
- AWS region
- MSK bootstrap brokers (TLS or SASL/SCRAM endpoints)
- Kafka topic
- S3 warehouse and checkpoint paths
- Glue database
- Iceberg catalog name

Never commit credentials, private broker endpoints, or secrets. Use IAM roles and Secrets Manager. The Spark runtime must have network access to MSK and S3, and the appropriate Kafka and Iceberg connector packages.

Example environment variables:

```bash
export AWS_REGION=ap-south-1
export MSK_BOOTSTRAP_SERVERS="b-1.example:9094,b-2.example:9094"
export KAFKA_TOPIC=movie-ratings
export S3_WAREHOUSE=s3://YOUR_BUCKET/warehouse/
export S3_CHECKPOINT=s3://YOUR_BUCKET/checkpoints/ratings-bronze/
export GLUE_DATABASE=ratings
export ICEBERG_CATALOG=glue_catalog
```

For MSK IAM authentication, configure the Kafka client/Spark Kafka connector for AWS MSK IAM and attach the required IAM permissions to the workload role. For TLS/SCRAM, configure the corresponding JAAS/secret settings securely. The sample consumer uses the Kafka bootstrap/security settings supplied by the runtime; adapt the Spark Kafka options to your chosen MSK authentication mode.

## 3. Create AWS tables and run

Create the Glue database `ratings` first, then submit the streaming job using `scripts/submit_streaming.sh` from your long-running Spark environment. The Bronze table is created automatically by the job.

Run the batch jobs in order:

```bash
spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1 \
  src/ratings/jobs/bronze_to_silver.py

spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1 \
  src/ratings/jobs/silver_to_gold.py
```

Use package versions compatible with the Spark version on your EMR release. Do not blindly copy the sample Iceberg version if your EMR release bundles a different version.

## 4. Orchestration

The Airflow DAG in `dags/ratings_medallion_pipeline.py` demonstrates the Silver-then-Gold dependency. Configure an Airflow connection or replace the placeholder submit commands with your EMR Serverless `StartJobRun` operator configuration. The DAG intentionally does not start/stop the continuous MSK consumer on every schedule.

## 5. Data quality and testing

```bash
pytest -q
```

Tests cover rating range validation, required fields, and deduplication logic. Integration tests against MSK, Glue and S3 should be added in an AWS test account.

## 6. Athena examples

See `sql/athena_queries.sql`.

## Production hardening checklist

- Use MSK TLS or IAM authentication and private subnets/security groups.
- Grant least-privilege IAM access to MSK, S3 and Glue.
- Store secrets in Secrets Manager; never in YAML or Git.
- Configure S3 lifecycle, encryption, retention and Iceberg compaction.
- Monitor consumer lag, streaming query progress, failed records and checkpoint health.
- Route malformed events to a quarantine table/prefix instead of silently dropping them.
- Define replay/backfill procedures before changing checkpoint locations.
- Make Gold aggregates rerunnable and define late-arriving-event handling.
- Add schema compatibility checks and CI tests.
