import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator


REGION = os.getenv("AWS_REGION", "ap-south-1")
APPLICATION_ID = os.getenv("EMR_SERVERLESS_APPLICATION_ID", "{{ var.value.emr_serverless_application_id }}")
EXECUTION_ROLE_ARN = os.getenv(
    "EMR_SERVERLESS_EXECUTION_ROLE_ARN",
    "{{ var.value.emr_serverless_execution_role_arn }}",
)
CODE_BUCKET = os.getenv("RATINGS_CODE_S3_BUCKET", "{{ var.value.ratings_code_s3_bucket }}")
ICEBERG_PACKAGE = os.getenv(
    "ICEBERG_PACKAGE",
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1",
)


def spark_job(entry_point: str, name: str):
    return {
        "name": name,
        "applicationId": APPLICATION_ID,
        "executionRoleArn": EXECUTION_ROLE_ARN,
        "jobDriver": {
            "sparkSubmit": {
                "entryPoint": f"s3://{CODE_BUCKET}/{entry_point}",
                "sparkSubmitParameters": (
                    f"--packages {ICEBERG_PACKAGE} "
                    "--conf spark.sql.extensions="
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
                ),
            }
        },
        "configurationOverrides": {
            "monitoringConfiguration": {
                "cloudWatchLoggingConfiguration": {"enabled": True},
                "s3MonitoringConfiguration": {"logUri": f"s3://{CODE_BUCKET}/emr-logs/"},
            }
        },
    }


with DAG(
    dag_id="ratings_medallion_emr_serverless",
    start_date=datetime(2026, 1, 1),
    schedule="0 * * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    tags=["ratings", "medallion", "iceberg", "emr-serverless"],
) as dag:
    bronze_to_silver = EmrServerlessStartJobOperator(
        task_id="bronze_to_silver",
        name="ratings-bronze-to-silver",
        application_id=APPLICATION_ID,
        execution_role_arn=EXECUTION_ROLE_ARN,
        job_driver=spark_job(
            "src/ratings/jobs/bronze_to_silver.py",
            "ratings-bronze-to-silver",
        )["jobDriver"],
        configuration_overrides=spark_job(
            "src/ratings/jobs/bronze_to_silver.py",
            "ratings-bronze-to-silver",
        )["configurationOverrides"],
        aws_conn_id="aws_default",
        region_name=REGION,
        wait_for_completion=True,
        deferrable=True,
    )

    silver_to_gold = EmrServerlessStartJobOperator(
        task_id="silver_to_gold",
        name="ratings-silver-to-gold",
        application_id=APPLICATION_ID,
        execution_role_arn=EXECUTION_ROLE_ARN,
        job_driver=spark_job(
            "src/ratings/jobs/silver_to_gold.py",
            "ratings-silver-to-gold",
        )["jobDriver"],
        configuration_overrides=spark_job(
            "src/ratings/jobs/silver_to_gold.py",
            "ratings-silver-to-gold",
        )["configurationOverrides"],
        aws_conn_id="aws_default",
        region_name=REGION,
        wait_for_completion=True,
        deferrable=True,
    )

    bronze_to_silver >> silver_to_gold
