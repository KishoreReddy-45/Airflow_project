from datetime import datetime
from airflow import DAG

from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator


with DAG(
    dag_id="ratings_medallion_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="0 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["ratings", "medallion", "emr-serverless"],
) as dag:
    bronze_to_silver = EmrServerlessStartJobOperator(
        task_id="bronze_to_silver",
        name="ratings-bronze-to-silver",
        application_id="{{ var.value.emr_serverless_application_id }}",
        execution_role_arn="{{ var.value.emr_serverless_execution_role_arn }}",
        job_driver={
            "sparkSubmit": {
                "entryPoint": "s3://{{ var.value.ratings_code_s3_bucket }}/src/ratings/jobs/bronze_to_silver.py",
                "sparkSubmitParameters": (
                    "--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1 "
                    "--conf spark.sql.extensions="
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
                ),
            }
        },
        aws_conn_id="aws_default",
        region_name="{{ var.value.aws_region | default('ap-south-1') }}",
        wait_for_completion=True,
        deferrable=True,
    )
    silver_to_gold = EmrServerlessStartJobOperator(
        task_id="silver_to_gold",
        name="ratings-silver-to-gold",
        application_id="{{ var.value.emr_serverless_application_id }}",
        execution_role_arn="{{ var.value.emr_serverless_execution_role_arn }}",
        job_driver={
            "sparkSubmit": {
                "entryPoint": "s3://{{ var.value.ratings_code_s3_bucket }}/src/ratings/jobs/silver_to_gold.py",
                "sparkSubmitParameters": (
                    "--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1 "
                    "--conf spark.sql.extensions="
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
                ),
            }
        },
        aws_conn_id="aws_default",
        region_name="{{ var.value.aws_region | default('ap-south-1') }}",
        wait_for_completion=True,
        deferrable=True,
    )
    bronze_to_silver >> silver_to_gold
