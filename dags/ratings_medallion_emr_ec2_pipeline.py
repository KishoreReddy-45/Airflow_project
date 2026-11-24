import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor


CLUSTER_ID = os.getenv("EMR_CLUSTER_ID", "{{ var.value.emr_cluster_id }}")
REGION = os.getenv("AWS_REGION", "ap-south-1")
CODE_BUCKET = os.getenv("RATINGS_CODE_S3_BUCKET", "{{ var.value.ratings_code_s3_bucket }}")
ICEBERG_PACKAGE = os.getenv(
    "ICEBERG_PACKAGE",
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1",
)


def spark_step(name: str, entry_point: str) -> dict:
    return {
        "Name": name,
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--packages",
                ICEBERG_PACKAGE,
                "--conf",
                "spark.sql.extensions="
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                f"s3://{CODE_BUCKET}/{entry_point}",
            ],
        },
    }


with DAG(
    dag_id="ratings_medallion_emr_ec2",
    start_date=datetime(2026, 1, 1),
    schedule="0 * * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    tags=["ratings", "medallion", "iceberg", "emr", "ec2"],
) as dag:
    add_steps = EmrAddStepsOperator(
        task_id="add_silver_then_gold_steps",
        job_flow_id=CLUSTER_ID,
        aws_conn_id="aws_default",
        region_name=REGION,
        steps=[
            spark_step(
                "ratings-bronze-to-silver",
                "src/ratings/jobs/bronze_to_silver.py",
            ),
            spark_step(
                "ratings-silver-to-gold",
                "src/ratings/jobs/silver_to_gold.py",
            ),
        ],
    )

    silver_step_sensor = EmrStepSensor(
        task_id="wait_for_bronze_to_silver",
        job_flow_id=CLUSTER_ID,
        step_id="{{ task_instance.xcom_pull(task_ids='add_silver_then_gold_steps', key='return_value')[0] }}",
        aws_conn_id="aws_default",
        region_name=REGION,
        poke_interval=60,
        timeout=3600,
    )

    gold_step_sensor = EmrStepSensor(
        task_id="wait_for_silver_to_gold",
        job_flow_id=CLUSTER_ID,
        step_id="{{ task_instance.xcom_pull(task_ids='add_silver_then_gold_steps', key='return_value')[1] }}",
        aws_conn_id="aws_default",
        region_name=REGION,
        poke_interval=60,
        timeout=3600,
    )

    add_steps >> silver_step_sensor >> gold_step_sensor
