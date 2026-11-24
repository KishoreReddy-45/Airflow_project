from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

# Replace these commands with EMR Serverless StartJobRun operators in MWAA.
# Package the project and dependencies where the EMR job runtime can access them.
with DAG(
    dag_id="ratings_medallion_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="0 * * * *",
    catchup=False,
    tags=["ratings", "medallion", "iceberg"],
) as dag:
    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command="spark-submit /opt/airflow/dags/repo/src/ratings/jobs/bronze_to_silver.py",
    )
    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command="spark-submit /opt/airflow/dags/repo/src/ratings/jobs/silver_to_gold.py",
    )
    bronze_to_silver >> silver_to_gold
