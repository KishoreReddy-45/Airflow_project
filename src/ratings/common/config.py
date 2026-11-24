import os

def env(name: str, default: str) -> str:
    return os.getenv(name, default)

AWS_REGION = env("AWS_REGION", "ap-south-1")
MSK_BOOTSTRAP_SERVERS = env("MSK_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = env("KAFKA_TOPIC", "movie-ratings")
S3_WAREHOUSE = env("S3_WAREHOUSE", "s3://YOUR_BUCKET/warehouse/")
S3_CHECKPOINT = env("S3_CHECKPOINT", "s3://YOUR_BUCKET/checkpoints/ratings-bronze/")
GLUE_DATABASE = env("GLUE_DATABASE", "ratings")
ICEBERG_CATALOG = env("ICEBERG_CATALOG", "glue_catalog")
