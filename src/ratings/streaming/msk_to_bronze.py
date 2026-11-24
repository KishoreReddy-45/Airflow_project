from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, lit
from ratings.common.config import (
    MSK_BOOTSTRAP_SERVERS, KAFKA_TOPIC, S3_WAREHOUSE,
    S3_CHECKPOINT, GLUE_DATABASE, ICEBERG_CATALOG
)
from ratings.common.schemas import RATING_SCHEMA

def create_spark():
    return (
        SparkSession.builder
        .appName("ratings-msk-to-bronze")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse", S3_WAREHOUSE)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate()
    )

def main():
    spark = create_spark()
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {ICEBERG_CATALOG}.{GLUE_DATABASE}")

    kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", MSK_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        kafka.select(
            from_json(col("value").cast("string"), RATING_SCHEMA).alias("event"),
            col("topic").alias("kafka_topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"),
        )
        .select("event.*", "kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp")
        .withColumn("ingested_at", current_timestamp())
        .withColumn("source_system", lit("amazon_msk"))
    )

    table = f"{ICEBERG_CATALOG}.{GLUE_DATABASE}.ratings_bronze"
    query = (
        parsed.writeStream
        .format("iceberg")
        .outputMode("append")
        .option("path", f"{S3_WAREHOUSE.rstrip('/')}/bronze/ratings_bronze")
        .option("checkpointLocation", S3_CHECKPOINT)
        .trigger(processingTime="30 seconds")
        .toTable(table)
    )
    query.awaitTermination()

if __name__ == "__main__":
    main()
