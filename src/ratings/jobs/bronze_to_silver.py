from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, current_timestamp
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from ratings.common.config import S3_WAREHOUSE, GLUE_DATABASE, ICEBERG_CATALOG

def main():
    spark = (
        SparkSession.builder.appName("ratings-bronze-to-silver")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse", S3_WAREHOUSE)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate()
    )
    db = GLUE_DATABASE
    bronze = spark.table(f"{ICEBERG_CATALOG}.{db}.ratings_bronze")

    valid = (
        bronze.filter(
            col("event_id").isNotNull()
            & col("user_id").isNotNull()
            & col("movie_id").isNotNull()
            & col("rating").between(0.5, 5.0)
            & col("event_ts").isNotNull()
        )
        .withColumn("event_ts", to_timestamp("event_ts"))
        .filter(col("event_ts").isNotNull())
    )

    # Keep the latest observed copy of each event_id in this processing batch.
    window = Window.partitionBy("event_id").orderBy(col("kafka_timestamp").desc())
    clean = (
        valid.withColumn("_rn", row_number().over(window))
        .filter(col("_rn") == 1)
        .drop("_rn")
        .withColumn("processed_at", current_timestamp())
    )
    clean.createOrReplaceTempView("incoming_ratings")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_CATALOG}.{db}.ratings_silver
        USING iceberg
        AS SELECT * FROM incoming_ratings WHERE 1 = 0
    """)
    spark.sql(f"""
        MERGE INTO {ICEBERG_CATALOG}.{db}.ratings_silver t
        USING incoming_ratings s
        ON t.event_id = s.event_id
        WHEN NOT MATCHED THEN INSERT *
    """)
    spark.stop()

if __name__ == "__main__":
    main()
