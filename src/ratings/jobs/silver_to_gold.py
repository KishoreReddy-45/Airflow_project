from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, current_date, date_format, to_date, col
from ratings.common.config import S3_WAREHOUSE, GLUE_DATABASE, ICEBERG_CATALOG

def main():
    spark = (
        SparkSession.builder.appName("ratings-silver-to-gold")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse", S3_WAREHOUSE)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate()
    )
    db = GLUE_DATABASE
    silver = spark.table(f"{ICEBERG_CATALOG}.{db}.ratings_silver")

    movie = silver.groupBy("movie_id").agg(
        avg("rating").alias("average_rating"),
        count("*").alias("rating_count")
    )
    daily = silver.withColumn("rating_date", to_date("event_ts")).groupBy("rating_date").agg(
        avg("rating").alias("average_rating"),
        count("*").alias("rating_count")
    )
    user = silver.groupBy("user_id").agg(
        avg("rating").alias("average_rating"),
        count("*").alias("rating_count")
    )

    for df, name in [
        (movie, "gold_movie_rating_summary"),
        (daily, "gold_daily_rating_summary"),
        (user, "gold_user_rating_summary"),
    ]:
        table = f"{ICEBERG_CATALOG}.{db}.{name}"
        df.createOrReplaceTempView("gold_incoming")
        spark.sql(f"CREATE TABLE IF NOT EXISTS {table} USING iceberg AS SELECT * FROM gold_incoming WHERE 1=0")
        spark.sql(f"INSERT OVERWRITE {table} SELECT * FROM gold_incoming")

    spark.stop()

if __name__ == "__main__":
    main()
