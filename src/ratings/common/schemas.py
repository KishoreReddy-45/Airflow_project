from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType
)

RATING_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", LongType(), True),
    StructField("movie_id", LongType(), True),
    StructField("rating", DoubleType(), True),
    StructField("event_ts", StringType(), True),
])
