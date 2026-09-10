import sys

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col,
    datediff,
    dayofmonth,
    month,
    round as spark_round,
    unix_timestamp,
    year,
)


args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "SOURCE_PATH", "TARGET_PATH"]
)

SOURCE_PATH = args["SOURCE_PATH"]
TARGET_PATH = args["TARGET_PATH"]


sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session


# Read raw Bronze Parquet data from S3
df = spark.read.parquet(SOURCE_PATH)


# Select useful columns
silver_df = df.select(
    col("VendorID").alias("vendor_id"),
    col("tpep_pickup_datetime").alias("pickup_datetime"),
    col("tpep_dropoff_datetime").alias("dropoff_datetime"),
    col("passenger_count"),
    col("trip_distance"),
    col("RatecodeID").alias("rate_code_id"),
    col("store_and_fwd_flag"),
    col("PULocationID").alias("pickup_location_id"),
    col("DOLocationID").alias("dropoff_location_id"),
    col("payment_type"),
    col("fare_amount"),
    col("extra"),
    col("mta_tax"),
    col("tip_amount"),
    col("tolls_amount"),
    col("improvement_surcharge"),
    col("total_amount"),
    col("congestion_surcharge"),
    col("Airport_fee").alias("airport_fee"),
    col("cbd_congestion_fee"),
)


# Data-quality filtering
silver_df = silver_df.filter(
    col("pickup_datetime").isNotNull()
    & col("dropoff_datetime").isNotNull()
    & (col("trip_distance") > 0)
    & (col("total_amount") > 0)
    & (col("dropoff_datetime") > col("pickup_datetime"))
)


# Add analytical columns
silver_df = (
    silver_df
    .withColumn(
        "trip_duration_minutes",
        spark_round(
            (
                unix_timestamp("dropoff_datetime")
                - unix_timestamp("pickup_datetime")
            ) / 60,
            2,
        ),
    )
    .withColumn("pickup_year", year("pickup_datetime"))
    .withColumn("pickup_month", month("pickup_datetime"))
    .withColumn("pickup_day", dayofmonth("pickup_datetime"))
)


# Additional sanity checks
silver_df = silver_df.filter(
    (col("trip_duration_minutes") > 0)
    & (col("trip_duration_minutes") <= 1440)
)


# Write optimized Silver data as partitioned Parquet
(
    silver_df.write
    .mode("overwrite")
    .partitionBy("pickup_year", "pickup_month")
    .parquet(TARGET_PATH)
)

print(f"Silver data written to {TARGET_PATH}")