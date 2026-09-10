import psycopg2
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    from_json,
    round as spark_round,
    sum as spark_sum,
    to_timestamp,
    window,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)


DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "database": os.getenv("POSTGRES_DB", "ecommerce"),
    "user": os.getenv("POSTGRES_USER", "ecommerce_user"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "port": int(os.getenv("POSTGRES_INTERNAL_PORT", "5432")),
}

spark = (
    SparkSession.builder
    .appName("EcommerceOrderStreaming")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


order_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("category", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("unit_price", DoubleType(), False),
    StructField("order_timestamp", StringType(), False),
    StructField("payment_method", StringType(), False),
    StructField("status", StringType(), False),
])


raw_orders = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "orders")
    .option("startingOffsets", "earliest")
    .load()
)


parsed_orders = (
    raw_orders
    .selectExpr("CAST(value AS STRING) AS json_value")
    .select(
        from_json(col("json_value"), order_schema).alias("order")
    )
    .select("order.*")
)


valid_orders = parsed_orders.filter(
    col("order_id").isNotNull()
    & col("customer_id").isNotNull()
    & col("product_id").isNotNull()
    & col("category").isNotNull()
    & (col("quantity") > 0)
    & (col("unit_price") > 0)
    & col("status").isin("completed", "cancelled")
)


transformed_orders = (
    valid_orders
    .withColumn(
        "order_total",
        spark_round(col("quantity") * col("unit_price"), 2)
    )
    .withColumn(
        "order_timestamp",
        to_timestamp(col("order_timestamp"))
    )
)


def write_orders_to_postgres(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    rows = batch_df.collect()

    connection = psycopg2.connect(**DB_CONFIG)

    cursor = connection.cursor()

    insert_query = """
        INSERT INTO orders (
            order_id,
            customer_id,
            product_id,
            category,
            quantity,
            unit_price,
            order_total,
            order_timestamp,
            payment_method,
            status
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (order_id)
        DO NOTHING;
    """

    for row in rows:
        cursor.execute(
            insert_query,
            (
                row.order_id,
                row.customer_id,
                row.product_id,
                row.category,
                row.quantity,
                row.unit_price,
                row.order_total,
                row.order_timestamp,
                row.payment_method,
                row.status,
            ),
        )

    connection.commit()
    cursor.close()
    connection.close()

    print(f"Orders batch {batch_id} processed.")


completed_orders = transformed_orders.filter(
    col("status") == "completed"
)


metrics = (
    completed_orders
    .withWatermark("order_timestamp", "10 minutes")
    .groupBy(
        window(col("order_timestamp"), "5 minutes"),
        col("category"),
    )
    .agg(
        count("*").alias("order_count"),
        spark_round(spark_sum("order_total"), 2).alias("total_revenue"),
        spark_round(avg("order_total"), 2).alias("avg_order_value"),
    )
)


def write_metrics_to_postgres(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    rows = batch_df.collect()

    connection = psycopg2.connect(**DB_CONFIG)

    cursor = connection.cursor()

    upsert_query = """
        INSERT INTO order_metrics (
            window_start,
            window_end,
            category,
            order_count,
            total_revenue,
            avg_order_value
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (window_start, window_end, category)
        DO UPDATE SET
            order_count = EXCLUDED.order_count,
            total_revenue = EXCLUDED.total_revenue,
            avg_order_value = EXCLUDED.avg_order_value;
    """

    for row in rows:
        cursor.execute(
            upsert_query,
            (
                row.window.start,
                row.window.end,
                row.category,
                row.order_count,
                row.total_revenue,
                row.avg_order_value,
            ),
        )

    connection.commit()
    cursor.close()
    connection.close()

    print(f"Metrics batch {batch_id} processed.")


orders_query = (
    transformed_orders.writeStream
    .foreachBatch(write_orders_to_postgres)
    .option("checkpointLocation", "/app/data/checkpoints/orders")
    .start()
)


metrics_query = (
    metrics.writeStream
    .outputMode("update")
    .foreachBatch(write_metrics_to_postgres)
    .option("checkpointLocation", "/app/data/checkpoints/metrics")
    .start()
)


spark.streams.awaitAnyTermination()