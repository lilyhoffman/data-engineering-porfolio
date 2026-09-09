import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


BRONZE_DIR = "data/bronze/stock_prices"
COMPANY_REFERENCE = "data/reference/companies.csv"
SILVER_DIR = "data/silver/stock_prices"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("StockPricesBronzeToSilver")
        .master("local[*]")
        .getOrCreate()
    )


def get_stock_schema():
    return StructType([
        StructField("Date", DateType(), False),
        StructField("Adj Close", DoubleType(), True),
        StructField("Close", DoubleType(), True),
        StructField("High", DoubleType(), True),
        StructField("Low", DoubleType(), True),
        StructField("Open", DoubleType(), True),
        StructField("Volume", LongType(), True),
        StructField("Ticker", StringType(), False),
        StructField(
            "ingestion_timestamp",
            TimestampType(),
            False
        ),
    ])


def get_company_schema():
    return StructType([
        StructField("ticker", StringType(), False),
        StructField("company_name", StringType(), False),
        StructField("sector", StringType(), True),
        StructField("industry", StringType(), True),
        StructField("exchange", StringType(), True),
    ])


def main():
    spark = create_spark_session()

    try:
        stock_input_path = os.path.join(
            BRONZE_DIR,
            "year=*",
            "month=*",
            "day=*",
            "*.csv"
        )

        print("Reading Bronze stock data...")

        stock_df = (
            spark.read
            .option("header", True)
            .schema(get_stock_schema())
            .csv(stock_input_path)
        )

        print(f"Bronze stock rows: {stock_df.count()}")

        stock_df = (
            stock_df
            .withColumnRenamed("Date", "trade_date")
            .withColumnRenamed("Adj Close", "adjusted_close")
            .withColumnRenamed("Close", "close_price")
            .withColumnRenamed("High", "high_price")
            .withColumnRenamed("Low", "low_price")
            .withColumnRenamed("Open", "open_price")
            .withColumnRenamed("Volume", "volume")
            .withColumnRenamed("Ticker", "ticker")
        )

        stock_df = stock_df.dropDuplicates([
            "ticker",
            "trade_date"
        ])

        stock_df = stock_df.filter(
            col("ticker").isNotNull()
            & col("trade_date").isNotNull()
            & (col("open_price") > 0)
            & (col("close_price") > 0)
            & (col("high_price") >= col("low_price"))
            & (col("volume") >= 0)
        )

        print(f"Valid stock rows: {stock_df.count()}")

        print("Reading company reference data...")

        company_df = (
            spark.read
            .option("header", True)
            .schema(get_company_schema())
            .csv(COMPANY_REFERENCE)
        )

        company_df = company_df.dropDuplicates(["ticker"])

        company_df = company_df.filter(
            col("ticker").isNotNull()
            & col("company_name").isNotNull()
        )

        print(f"Company reference rows: {company_df.count()}")

        silver_df = stock_df.join(
            company_df,
            on="ticker",
            how="left"
        )

        print(f"Joined Silver rows: {silver_df.count()}")

        silver_df = silver_df.select(
            "ticker",
            "company_name",
            "sector",
            "industry",
            "exchange",
            "trade_date",
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "adjusted_close",
            "volume",
            "ingestion_timestamp"
        )

        print("Silver schema:")
        silver_df.printSchema()

        print("Sample Silver data:")
        silver_df.show(10, truncate=False)

        os.makedirs(
            SILVER_DIR,
            exist_ok=True
        )

        (
            silver_df.write
            .mode("overwrite")
            .parquet(SILVER_DIR)
        )

        print(
            f"Silver data written to {SILVER_DIR}"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()