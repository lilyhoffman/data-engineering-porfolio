import glob
import os

import pandas as pd
import psycopg2


SILVER_DIR = "data/silver/stock_prices"

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "database": os.getenv("POSTGRES_DB", "market_data"),
    "user": os.getenv("POSTGRES_USER", "market_user"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
}


def get_silver_files():
    return glob.glob(
        f"{SILVER_DIR}/*.parquet"
    )


def load_companies(df, connection):
    companies = (
        df[
            [
                "ticker",
                "company_name",
                "sector",
                "industry",
                "exchange",
            ]
        ]
        .drop_duplicates(subset=["ticker"])
    )

    cursor = connection.cursor()
    rows_inserted = 0

    try:
        for _, row in companies.iterrows():

            cursor.execute(
                """
                INSERT INTO companies (
                    ticker,
                    company_name,
                    sector,
                    industry,
                    exchange
                )
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (ticker)
                DO UPDATE SET
                    company_name = EXCLUDED.company_name,
                    sector = EXCLUDED.sector,
                    industry = EXCLUDED.industry,
                    exchange = EXCLUDED.exchange;
                """,
                (
                    row["ticker"],
                    row["company_name"],
                    row["sector"],
                    row["industry"],
                    row["exchange"],
                )
            )

            rows_inserted += cursor.rowcount

    finally:
        cursor.close()

    return rows_inserted


def load_stock_prices(df, connection):
    cursor = connection.cursor()
    rows_inserted = 0

    try:
        for _, row in df.iterrows():

            cursor.execute(
                """
                INSERT INTO stock_prices (
                    ticker,
                    trade_date,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    adjusted_close,
                    volume,
                    ingestion_timestamp
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ticker, trade_date)
                DO NOTHING;
                """,
                (
                    row["ticker"],
                    row["trade_date"],
                    row["open_price"],
                    row["high_price"],
                    row["low_price"],
                    row["close_price"],
                    row["adjusted_close"],
                    row["volume"],
                    row["ingestion_timestamp"],
                )
            )

            rows_inserted += cursor.rowcount

    finally:
        cursor.close()

    return rows_inserted


def main():
    files = get_silver_files()

    print(f"Found {len(files)} Silver Parquet files.")

    if not files:
        print("No Silver files found.")
        return

    connection = psycopg2.connect(**DB_CONFIG)

    try:
        company_rows = 0
        stock_rows = 0

        for file_path in files:

            print(f"\nLoading {file_path}...")

            df = pd.read_parquet(file_path)

            if df.empty:
                continue

            company_rows += load_companies(
                df,
                connection
            )

            stock_rows += load_stock_prices(
                df,
                connection
            )

        connection.commit()

        print("\nLoad complete.")
        print(f"Company rows processed: {company_rows}")
        print(f"New stock price rows inserted: {stock_rows}")

    except Exception as e:
        connection.rollback()
        print(f"\nLoad failed: {e}")
        raise

    finally:
        connection.close()


if __name__ == "__main__":
    main()