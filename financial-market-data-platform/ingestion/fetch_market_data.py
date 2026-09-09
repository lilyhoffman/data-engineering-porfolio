import glob
import os
from datetime import datetime

import pandas as pd
import yfinance as yf


TICKERS = [
    "AAPL",
    "MSFT",
    "NVDA",
    "JPM",
    "AMZN"
]

BRONZE_DIR = "data/bronze/stock_prices"


def get_partition_path():
    today = datetime.now()

    return os.path.join(
        BRONZE_DIR,
        f"year={today.year}",
        f"month={today.month:02d}",
        f"day={today.day:02d}"
    )


def get_latest_loaded_date(ticker):
    pattern = os.path.join(
        BRONZE_DIR,
        "year=*",
        "month=*",
        "day=*",
        f"{ticker}.csv"
    )

    files = glob.glob(pattern)

    if not files:
        return None

    latest_date = None

    for file_path in files:
        try:
            df = pd.read_csv(file_path)

            if "Date" not in df.columns or df.empty:
                continue

            dates = pd.to_datetime(
                df["Date"],
                errors="coerce"
            ).dropna()

            if dates.empty:
                continue

            file_latest_date = dates.max()

            if latest_date is None or file_latest_date > latest_date:
                latest_date = file_latest_date

        except Exception as e:
            print(f"Warning: Could not read {file_path}: {e}")

    return latest_date


def fetch_stock_data(ticker, start_date=None):

    if start_date is None:
        data = yf.download(
            ticker,
            period="1mo",
            interval="1d",
            auto_adjust=False,
            progress=False,
            multi_level_index=False
        )

    else:
        data = yf.download(
            ticker,
            start=start_date,
            interval="1d",
            auto_adjust=False,
            progress=False,
            multi_level_index=False
        )

    if data.empty:
        return data

    data = data.reset_index()

    data["Ticker"] = ticker
    data["ingestion_timestamp"] = datetime.now()

    expected_columns = [
        "Date",
        "Adj Close",
        "Close",
        "High",
        "Low",
        "Open",
        "Volume",
        "Ticker",
        "ingestion_timestamp"
    ]

    data = data[expected_columns]

    return data


def main():

    partition_path = get_partition_path()

    os.makedirs(
        partition_path,
        exist_ok=True
    )

    for ticker in TICKERS:

        print("\n------------------------------")
        print(f"Processing {ticker}")
        print("------------------------------")

        latest_loaded_date = get_latest_loaded_date(ticker)

        if latest_loaded_date is None:
            start_date = None

            print(
                f"No existing data for {ticker}. "
                "Loading initial history."
            )

        else:
            start_date = (
                latest_loaded_date
                + pd.Timedelta(days=1)
            )

            print(
                f"Latest loaded date for {ticker}: "
                f"{latest_loaded_date.date()}"
            )

        today = pd.Timestamp.now().normalize()

        if start_date is not None and start_date > today:
            print(f"{ticker} is already up to date.")
            continue

        if start_date is None:
            print(f"Fetching initial history for {ticker}.")
        else:
            print(
                f"Fetching {ticker} from "
                f"{start_date.date()} onward."
            )

        df = fetch_stock_data(
            ticker,
            start_date=start_date
        )

        if df.empty:
            print(f"No new data available for {ticker}.")
            continue

        output_path = os.path.join(
            partition_path,
            f"{ticker}.csv"
        )

        df.to_csv(
            output_path,
            index=False
        )

        print(
            f"Saved {len(df)} rows to {output_path}"
        )

    print("\nMarket data ingestion complete.")


if __name__ == "__main__":
    main()