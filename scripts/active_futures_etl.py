import re
import requests

import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook
from sqlalchemy.orm import sessionmaker

# Set the postgres hook and session for the DB loading process
pg_hook = PostgresHook(postgres_conn_id="postgres_localhost")
engine = pg_hook.get_sqlalchemy_engine()
Session = sessionmaker(bind=engine)
session = Session()


def get_raw_active_futures() -> pd.DataFrame:
    """Gets all active futures contracts"""
    request = requests.get("https://api-futures.kucoin.com/api/v1/contracts/active")
    request_data = request.json()
    active_futures_df = pd.DataFrame.from_dict(request_data["data"])
    return active_futures_df


def get_active_futures() -> pd.DataFrame:
    """Gets the active futures contracts and processes the data to return a
    clean dataframe"""
    df = get_raw_active_futures()
    clean_df = df.copy()

    # Perpetuals always finish with an "M" so we only keep the perpetuals
    clean_df = clean_df[clean_df["symbol"].str.endswith("TM")]

    # Drop irrelevant columns
    keep = [
        "symbol",
        "firstOpenDate",
        "baseCurrency",
        "quoteCurrency",
        "tickSize",
        "initialMargin",
        "maintainMargin",
        "maxRiskLimit",
        "minRiskLimit",
        "makerFeeRate",
        "takerFeeRate",
        "fundingFeeRate",
        "predictedFundingFeeRate",
        "openInterest",
        "turnoverOf24h",
        "volumeOf24h",
        "nextFundingRateTime",
        "maxLeverage",
    ]

    clean_df = clean_df[keep]

    # Turn camelCase column names to snake_case
    # If needed make this more efficient by pre-compiling the regex as shown in
    # https://stackoverflow.com/questions/1175208/elegant-python-function-to-convert-camelcase-to-snake-case
    clean_df.columns = list(
        map(lambda col: re.sub(r"(?<!^)(?=[A-Z])", "_", col).lower(), clean_df.columns)
    )

    # Small correction for BTC
    #     clean_df["symbol"] = clean_df["symbol"].str.replace("XBT", "BTC")
    clean_df["base_currency"] = clean_df["base_currency"].str.replace("XBT", "BTC")

    # Convert unix timestamps to regular timestamps
    clean_df["first_open_date"] = pd.to_datetime(clean_df["first_open_date"], unit="ms")
    clean_df["first_open_date"] = list(
        map(lambda t: t.replace(microsecond=0), clean_df["first_open_date"])
    )

    clean_df["next_funding_rate_time"] = pd.to_datetime(
        clean_df["next_funding_rate_time"], unit="ms"
    )
    # clean_df["time_to_next_funding_rate"] = list(map(lambda t: t.strftime("%H:%M:%S"), symbols["next_funding_rate_time"]))
    clean_df["next_funding_rate_time"] = list(
        map(
            lambda t: t.time().replace(microsecond=0),
            clean_df["next_funding_rate_time"],
        )
    )
    clean_df.rename(
        columns={"next_funding_rate_time": "time_to_next_funding_rate"}, inplace=True
    )

    return clean_df


def load_active_futures() -> None:
    """Loads the clean active futures data to the PostgreSQL database"""

    # Get the new data
    df = get_active_futures()

    # Drop the old data. I do this after already having new data to minimize
    # the time the table is unavailable.
    session.execute("""TRUNCATE TABLE kucoin_active_futures""")
    session.commit()
    session.close()

    # Load new data
    df.to_csv("active_futures_df.csv", index=False, header=False, sep="\t")
    pg_hook.bulk_load("kucoin_active_futures", "active_futures_df.csv")


if __name__ == "__main__":
    load_active_futures()
