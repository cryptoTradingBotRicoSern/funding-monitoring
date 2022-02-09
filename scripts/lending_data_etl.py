import datetime as dt
import requests

import numpy as np
import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert

import models

# Set the postgres hook for the DB loading process
pg_hook = PostgresHook(postgres_conn_id="postgres_localhost")
engine = pg_hook.get_sqlalchemy_engine()
Session = sessionmaker(bind=engine)
session = Session()


# Extract
def get_raw_lending_data() -> pd.DataFrame:
    """Gets KuCoin lending data at the current point in time for each term
    length (7 days, 14 days, and 28 days)"""
    request = requests.get("https://api.kucoin.com/api/v1/margin/market?currency=USDT")
    request_data = request.json()
    lending_df = pd.DataFrame.from_dict(request_data["data"])
    return lending_df


# Transform
def get_lending_data() -> pd.DataFrame:
    """Returns the lowest available USDT interest rates for each term length
    (7 days, 14 days, and 28 days) at the current point in time"""
    base_df = get_raw_lending_data()
    df = base_df.copy()

    # Rename variables
    df.rename(
        columns={"dailyIntRate": "daily_interest_rate", "term": "loan_duration_days"},
        inplace=True,
    )

    # We're just interested in returning the lowest available lending rates
    lending_df = (
        df.groupby("loan_duration_days")["daily_interest_rate"].min().reset_index()
    )

    # Add annualized interest rates
    lending_df["annualized_interest_rate"] = (
        lending_df["daily_interest_rate"].astype(float) * 365
    )

    # We will add the timestamp at which we downloaded the data rounded to 5min
    lending_df["created_at"] = dt.datetime.utcnow()
    ns5min = 5 * 60 * 1000000000  # Get the nanoseconds in 5 min to do the rounding
    lending_df["created_at"] = pd.to_datetime(
        ((lending_df["created_at"].astype(np.int64) // ns5min + 1) * ns5min)
    )
    # Pandas is anoying so we need to convert the column from pandas timestamp
    # to a datetime again.
    lending_df["created_at"] = lending_df["created_at"].dt.to_pydatetime()

    # Reorganize the columns
    lending_df = lending_df[
        [
            "created_at",
            "daily_interest_rate",
            "loan_duration_days",
            "annualized_interest_rate",
        ]
    ]

    return lending_df


# Load
def load_lending_data() -> None:
    """Loads the clean lending data to the PostgreSQL database"""
    lending_list = get_lending_data().to_dict("r")

    stmt = insert(models.KuCoinLendingRates).values(lending_list)
    # If we already have a record, we will update the funding rate
    stmt = stmt.on_conflict_do_update(
        constraint="kucoin_lending_rates_pkey",
        set_={
            "daily_interest_rate": stmt.excluded.daily_interest_rate,
            "annualized_interest_rate": stmt.excluded.annualized_interest_rate,
        },
    )
    session.execute(stmt)
    session.commit()
    session.close()


if __name__ == "__main__":
    load_lending_data()
