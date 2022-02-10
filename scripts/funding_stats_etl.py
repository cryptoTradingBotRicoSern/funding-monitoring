import datetime as dt
import requests

import numpy as np
import pandas as pd

from airflow.hooks.postgres_hook import PostgresHook
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert


from models import KuCoinFundingRates

# Set the postgres hook and session for the DB loading process
pg_hook = PostgresHook(postgres_conn_id="postgres_localhost")
engine = pg_hook.get_sqlalchemy_engine()
Session = sessionmaker(bind=engine)
session = Session()


def query_funding_data() -> pd.DataFrame:
    raw_results = (
        session.query(KuCoinFundingRates)
        .filter(
            KuCoinFundingRates.funding_time >= dt.date.today() - dt.timedelta(days=120)
        )
        .order_by(
            KuCoinFundingRates.symbol.asc(), KuCoinFundingRates.funding_time.desc()
        )
        .all()
    )
    raw_df = pd.DataFrame(list(map(lambda res: res.__dict__, raw_results)))
    raw_df.drop("_sa_instance_state", axis=1, inplace=True)
    raw_df = raw_df[["symbol", "funding_period", "funding_time", "funding_rate"]]
    return raw_df


def get_futures_data() -> pd.DataFrame:
    """Gets all active futures contracts"""
    request = requests.get("https://api-futures.kucoin.com/api/v1/contracts/active")
    request_data = request.json()
    active_futures_df = pd.DataFrame.from_dict(request_data["data"])
    active_futures_df = active_futures_df[
        active_futures_df["symbol"].str.endswith("TM").reset_index(drop=True)
    ]
    keep = ["symbol", "predictedFundingFeeRate", "turnoverOf24h"]

    clean_df = active_futures_df[keep]

    clean_df.rename(
        columns={
            "predictedFundingFeeRate": "predicted_funding_rate",
            "turnoverOf24h": "dollar_volume_24h",
        },
        inplace=True,
    )

    return clean_df


def build_stats_table() -> pd.DataFrame:
    """Builds the funding stats table which consists of:
    - Averages of funding rates over time
    - 24h Volume"""
    full_funding_stats = pd.DataFrame()
    raw_df = query_funding_data()
    futures_data = get_futures_data()
    universe = futures_data["symbol"].unique()

    for symbol in universe:
        df = raw_df[raw_df["symbol"] == symbol].reset_index(drop=True)

        funding_stats = dict()
        funding_stats["symbol"] = symbol
        funding_stats["funding_8h"] = (
            df.loc[0, "funding_rate"] * 3 * 365
        )  # The most recent funding rate data point

        periods = ["24h", "3d", "7d", "14d", "30d", "90d"]
        num_funding_sessions = [3, 9, 21, 42, 90, 270]

        for period, num_sessions in zip(periods, num_funding_sessions):
            if len(df) >= num_sessions:
                funding_stats["funding_" + period] = (
                    df["funding_rate"]
                    .rolling(num_sessions)
                    .mean()
                    .dropna()
                    .reset_index(drop=True)[0]
                    * 3
                    * 365
                )
            else:
                funding_stats["funding_" + period] = np.NaN

        full_funding_stats = full_funding_stats.append(funding_stats, ignore_index=True)

        final_df = pd.merge(full_funding_stats, futures_data, on="symbol")
        final_df["predicted_funding_rate"] = (
            final_df["predicted_funding_rate"] * 3 * 365
        )
        final_df = final_df[
            [
                "symbol",
                "dollar_volume_24h",
                "predicted_funding_rate",
                "funding_8h",
                "funding_24h",
                "funding_3d",
                "funding_7d",
                "funding_14d",
                "funding_30d",
                "funding_90d",
            ]
        ]

    return final_df


def load_funding_stats_table():
    # Get the new data
    df = build_stats_table()

    # # Load new data
    df.to_sql("kucoin_funding_stats", con=engine, if_exists="replace", index=False)


if __name__ == "__main__":
    load_funding_stats_table()
