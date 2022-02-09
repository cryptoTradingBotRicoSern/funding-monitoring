import datetime as dt
import numpy as np
import pandas as pd

from airflow.hooks.postgres_hook import PostgresHook
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert


from models import KuCoinFundingRates, KuCoinActiveFutures

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


def build_stats_table() -> pd.DataFrame:
    """Builds the funding stats table which consists of:
    - Averages of funding rates over time
    - 24h Volume"""
    full_funding_stats = pd.DataFrame()
    raw_df = query_funding_data()
    universe = [
        sym[0] for sym in session.query(KuCoinActiveFutures.symbol).distinct().all()
    ]

    for symbol in universe:
        df = raw_df[raw_df["symbol"] == symbol].reset_index(drop=True)

        funding_stats = dict()
        funding_stats["symbol"] = symbol
        funding_stats["volume"] = (
            session.query(KuCoinActiveFutures.futures_dollar_volume_24h)
            .filter(KuCoinActiveFutures.symbol == symbol)
            .first()[0]
        ).astype(float)
        funding_stats["predicted_funding_rate"] = (
            session.query(KuCoinActiveFutures.predicted_funding_fee_rate)
            .filter(KuCoinActiveFutures.symbol == symbol)
            .first()[0]
        )
        funding_stats["funding_8h"] = (
            df.loc[0, "funding_rate"] * 365
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
                    * 365
                )
            else:
                funding_stats["funding_" + period] = np.NaN

        full_funding_stats = full_funding_stats.append(funding_stats, ignore_index=True)

    return full_funding_stats


def load_funding_stats_table():
    # Get the new data
    df = build_stats_table()

    print(df)

    # Drop the old data. I do this after already having new data to minimize
    # the time the table is unavailable.
    # session.execute("""TRUNCATE TABLE kucoin_funding_stats""")
    # session.commit()
    # session.close()

    # # Load new data
    # df.to_csv("funding_stats_df.csv", index=False, header=False, sep="\t")
    # pg_hook.bulk_load("kucoin_funding_stats", "funding_stats_df.csv")


if __name__ == "__main__":
    load_funding_stats_table()
