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


def get_raw_spot_pairs() -> pd.DataFrame:
    """Gets all active spot pairs"""
    request = requests.get("https://api.kucoin.com/api/v1/market/allTickers")
    request_data = request.json()
    spot_pairs_df = pd.DataFrame().from_dict(request_data["data"]["ticker"])
    return spot_pairs_df


def get_spot_pairs() -> pd.DataFrame:
    """Gets a the spot pairs data frame and returns a clean version of it"""
    df = get_raw_spot_pairs()
    clean_df = df.copy()

    # Drop irrelevant columns
    keep = ["symbol", "vol", "volValue", "takerFeeRate", "makerFeeRate"]
    clean_df = clean_df[keep]

    # Turn camelCase to snake_case
    clean_df.columns = list(
        map(lambda col: re.sub(r"(?<!^)(?=[A-Z])", "_", col).lower(), clean_df.columns)
    )

    # Rename columns
    clean_df.rename(
        columns={
            "vol": "spot_unit_volume24h",
            "vol_value": "spot_dollar_volume24h",
            "taker_fee_rate": "spot_taker_fee_rate",
            "maker_fee_rate": "spot_maker_fee_rate",
        },
        inplace=True,
    )

    # Create base and quote currencies
    clean_df["base_currency"] = list(
        map(lambda string: string.split("-")[0], clean_df["symbol"])
    )
    clean_df["quote_currency"] = list(
        map(lambda string: string.split("-")[1], clean_df["symbol"])
    )

    # Just keep USDT pairs
    clean_df = clean_df[clean_df["quote_currency"] == "USDT"].reset_index(drop=True)

    return clean_df


def load_active_spot_pairs() -> None:
    """Loads the clean lending data to the PostgreSQL database"""

    # Get the new data
    df = get_spot_pairs()

    # Drop the old data
    session.execute("""TRUNCATE TABLE kucoin_active_spot_pairs""")
    session.commit()
    session.close()

    # Load new data
    df.to_csv("active_spot_pairs_df.csv", index=False, header=False, sep="\t")
    pg_hook.bulk_load("kucoin_active_spot_pairs", "active_spot_pairs_df.csv")


if __name__ == "__main__":
    load_active_spot_pairs()
