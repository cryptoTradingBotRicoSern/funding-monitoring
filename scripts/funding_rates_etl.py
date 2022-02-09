import requests

import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert


import models

# Set the postgres hook and session for the DB loading process
pg_hook = PostgresHook(postgres_conn_id="postgres_localhost")
engine = pg_hook.get_sqlalchemy_engine()
Session = sessionmaker(bind=engine)
session = Session()


def get_active_future_symbols() -> list:
    """Returns a list of active futures contracts based on the information
    in the kucoin_active_futures table"""
    result = session.query(models.KuCoinActiveFutures.symbol).distinct().all()
    symbol_list = list(map(lambda symbol: symbol[0], result))
    return symbol_list


def funding_table_is_empty() -> bool:
    """Returns True if the funding table is empty and False otherwise"""
    result = session.query(models.KuCoinFundingRates).first()
    return result is None


def get_funding_rates(symbol: str) -> pd.DataFrame:
    url = f"https://futures.kucoin.com/_api/web-front/contract/{symbol}/funding-rates?"
    request = requests.get(url)
    request_data = request.json()
    funding_df = pd.DataFrame.from_dict(request_data["data"]["dataList"])

    # We need to know if we have downloaded all the data available.
    # We will know that by looking at the "hasMore" value in the payload.
    # To query further, we will use the last data point as a reference
    # (since we know that results are sorted in descending order from the most
    # recent data point)
    # has_more = request_data["data"]["hasMore"]
    # end_time = funding_df["timePoint"][funding_df.index[-1]]

    # If the table is empty, we will fill it with historical data. Otherwise,
    # we will verify if we already have the latest funding data. If so, we won't
    # do anything. Otherwise, we will take the latest funding data and upload it
    # to the database.

    # if funding_table_is_empty() == True:

    #     while has_more == True:
    #         url = f"https://futures.kucoin.com/_api/web-front/contract/{symbol}/funding-rates?&endTime={end_time}"
    #         request = requests.get(url)
    #         request_data = request.json()
    #         new_data_df = pd.DataFrame.from_dict(request_data["data"]["dataList"])

    #         funding_df = funding_df.append(new_data_df, ignore_index=True)
    #         has_more = request_data["data"]["hasMore"]
    #         end_time = new_data_df["timePoint"][new_data_df.index[-1]]

    #         # Be kind to the API
    #         time.sleep(0.5)
    # else:
    #     funding_df = funding_df.head(1)

    funding_df = funding_df.head(1)

    return funding_df


def clean_funding_df(data_frame: pd.DataFrame) -> pd.DataFrame:
    clean_df = data_frame.copy()

    # Rename to more legible columns
    clean_df.rename(
        columns={
            "granularity": "funding_period",
            "timePoint": "funding_time",
            "value": "funding_rate",
        },
        inplace=True,
    )

    # Convert Unix to regular timestamps
    clean_df["funding_period"] = pd.to_datetime(clean_df["funding_period"], unit="ms")
    clean_df["funding_period"] = list(
        map(lambda t: t.time(), clean_df["funding_period"])
    )
    clean_df["funding_time"] = pd.to_datetime(clean_df["funding_time"], unit="ms")

    # We can't guarantee that we don't get duplicates, so we make sure to drop
    # them as part of the cleaning process
    clean_df.drop_duplicates(subset=["symbol", "funding_time"], inplace=True)

    return clean_df


def load_funding_df(funding_list: list) -> None:
    # We will try to insert all values we have collected for funding at the
    # given point in time.
    stmt = insert(models.KuCoinFundingRates).values(funding_list)
    # If we already have a record, we will update the funding rate
    stmt = stmt.on_conflict_do_update(
        constraint="kucoin_funding_rates_pkey",
        set_={"funding_rate": stmt.excluded.funding_rate},
    )
    session.execute(stmt)
    session.commit()
    session.close()


def main() -> None:
    full_funding_list = list()
    universe = get_active_future_symbols()

    # We will take all active futures and put them in an appropriate format.
    # We will then
    for symbol in universe:
        raw_df = get_funding_rates(symbol)
        clean_df = clean_funding_df(raw_df)
        clean_dict = clean_df.to_dict("r")[0]
        full_funding_list.append(clean_dict)

    load_funding_df(full_funding_list)


if __name__ == "__main__":
    main()
