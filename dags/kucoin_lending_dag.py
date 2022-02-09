import datetime as dt
import requests

import numpy as np
import pandas as pd
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

default_args = {
    "owner": "santi",
    "retries": 5,
    "retry_delay": dt.timedelta(minutes=1),
    "email": ["santiago.rico.ramirez@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
}

pg_hook = PostgresHook(postgres_conn_id="postgres_localhost")


def load_lending_data():
    """Gets the lowest available USDT interest rates on KuCoin for each term length
    (7 days, 14 days, and 28 days) at the current point in time and uploads them to
    the PostgreSQL database"""

    request = requests.get("https://api.kucoin.com/api/v1/margin/market?currency=USDT")
    request_data = request.json()
    all_data = pd.DataFrame.from_dict(request_data["data"])
    all_data.rename(columns={"dailyIntRate": "daily_interest_rate"}, inplace=True)

    # We will just return the lowest available lending rate for each of the terms
    lending_df = all_data.groupby("term")["daily_interest_rate"].min().reset_index()

    # Getting the created_at column
    lending_df["created_at"] = dt.datetime.utcnow()
    ns5min = 5 * 60 * 1000000000
    lending_df["created_at"] = pd.to_datetime(
        ((lending_df["created_at"].astype(np.int64) // ns5min + 1) * ns5min)
    )
    lending_df["created_at"] = lending_df["created_at"].dt.to_pydatetime()

    # Reorganizing the columns
    lending_df = lending_df[["created_at", "daily_interest_rate", "term"]]

    lending_df.to_csv("lending_df.csv", index=False, sep="\t", header=False)
    pg_hook.bulk_load("kucoin_lending_rates", "lending_df.csv")


# def load_lending_data():
#     """Gets the lowest available USDT interest rates on KuCoin for each term length
#     (7 days, 14 days, and 28 days) at the current point in time and uploads them to
#     the PostgreSQL database"""

#     request = requests.get("https://api.kucoin.com/api/v1/margin/market?currency=USDT")
#     request_data = request.json()
#     all_data = pd.DataFrame.from_dict(request_data["data"])
#     all_data.rename(columns={"dailyIntRate": "daily_interest_rate"}, inplace=True)

#     # We will just return the lowest available lending rate for each of the terms
#     lending_df = all_data.groupby("term")["daily_interest_rate"].min().reset_index()

#     # Getting the created_at column
#     lending_df["created_at"] = dt.datetime.utcnow()
#     # ns5min=5*60*1000000000
#     # lending_df["created_at"] = pd.to_datetime(((lending_df["created_at"].astype(np.int64) // ns5min + 1 ) * ns5min))
#     lending_df["created_at"] = lending_df["created_at"].dt.to_pydatetime()

#     # Reorganizing the columns
#     lending_df = lending_df[["created_at", "daily_interest_rate", "term"]]


#     with tempfile.NamedTemporaryFile() as f:
#       f.write(lending_df.to_csv(index=False, sep="\t", header=False))
#       f.flush()
#       pg_hook.bulk_load("kucoin_lending_rates", f.name)

with DAG(
    dag_id="kucoin_lending_dag",
    default_args=default_args,
    description="A DAG to extract USDT lending and borrowing interest rates every 5 min",
    start_date=dt.datetime(2022, 1, 24),
    catchup=False,
    schedule_interval=dt.timedelta(minutes=5),
) as dag:
    create_lending_table = PostgresOperator(
        task_id="create_lending_table",
        postgres_conn_id="postgres_localhost",
        sql="""
        create table if not exists kucoin_lending_rates(
            created_at timestamp,
            daily_interest_rate double precision,
            term smallint,
            primary key (created_at, term)
        )
        """,
    )

    load_lending_data = PythonOperator(
        task_id="load_lending_data", python_callable=load_lending_data
    )

    # load_lending_data = PostgresOperator(
    #     task_id = "load_lending_data",
    #     postgres_conn_id="postgres_localhost",
    #     sql="""
    #     copy kucoin_lending_rates
    #     from '/var/lib/docker/volumes/airflow-docker_postgres-db-volume/_data/lending_df.csv'
    #     delimiter ',' csv;
    #     """

    # )

create_lending_table >> load_lending_data
