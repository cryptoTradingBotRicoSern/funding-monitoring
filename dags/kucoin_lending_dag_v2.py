import datetime as dt
import requests

import numpy as np
import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
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


with DAG(
    dag_id="kucoin_lending_dag_v2",
    default_args=default_args,
    description="A DAG to extract USDT lending and borrowing interest rates every 5 min",
    start_date=dt.datetime(2022, 1, 24),
    catchup=False,
    schedule_interval=dt.timedelta(minutes=5),
) as dag:
    create_tables = BashOperator(
        task_id="create_tables", bash_command="python3 /opt/airflow/scripts/models.py"
    )

    append_new_lending_data = BashOperator(
        task_id="append_new_lending_data",
        bash_command="python3 /opt/airflow/scripts/lending_data_etl.py",
    )

    update_active_futures = BashOperator(
        task_id="update_active_futures",
        bash_command="python3 /opt/airflow/scripts/active_futures_etl.py",
    )

    update_active_spot_pairs = BashOperator(
        task_id="update_active_spot_pairs",
        bash_command="python3 /opt/airflow/scripts/active_spot_pairs_etl.py",
    )

    update_funding_data = BashOperator(
        task_id="update_funding_data",
        bash_command="python3 /opt/airflow/scripts/funding_rates_etl.py",
    )

    update_funding_stats = BashOperator(
        task_id="update_funding_stats",
        bash_command="python3 /opt/airflow/scripts/funding_stats_etl.py",
    )

(
    create_tables
    >> [update_active_futures, update_active_spot_pairs, append_new_lending_data]
    >> update_funding_data
    >> update_funding_stats
)
