from sqlalchemy import Column, Integer, String
from sqlalchemy.sql.sqltypes import DateTime, Float
from sqlalchemy.ext.declarative import declarative_base
from airflow.hooks.postgres_hook import PostgresHook


Base = declarative_base()

# Create postgres hook and engine
pg_hook = PostgresHook(postgres_conn_id="postgres_localhost")
engine = pg_hook.get_sqlalchemy_engine()


class KuCoinLendingRates(Base):
    __tablename__ = "kucoin_lending_rates"

    created_at = Column(DateTime, nullable=False, primary_key=True)
    daily_interest_rate = Column(Float, nullable=False)
    loan_duration_days = Column(Integer, nullable=False, primary_key=True)
    annualized_interest_rate = Column(Float, nullable=False)


class KuCoinActiveFutures(Base):
    __tablename__ = "kucoin_active_futures"

    symbol = Column(String, nullable=False, primary_key=True)
    first_open_date = Column(DateTime, nullable=False)
    base_currency = Column(String, nullable=False)
    quote_currency = Column(String, nullable=False)
    tick_size = Column(Float, nullable=False)
    initial_margin = Column(Float, nullable=False)
    maintain_margin = Column(Float, nullable=False)
    max_risk_limit = Column(Integer, nullable=False)
    min_risk_limit = Column(Integer, nullable=False)
    futures_maker_fee = Column(Float, nullable=False)
    futures_taker_fee = Column(Float, nullable=False)
    funding_fee_rate = Column(Float, nullable=False)
    predicted_funding_fee_rate = Column(Float, nullable=False)
    open_interest = Column(Integer, nullable=True)
    futures_dollar_volume_24h = Column(Float, nullable=True)
    futures_unit_volume_24h = Column(Float, nullable=True)
    time_to_next_funding_rate = Column(String, nullable=False)
    max_leverage = Column(Integer, nullable=False)


class KuCoinSpotPairs(Base):
    __tablename__ = "kucoin_active_spot_pairs"

    symbol = Column(String, nullable=False, primary_key=True)
    spot_unit_volume24h = Column(Float, nullable=True)
    spot_dollar_volume24h = Column(Float, nullable=True)
    spot_taker_fee = Column(Float, nullable=False)
    spot_maker_fee = Column(Float, nullable=False)
    base_currency = Column(String, nullable=False)
    quote_currency = Column(String, nullable=False)


class KuCoinFundingRates(Base):
    __tablename__ = "kucoin_funding_rates"

    symbol = Column(String, nullable=False, primary_key=True)
    funding_period = Column(String, nullable=False)
    funding_time = Column(DateTime, nullable=False, primary_key=True)
    funding_rate = Column(Float, nullable=False)


class KuCoinFundingStats(Base):
    __tablename__ = "kucoin_funding_stats"

    symbol = Column(String, nullable=False, primary_key=True)
    volume = Column(Float, nullable=True)
    predicted_funding_rate = Column(Float, nullable=False)
    funding_8h = Column(Float, nullable=True)
    funding_24h = Column(Float, nullable=True)
    funding_3d = Column(Float, nullable=True)
    funding_7d = Column(Float, nullable=True)
    funding_14d = Column(Float, nullable=True)
    funding_30d = Column(Float, nullable=True)
    funding_90d = Column(Float, nullable=True)


Base.metadata.create_all(engine)
