import logging
from typing import Optional
from rich.logging import RichHandler

logging.basicConfig(level="INFO", format="%(message)s", datefmt="[%X]", handlers=[RichHandler()])
LOGGER = logging.getLogger(__name__)

import os
import pandas as pd
from snowflake.connector import SnowflakeConnection, connect
from snowflake.connector.errors import DatabaseError


def connect_to_snowflake(
    snowflake_user: str = None,
    snowflake_pass: str = None,
    warehouse: str = "HUMAN_WH",
    snowflake_acct: str = "tw61901.us-east-1",
    role: str = "EVERYONE",
) -> SnowflakeConnection:
    snowflake_user = os.getenv("SNOWFLAKE_USER")
    snowflake_pass = os.getenv("SNOWFLAKE_PASS")
    if snowflake_user and snowflake_pass:
        logging.info("Connecting to Snowflake with Standard Credentials")
        try:
            conn = connect(
                user=snowflake_user,
                password=snowflake_pass,
                account=snowflake_acct,
                warehouse=warehouse,
                role=role,
            )
            return conn
        except DatabaseError:
            logging.info("Standard Credentials Failed, Trying Browser Auth")
            logging.info("Connecting to Snowflake with Browser Auth")
            conn = connect(
                user=snowflake_user,
                account=snowflake_acct,
                warehouse=warehouse,
                authenticator="externalbrowser",
                role=role,
            )
            return conn
    else:
        raise ValueError("Missing Snowflake credentials")


def get_data_from_snowflake(query: str, conn: Optional[SnowflakeConnection] = None) -> pd.DataFrame:

    if not conn:
        conn = connect_to_snowflake()

    df = pd.read_sql(query, conn)
    df = df.rename(str.lower, axis="columns")

    return df
