import pandas as pd
from google.cloud import bigquery
import os
import streamlit as st
from datetime import datetime, timedelta
import pytz
import warnings

warnings.filterwarnings("ignore", message="BigQuery Storage module not found")

if "GCP_KEYFILE_PATH" in st.secrets:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = st.secrets["GCP_KEYFILE_PATH"]

DEFAULT_TABLE = "river-treat-468823.ais_data.ais_messages"
TABLE_NAME = st.secrets.get("BQ_TABLE", os.getenv("BQ_TABLE", DEFAULT_TABLE))


def get_default_dates():
    gmt_minus_6 = pytz.timezone("America/Mexico_City")
    now = datetime.now(gmt_minus_6)
    current_2024 = now.replace(year=2024)
    start_date = (current_2024 - timedelta(days=2)).date()
    end_date = (current_2024 + timedelta(days=2)).date()
    return start_date, end_date


def run_query_df(query, params=None):
    client = bigquery.Client()
    return client.query(query).to_dataframe()


def distinct_values(column, table=None):
    if table is None:
        table = TABLE_NAME
    client = bigquery.Client()
    query = f"SELECT DISTINCT {column} FROM `{table}` ORDER BY {column}"
    return client.query(query).to_dataframe()[column].tolist()
