"""
DAG: OData → PostgreSQL Pipeline
Schedule: Daily at 6:00 AM
Description: Fetches data from a public OData API (with pagination support),
             cleans it, and loads it into PostgreSQL.
             Uses temp files instead of XCom to handle large datasets.
"""

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
import pandas as pd
import logging
import os
import json

# ── CONFIG ─────────────────────────────────────────────────────────────────────
ODATA_BASE_URL = "https://data.montgomerycountymd.gov/api/odata/v4/2x7e-w8x3"
TABLE_NAME     = "uof.details"
POSTGRES_CONN  = "postgres_reporting"
PAGE_SIZE      = 1000
TEMP_RAW       = "/tmp/odata_raw.json"
TEMP_CLEAN     = "/tmp/odata_clean.parquet"
# ───────────────────────────────────────────────────────────────────────────────

default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def fetch_odata(**context):
    all_records = []
    params = {"$top": PAGE_SIZE, "$skip": 0, "$format": "json"}
    url = ODATA_BASE_URL

    while url:
        logging.info(f"Fetching: {url} | skip={params.get('$skip', 'n/a')}")
        response = requests.get(url, params=params if url == ODATA_BASE_URL else None)
        response.raise_for_status()

        data = response.json()
        records = data.get("value", [])
        all_records.extend(records)
        logging.info(f"  → Got {len(records)} records (total so far: {len(all_records)})")

        url = data.get("@odata.nextLink") or data.get("odata.nextLink")
        if not url:
            if len(records) < PAGE_SIZE:
                break
            params["$skip"] += PAGE_SIZE

    logging.info(f"Fetch complete. Total records: {len(all_records)}")

    with open(TEMP_RAW, "w") as f:
        json.dump(all_records, f)
    logging.info(f"Saved raw data to {TEMP_RAW}")


def clean_data(**context):
    with open(TEMP_RAW, "r") as f:
        raw = json.load(f)

    df = pd.DataFrame(raw)

    if df.empty:
        logging.warning("No data to clean.")
        df.to_parquet(TEMP_CLEAN, index=False)
        return

    logging.info(f"Cleaning {len(df)} records. Columns: {list(df.columns)}")

    # Normalize column names
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Drop duplicates and empty rows
    df = df.drop_duplicates()
    df = df.dropna(how="all")

    # Strip whitespace from string columns only
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].astype(str).str.strip().replace('nan', None)

    # Add pipeline metadata
    df["_row_index"] = range(1, len(df) + 1)
    df["_loaded_at"] = datetime.utcnow().isoformat()

    logging.info(f"Cleaning complete. {len(df)} records remaining.")
    df.to_parquet(TEMP_CLEAN, index=False)
    logging.info(f"Saved clean data to {TEMP_CLEAN}")


def load_to_postgres(**context):
    df = pd.read_parquet(TEMP_CLEAN)

    if df.empty:
        logging.warning("No records to load.")
        return

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN)
    engine = hook.get_sqlalchemy_engine()

    if "." in TABLE_NAME:
        schema, table = TABLE_NAME.split(".", 1)
    else:
        schema, table = "public", TABLE_NAME

    with engine.begin() as conn:
        conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        conn.execute(f'DROP TABLE IF EXISTS "{schema}"."{table}" CASCADE')
        conn.execute(f'DROP TYPE IF EXISTS "{schema}"."{table}" CASCADE')

    df.to_sql(
        name=table,
        con=engine,
        schema=schema,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=500,
    )

    logging.info(f"Loaded {len(df)} records into {TABLE_NAME}.")

    # Clean up temp files
    for f in [TEMP_RAW, TEMP_CLEAN]:
        if os.path.exists(f):
            os.remove(f)


with DAG(
    dag_id="odata_to_postgres",
    default_args=default_args,
    description="Fetch OData → clean → load to PostgreSQL (daily)",
    schedule="0 7 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["odata", "postgres", "reporting"],
) as dag:

    t1 = PythonOperator(task_id="fetch_odata", python_callable=fetch_odata)
    t2 = PythonOperator(task_id="clean_data", python_callable=clean_data)
    t3 = PythonOperator(task_id="load_to_postgres", python_callable=load_to_postgres)

    t1 >> t2 >> t3
