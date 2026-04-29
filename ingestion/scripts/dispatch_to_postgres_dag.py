"""
DAG: Dispatch → PostgreSQL Pipeline
Schedule: Daily at 6:00 AM
Description: Fetches Montgomery County police dispatch data from the Socrata JSON API
             (with pagination support), cleans it, and loads it into PostgreSQL.
             Uses temp files instead of XCom to handle large datasets.
"""

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
import pandas as pd
import logging
import os
import json

# ── CONFIG ─────────────────────────────────────────────────────────────────────
BASE_URL      = "https://data.montgomerycountymd.gov/resource/98cc-bc7d.json"
SELECT_COLS   = (
    "incident_id,cr_number,crash_reports,start_time,end_time,"
    "initial_type,close_type,longitude,latitude,"
    "police_district_number,sector,pra,geolocation"
)
WHERE_CLAUSE  = "cr_number IS NOT NULL AND start_time >= '2022-01-01T00:00:01'"
TABLE_NAME    = "uof.dispatch"
POSTGRES_CONN = "postgres_reporting"
PAGE_SIZE     = 1000
TEMP_RAW      = "/tmp/dispatch_raw.json"
TEMP_CLEAN    = "/tmp/dispatch_clean.parquet"
# ───────────────────────────────────────────────────────────────────────────────

default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def fetch_data(**context):
    all_records = []
    offset = 0

    while True:
        params = {
            "$select": SELECT_COLS,
            "$where": WHERE_CLAUSE,
            "$limit": PAGE_SIZE,
            "$offset": offset,
        }
        logging.info(f"Fetching: offset={offset}")
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()

        records = response.json()
        all_records.extend(records)
        logging.info(f"  → Got {len(records)} records (total so far: {len(all_records)})")

        if len(records) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

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

    # Serialize unhashable types (e.g. geolocation dicts) before deduplication
    for col in df.columns:
        if df[col].dtype == object:
            if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
                df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)

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
    dag_id="dispatch_to_postgres",
    default_args=default_args,
    description="Fetch Montgomery County dispatch data → clean → load to PostgreSQL (daily)",
    schedule="0 7 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dispatch", "postgres", "reporting"],
) as dag:

    t1 = PythonOperator(task_id="fetch_data", python_callable=fetch_data)
    t2 = PythonOperator(task_id="clean_data", python_callable=clean_data)
    t3 = PythonOperator(task_id="load_to_postgres", python_callable=load_to_postgres)
    t4 = TriggerDagRunOperator(
        task_id="trigger_district_join",
        trigger_dag_id="district_join_dag",
        wait_for_completion=False,
    )

    t1 >> t2 >> t3 >> t4
