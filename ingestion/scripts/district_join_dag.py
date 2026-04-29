"""
DAG: District Spatial Join
Schedule: None (triggered by dispatch_to_postgres after each load)
Description: Reads uof.dispatch, performs a point-in-polygon spatial join against
             the police districts shapefile, and writes results to uof.dispatch_geo
             with an object_id column matching the shapefile's OBJECTID field.
             This allows Tableau to join the shapefile to the data by object_id.
             Optionally exports the latest results to a fixed Google Sheets tab,
             replacing that tab's contents on each run.
"""

from airflow.decorators import dag, task
from datetime import datetime
import os

SHAPEFILE_PATH = "/usr/local/airflow/include/Police_District/Police_District.shp"
SOURCE_TABLE = "uof.dispatch"
TARGET_SCHEMA = "uof"
TARGET_TABLE = "dispatch_geo"
POSTGRES_CONN = "postgres_reporting"
GOOGLE_SHEET_ID_VAR = "district_join_google_sheet_id"
GOOGLE_WORKSHEET_VAR = "district_join_google_worksheet"
GOOGLE_SERVICE_ACCOUNT_PATH_ENV = "GOOGLE_SERVICE_ACCOUNT_PATH"
GOOGLE_SHEETS_EXCLUDED_COLUMNS = {
    "incident_id",
    "end_time",
    "initial_type",
    "pra",
    "PRA",
    "crash_reports",
    "_loaded_at",
    "_row_index"
}


@dag(
    dag_id="district_join_dag",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dispatch", "geo", "reporting"],
)
def district_join_dag():
    @task(retries=2)
    def spatial_join():
        import geopandas as gpd
        import pandas as pd
        import logging
        from sqlalchemy import Integer, text
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN)
        engine = hook.get_sqlalchemy_engine()

        logging.info(f"Reading {SOURCE_TABLE} from PostgreSQL...")
        df = pd.read_sql(f"SELECT * FROM {SOURCE_TABLE}", engine)
        logging.info(f"  → {len(df)} rows loaded")

        # Coerce lat/lon to numeric
        df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
        df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")

        valid = df.dropna(subset=["longitude", "latitude"]).copy()
        invalid = df[df["longitude"].isna() | df["latitude"].isna()].copy()
        logging.info(f"  → {len(valid)} rows with valid coordinates, {len(invalid)} without")

        # Build GeoDataFrame from valid coordinates
        gdf = gpd.GeoDataFrame(
            valid,
            geometry=gpd.points_from_xy(valid["longitude"], valid["latitude"]),
            crs="EPSG:4326",
        )

        logging.info(f"Loading shapefile: {SHAPEFILE_PATH}")
        districts = gpd.read_file(SHAPEFILE_PATH).to_crs("EPSG:4326")
        logging.info(f"  → {len(districts)} district polygons loaded")

        # Point-in-polygon join — left join keeps all dispatch rows
        joined = gpd.sjoin(
            gdf,
            districts[["OBJECTID", "geometry"]],
            how="left",
            predicate="within",
        )
        joined = joined.rename(columns={"OBJECTID": "object_id"})
        joined = joined.drop(columns=["geometry", "index_right"], errors="ignore")

        # Re-attach rows with no coordinates (object_id will be null)
        invalid["object_id"] = None
        result = pd.concat([joined, invalid], ignore_index=True)
        result["object_id"] = pd.to_numeric(result["object_id"], errors="coerce").astype("Int64")

        matched = result["object_id"].notna().sum()
        logging.info(f"Spatial join complete. {matched}/{len(result)} rows matched to a district.")

        # Write to postgres, replacing each run
        logging.info(f"Writing to {TARGET_SCHEMA}.{TARGET_TABLE}...")
        with engine.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{TARGET_SCHEMA}"'))
            conn.execute(text(f'DROP TABLE IF EXISTS "{TARGET_SCHEMA}"."{TARGET_TABLE}" CASCADE'))
        result.to_sql(
            TARGET_TABLE,
            engine,
            schema=TARGET_SCHEMA,
            if_exists="append",
            index=False,
            dtype={"object_id": Integer()},
        )
        logging.info(f"Done. {len(result)} rows written to {TARGET_SCHEMA}.{TARGET_TABLE}.")

    @task(retries=2)
    def export_to_google_sheet():
        import logging
        import pandas as pd
        import gspread
        from airflow.exceptions import AirflowSkipException
        from airflow.models import Variable
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from gspread.exceptions import WorksheetNotFound
        from gspread.utils import rowcol_to_a1

        spreadsheet_id = Variable.get(GOOGLE_SHEET_ID_VAR, default_var=None)
        worksheet_name = Variable.get(GOOGLE_WORKSHEET_VAR, default_var=TARGET_TABLE)
        service_account_path = os.getenv(GOOGLE_SERVICE_ACCOUNT_PATH_ENV)

        if not spreadsheet_id:
            raise AirflowSkipException(
                f"Skipping Google Sheets export because Airflow Variable "
                f"'{GOOGLE_SHEET_ID_VAR}' is not set."
            )

        if not service_account_path:
            raise AirflowSkipException(
                f"Skipping Google Sheets export because env var "
                f"'{GOOGLE_SERVICE_ACCOUNT_PATH_ENV}' is not set."
            )

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN)
        engine = hook.get_sqlalchemy_engine()
        query = f"""
            SELECT *
            FROM "{TARGET_SCHEMA}"."{TARGET_TABLE}"
            WHERE object_id IS NOT NULL
              AND TRIM(CAST(object_id AS TEXT)) <> ''
        """

        logging.info(f"Loading {TARGET_SCHEMA}.{TARGET_TABLE} for Google Sheets export...")
        df = pd.read_sql(query, engine)
        logging.info(f"  -> {len(df)} rows loaded for export")

        export_columns = [
            column for column in df.columns if column not in GOOGLE_SHEETS_EXCLUDED_COLUMNS
        ]
        df = df[export_columns]
        logging.info(
            f"  -> Exporting {len(export_columns)} columns after excluding "
            f"{sorted(GOOGLE_SHEETS_EXCLUDED_COLUMNS)}"
        )

        # Convert values to Sheets-friendly strings while keeping blanks empty.
        export_df = df.astype(object).where(pd.notnull(df), "")
        export_df = export_df.applymap(
            lambda value: value.isoformat() if hasattr(value, "isoformat") else value
        )

        client = gspread.service_account(filename=service_account_path)
        spreadsheet = client.open_by_key(spreadsheet_id)

        try:
            worksheet = spreadsheet.worksheet(worksheet_name)
        except WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(
                title=worksheet_name,
                rows=max(len(export_df) + 1, 1000),
                cols=max(len(export_df.columns), 26),
            )

        values = [export_df.columns.tolist()] + export_df.values.tolist()

        logging.info(
            f"Replacing data in Google Sheet '{worksheet_name}' "
            f"({len(values) - 1} data rows)."
        )
        worksheet.clear()

        if not values:
            logging.info("No rows to export after building payload.")
            return

        chunk_size = 5000
        total_rows = len(values)
        total_cols = max(len(values[0]), 1)
        worksheet.resize(rows=max(total_rows, 1), cols=total_cols)

        for start in range(0, total_rows, chunk_size):
            chunk = values[start:start + chunk_size]
            end_row = start + len(chunk)
            end_col = max(len(chunk[0]), 1)
            target_range = f"A{start + 1}:{rowcol_to_a1(end_row, end_col)}"
            worksheet.update(target_range, chunk, value_input_option="RAW")

        logging.info(
            f"Google Sheets export complete: {len(export_df)} rows written to "
            f"worksheet '{worksheet_name}'."
        )

    join_complete = spatial_join()
    join_complete >> export_to_google_sheet()


district_join_dag()
