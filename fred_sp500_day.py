from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Variable
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from datetime import datetime, timedelta
import requests
import pandas as pd
from google.cloud import bigquery, storage

def fetch_and_load():
    API_KEY = Variable.get("fred_api_key")
    PROJECT_ID = "rational-lambda-487313-t2"
    BUCKET_NAME = "fred-data-backup"
    TABLE_ID = f"{PROJECT_ID}.fred_data.sp500_daily"

    hook = GoogleBaseHook(gcp_conn_id="google_cloud_default")
    credentials = hook.get_credentials()

    start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    today = datetime.now().strftime("%Y-%m-%d")

    params = {
        "series_id": "SP500",
        "api_key": API_KEY,
        "file_type": "json",
        "observation_start": start_date,
        "observation_end": today,
        "sort_order": "desc",
        "limit": 5
    }

    response = requests.get(
        "https://api.stlouisfed.org/fred/series/observations",
        params=params
    )
    response.raise_for_status()

    observations = response.json().get("observations", [])

    if not observations:
        print("no data")
        return

    df = pd.DataFrame(observations)[["date", "value"]]
    df.columns = ["date", "sp500"]
    df["sp500"] = pd.to_numeric(df["sp500"], errors="coerce")
    df = df.dropna(subset=["sp500"])

    if df.empty:
        print("empty")
        return

    df["date"] = pd.to_datetime(df["date"]).dt.date

    filename = f"sp500_{today}.csv"
    df.to_csv(filename, index=False)
    storage_client = storage.Client(project=PROJECT_ID, credentials=credentials)
    storage_client.bucket(BUCKET_NAME).blob(f"daily/{filename}").upload_from_filename(filename)

    client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    client.load_table_from_dataframe(df, TABLE_ID, job_config=job_config).result()
    print(f"success {len(df)} rows")


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="fred_sp500_daily",
    default_args=default_args,
    start_date=datetime(2026, 2, 22),
    schedule="0 3 * * 1-5",
    catchup=False
) as dag:
    task = PythonOperator(
        task_id="fetch_sp500_to_bq",
        python_callable=fetch_and_load
    )
