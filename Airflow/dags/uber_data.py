# uber_etl_dag.py
import os
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import storage

# Paths
CSV_FILE = os.path.join(os.getcwd(), "data", "uber.csv")  # CSV inside dags folder
TRANSFORMED_DIR = os.path.join(os.getcwd(), "data", "data")
TRANSFORMED_FILE = os.path.join(TRANSFORMED_DIR, "uber_transformed.csv")

# GCS Settings
GCS_BUCKET = "uber_data_etl"  # replace with your bucket
GCS_FILE_NAME = "uber_cleaned.csv"
CONTENT_TYPE = "text/csv"

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 8, 12),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="uber_etl_dag",
    default_args=default_args,
    description="ETL Uber rides and upload to GCS",
    schedule="@daily",
    catchup=False,
    tags=["etl", "uber"],
)


# -------------------------
# TASKS
# -------------------------
def extract_data(**kwargs):
    if not os.path.exists(CSV_FILE):
        raise FileNotFoundError(f"{CSV_FILE} not found!")

    df = pd.read_csv(CSV_FILE)
    os.makedirs(TRANSFORMED_DIR, exist_ok=True)
    kwargs["ti"].xcom_push(key="raw_data", value=CSV_FILE)
    print(f"Extracted {len(df)} rows from {CSV_FILE}")
    return "Extraction complete"


def transform_data(**kwargs):
    ti = kwargs["ti"]
    raw_file = ti.xcom_pull(key="raw_data", task_ids="extract_uber_data")  # ✅ fixed

    if not raw_file or not os.path.exists(raw_file):
        raise FileNotFoundError(f"{raw_file} not found for transformation!")

    df = pd.read_csv(raw_file)

    drop_cols = [
        "Customer Rating",
        "Avg VTAT",
        "Avg CTAT",
        "Cancelled Rides by Driver",
        "Driver Cancellation Reason",
        "Incomplete Rides",
        "Incomplete Rides Reason",
        "Cancelled Rides by Customer",
        "Reason for cancelling by Customer",
    ]
    df = df.drop(columns=drop_cols, errors="ignore")

    # Fill missing values
    df["Booking Value"] = df["Booking Value"].fillna(df["Booking Value"].median())
    df["Ride Distance"] = df["Ride Distance"].fillna(df["Ride Distance"].median())
    df["Driver Ratings"] = df["Driver Ratings"].fillna(df["Driver Ratings"].median())
    df["Payment Method"] = df["Payment Method"].fillna(df["Payment Method"].mode()[0])

    df.columns = df.columns.str.replace(" ", "_")
    df.to_csv(TRANSFORMED_FILE, index=False)

    ti.xcom_push(key="transformed_file", value=TRANSFORMED_FILE)
    print(f"Transformed data saved to {TRANSFORMED_FILE}")
    return "Transformation complete"


def load_to_gcs(**kwargs):
    ti = kwargs["ti"]
    transformed_file = ti.xcom_pull(
        key="transformed_file", task_ids="transform_uber_data"
    )  # ✅ explicitly pull

    if not transformed_file or not os.path.exists(transformed_file):
        raise FileNotFoundError(f"{transformed_file} not found for upload!")

    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(GCS_FILE_NAME)
    blob.upload_from_filename(transformed_file, content_type=CONTENT_TYPE)
    print(f"Uploaded {GCS_FILE_NAME} to gs://{GCS_BUCKET}/")
    return "Upload complete"


# -------------------------
# AIRFLOW TASK OPERATORS
# -------------------------
extract_task = PythonOperator(
    task_id="extract_uber_data", python_callable=extract_data, dag=dag
)

transform_task = PythonOperator(
    task_id="transform_uber_data", python_callable=transform_data, dag=dag
)

load_task = PythonOperator(
    task_id="load_uber_to_gcs", python_callable=load_to_gcs, dag=dag
)

# DAG dependencies
extract_task >> transform_task >> load_task
