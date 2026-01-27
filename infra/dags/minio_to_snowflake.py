import os
import json
import boto3
import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# ---------------- CONFIG ---------------- #

# MinIO (inside docker network)
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password123"
BUCKET = "bronze-transactions"

# Local temp directory (Airflow container)
LOCAL_DIR = "/tmp/minio_downloads"

# Snowflake
SNOWFLAKE_USER = "GP17"
SNOWFLAKE_PASSWORD = "Vedpatel170111*"
SNOWFLAKE_ACCOUNT = "ekb16777"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
SNOWFLAKE_DB = "STOCKS_MDS"
SNOWFLAKE_SCHEMA = "COMMON"
SNOWFLAKE_TABLE = "BRONZE_STOCK_QUOTES_RAW"

# ---------------- TASK 1: DOWNLOAD FROM MINIO ---------------- #

def download_from_minio():
    os.makedirs(LOCAL_DIR, exist_ok=True)

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )

    objects = s3.list_objects_v2(Bucket=BUCKET).get("Contents", [])

    if not objects:
        print("No new files in MinIO.")
        return []

    local_files = []

    for obj in objects:
        key = obj["Key"]
        local_file = os.path.join(LOCAL_DIR, os.path.basename(key))

        s3.download_file(BUCKET, key, local_file)
        print(f"Downloaded {key} -> {local_file}")

        local_files.append((key, local_file))

    return local_files


# ---------------- TASK 2: LOAD TO SNOWFLAKE ---------------- #

def load_to_snowflake(**kwargs):
    files = kwargs['ti'].xcom_pull(task_ids='download_minio')

    if not files:
        print("No files to load into Snowflake.")
        return

    # Connect to Snowflake
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DB,
        schema=SNOWFLAKE_SCHEMA
    )

    cur = conn.cursor()

    # Upload each file to Snowflake table stage
    for key, file_path in files:
        cur.execute(f"PUT file://{file_path} @%{SNOWFLAKE_TABLE}")
        print(f"Uploaded {file_path} to Snowflake stage")

    # Copy into bronze table (RAW JSON)
    cur.execute(f"""
        COPY INTO {SNOWFLAKE_TABLE}
        FROM @%{SNOWFLAKE_TABLE}
        FILE_FORMAT = (TYPE = 'JSON')
    """)

    print("COPY INTO Snowflake completed")

    # Cleanup local + MinIO files (exactly-once ingestion)
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )

    for key, file_path in files:
        os.remove(file_path)
        s3.delete_object(Bucket=BUCKET, Key=key)
        print(f"Deleted processed file {key}")

    cur.close()
    conn.close()


# ---------------- DAG DEFINITION ---------------- #

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 23),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="minio_to_snowflake",
    default_args=default_args,
    schedule_interval="*/1 * * * *",   # every 1 minute
    catchup=False,
    tags=["stocks", "bronze", "snowflake"],
) as dag:

    download_task = PythonOperator(
        task_id="download_minio",
        python_callable=download_from_minio,
    )

    load_task = PythonOperator(
        task_id="load_snowflake",
        python_callable=load_to_snowflake,
    )

    download_task >> load_task
