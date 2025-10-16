from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

def upload_to_minio():
    hook = S3Hook(aws_conn_id="minio_conn")
    bucket_name = "test-bucket"
    file_key = "hello.txt"
    local_file = "/opt/airflow/dags/data/hello.txt"

    # ensure directory exists
    os.makedirs(os.path.dirname(local_file), exist_ok=True)

    # create test file
    with open(local_file, "w") as f:
        f.write("Hello from Airflow to MinIO!")

    # upload file to MinIO
    hook.load_file(
        filename=local_file,
        key=file_key,
        bucket_name=bucket_name,
        replace=True
    )
    print(f"Uploaded {file_key} to bucket {bucket_name}")

with DAG(
    dag_id="minio_test_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    upload_task = PythonOperator(
        task_id="upload_file",
        python_callable=upload_to_minio,
    )
