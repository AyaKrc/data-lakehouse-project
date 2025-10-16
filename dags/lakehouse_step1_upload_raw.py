from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import os

# -------------------------------
# CONFIG
# -------------------------------
BUCKET = "test-bucket"
RAW_PREFIX = "raw"
LOCAL_DATA_DIR = "/opt/airflow/dags/data"

RAW_FILES = [
    ("green_tripdata_2022-01.parquet", f"{RAW_PREFIX}/green/2022-01.parquet"),
    ("green_tripdata_2022-08.parquet", f"{RAW_PREFIX}/green/2022-08.parquet"),
    ("yellow_tripdata_2022-01.parquet", f"{RAW_PREFIX}/yellow/2022-01.parquet"),
    ("yellow_tripdata_2022-08.parquet", f"{RAW_PREFIX}/yellow/2022-08.parquet"),
    ("taxi_zone_lookup.csv", f"{RAW_PREFIX}/ref/taxi_zone_lookup.csv"),
]

# -------------------------------
# TASKS
# -------------------------------
def ensure_bucket():
    """Create MinIO bucket if it doesn't exist"""
    hook = S3Hook(aws_conn_id="minio_conn")
    client = hook.get_conn()
    
    try:
        # Check if bucket exists
        client.head_bucket(Bucket=BUCKET)
        print(f"✓ Bucket '{BUCKET}' already exists")
    except:
        # Bucket doesn't exist, create it
        try:
            client.create_bucket(Bucket=BUCKET)
            print(f"✓ Created bucket '{BUCKET}'")
        except Exception as e:
            print(f"⚠ Error creating bucket: {e}")
            raise

def upload_raw():
    """Upload raw data files to MinIO/S3"""
    hook = S3Hook(aws_conn_id="minio_conn")
    
    uploaded = 0
    failed = 0
    
    print(f"\n{'='*60}")
    print(f"Starting upload of {len(RAW_FILES)} files to MinIO")
    print(f"{'='*60}\n")
    
    for local_name, key in RAW_FILES:
        local_path = os.path.join(LOCAL_DATA_DIR, local_name)
        
        # Check if file exists
        if not os.path.exists(local_path):
            print(f" File not found: {local_path}")
            failed += 1
            continue
        
        # Get file size
        file_size_mb = os.path.getsize(local_path) / (1024 * 1024)
        
        try:
            # Upload to MinIO
            hook.load_file(
                filename=local_path, 
                key=key, 
                bucket_name=BUCKET, 
                replace=True
            )
            print(f" Uploaded: {local_name}")
            print(f"   → s3://{BUCKET}/{key}")
            print(f"   → Size: {file_size_mb:.2f} MB\n")
            uploaded += 1
            
        except Exception as e:
            print(f" Failed to upload {local_name}: {e}\n")
            failed += 1
    
    # Summary
    print(f"\n{'='*60}")
    print(f" UPLOAD SUMMARY")
    print(f"{'='*60}")
    print(f" Successfully uploaded: {uploaded}/{len(RAW_FILES)}")
    print(f" Failed: {failed}/{len(RAW_FILES)}")
    print(f"{'='*60}\n")
    
    # Raise error if any failed
    if failed > 0:
        raise Exception(f"{failed} file(s) failed to upload")

# -------------------------------
# DAG DEFINITION
# -------------------------------
with DAG(
    dag_id="lakehouse_step1_upload_raw",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["lakehouse", "bronze", "ingestion"],
    description="Upload raw data files to MinIO (Bronze layer)",
) as dag:
    
    t_bucket = PythonOperator(
        task_id="ensure_bucket",
        python_callable=ensure_bucket
    )
    
    t_upload = PythonOperator(
        task_id="upload_raw_files",
        python_callable=upload_raw
    )

    t_bucket >> t_upload