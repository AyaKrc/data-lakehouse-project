from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys

# Add dags directory to path for imports
sys.path.insert(0, '/opt/airflow/dags')

# -------------------------------
# Task Functions
# -------------------------------
def run_step1(**context):
    """Execute Step 1: Upload raw data"""
    from lakehouse_step1_upload_raw import ensure_bucket, upload_raw
    
    print(f"\n{'='*60}")
    print(f" STEP 1: Bronze Layer - Upload Raw Data")
    print(f"{'='*60}\n")
    
    ensure_bucket()
    upload_raw()
    
    print(f" Step 1 Complete\n")

def run_step2(**context):
    """Execute Step 2: Transform to silver"""
    from lakehouse_step2_to_silver import transform_to_silver
    
    print(f"\n{'='*60}")
    print(f" STEP 2: Silver Layer - Clean & Validate")
    print(f"{'='*60}\n")
    
    transform_to_silver()
    
    print(f" Step 2 Complete\n")

def run_step3(**context):
    """Execute Step 3: Aggregate to gold"""
    from lakehouse_step3_to_gold import aggregate_to_gold
    
    print(f"\n{'='*60}")
    print(f" STEP 3: Gold Layer - Create Aggregations")
    print(f"{'='*60}\n")
    
    aggregate_to_gold()
    
    print(f" Step 3 Complete\n")

def run_step4(**context):
    """Execute Step 4: Load to Postgres"""
    from lakehouse_step4_load_postgres import load_gold_to_postgres, verify_postgres_tables
    
    print(f"\n{'='*60}")
    print(f" STEP 4: Warehouse - Load to Postgres")
    print(f"{'='*60}\n")
    
    load_gold_to_postgres()
    verify_postgres_tables()
    
    print(f" Step 4 Complete\n")

def print_pipeline_summary(**context):
    """Print final pipeline summary"""
    print(f"\n{'='*60}")
    print(f" LAKEHOUSE MASTER PIPELINE COMPLETE")
    print(f"{'='*60}")
    print(f" All 4 steps executed successfully!")
    print(f"\n Pipeline Flow:")
    print(f"   Bronze (Raw) → Silver (Cleaned) → Gold (Aggregated) → Warehouse (Postgres)")
    print(f"\n Data Locations:")
    print(f"   • MinIO: s3://test-bucket/ (raw/, silver/, gold/)")
    print(f"   • Postgres: lakehouse-postgres:5432/lakehouse")
    print(f"\n Access Points:")
    print(f"   • MinIO Console: http://localhost:9001 (minio/minio123)")
    print(f"   • Metabase: http://localhost:3000")
    print(f"   • Jupyter: http://localhost:8888")
    print(f"\n Tables in Postgres:")
    print(f"   • avg_fare_by_month_borough")
    print(f"   • borough_rankings")
    print(f"   • hourly_trip_patterns")
    print(f"   • daily_summary")
    print(f"{'='*60}\n")

# -------------------------------
# DAG Definition
# -------------------------------
with DAG(
    dag_id="lakehouse_master_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["lakehouse", "master", "orchestration", "end-to-end"],
    description="Master pipeline: Bronze → Silver → Gold → Warehouse",
) as dag:

    t_step1 = PythonOperator(
        task_id="step1_bronze_upload",
        python_callable=run_step1,
    )

    t_step2 = PythonOperator(
        task_id="step2_silver_transform",
        python_callable=run_step2,
    )

    t_step3 = PythonOperator(
        task_id="step3_gold_aggregate",
        python_callable=run_step3,
    )

    t_step4 = PythonOperator(
        task_id="step4_warehouse_load",
        python_callable=run_step4,
    )

    t_summary = PythonOperator(
        task_id="pipeline_summary",
        python_callable=print_pipeline_summary,
    )

    # Define pipeline order
    t_step1 >> t_step2 >> t_step3 >> t_step4 >> t_summary