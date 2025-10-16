from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import os
import pandas as pd

# -----------------------------
# CONFIG
# -----------------------------
BUCKET = "test-bucket"
GOLD_PREFIX = "gold"
WORKDIR = "/tmp/airflow-work-pg"

# Define all gold tables to load
GOLD_TABLES = [
    {
        "file": "avg_fare_by_month_borough.csv",
        "table": "avg_fare_by_month_borough",
        "format": "csv"
    },
    {
        "file": "borough_rankings.csv",
        "table": "borough_rankings",
        "format": "csv"
    },
    {
        "file": "hourly_trip_patterns.parquet",
        "table": "hourly_trip_patterns",
        "format": "parquet"
    },
    {
        "file": "daily_summary.parquet",
        "table": "daily_summary",
        "format": "parquet"
    }
]

# -----------------------------
# TASK: Load Gold -> Postgres
# -----------------------------
def load_gold_to_postgres():
    """Load all gold layer tables from MinIO to Postgres"""
    
    hook_s3 = S3Hook(aws_conn_id="minio_conn")
    hook_pg = PostgresHook(postgres_conn_id="lakehouse_postgres")

    os.makedirs(WORKDIR, exist_ok=True)
    
    print(f"\n{'='*60}")
    print(f"🚀 Starting Gold Layer → Postgres Load")
    print(f"{'='*60}\n")
    
    loaded_tables = []
    failed_tables = []
    
    for table_config in GOLD_TABLES:
        table_file = table_config["file"]
        table_name = table_config["table"]
        file_format = table_config["format"]
        
        try:
            print(f"\n{'='*60}")
            print(f"📥 Loading: {table_name}")
            print(f"{'='*60}")
            
            # Download from MinIO
            gold_key = f"{GOLD_PREFIX}/{table_file}"
            tmp_dir = os.path.join(WORKDIR, "downloads")
            os.makedirs(tmp_dir, exist_ok=True)

            print(f"📦 Downloading from MinIO...")
            print(f"   Key: s3://{BUCKET}/{gold_key}")
            
            tmp_file = hook_s3.download_file(
                key=gold_key,
                bucket_name=BUCKET,
                local_path=tmp_dir
            )

            # Final file path
            local_path = os.path.join(WORKDIR, table_file)
            os.replace(tmp_file, local_path)
            
            file_size_mb = os.path.getsize(local_path) / (1024 * 1024)
            print(f"✓ Downloaded: {file_size_mb:.2f} MB")

            # Load into pandas based on format
            print(f"📊 Reading {file_format.upper()} file...")
            if file_format == 'csv':
                df = pd.read_csv(local_path)
            elif file_format == 'parquet':
                df = pd.read_parquet(local_path)
            else:
                raise ValueError(f"Unsupported format: {file_format}")
            
            print(f"✓ Loaded {len(df):,} rows, {len(df.columns)} columns")
            print(f"   Columns: {', '.join(df.columns.tolist()[:5])}{'...' if len(df.columns) > 5 else ''}")
            
            # Check for data quality
            null_counts = df.isnull().sum()
            if null_counts.sum() > 0:
                print(f"⚠ Warning: Found null values:")
                for col, count in null_counts[null_counts > 0].items():
                    print(f"   - {col}: {count} nulls ({count/len(df)*100:.1f}%)")

            # Push to Postgres
            print(f"💾 Writing to Postgres table '{table_name}'...")
            engine = hook_pg.get_sqlalchemy_engine()
            df.to_sql(
                table_name, 
                engine, 
                if_exists="replace", 
                index=False,
                method='multi',  # Faster bulk insert
                chunksize=1000
            )

            # Verify the load
            verify_query = f"SELECT COUNT(*) FROM {table_name};"
            result = hook_pg.get_first(verify_query)
            pg_count = result[0] if result else 0
            
            if pg_count == len(df):
                print(f"✅ Successfully loaded {pg_count:,} rows to Postgres")
                loaded_tables.append({
                    "table": table_name,
                    "rows": pg_count,
                    "columns": len(df.columns)
                })
            else:
                print(f"⚠ Row count mismatch! CSV: {len(df)}, Postgres: {pg_count}")
                failed_tables.append({
                    "table": table_name,
                    "error": "Row count mismatch"
                })
            
        except Exception as e:
            print(f"❌ Error loading {table_name}: {e}")
            failed_tables.append({
                "table": table_name,
                "error": str(e)
            })
            # Continue with other tables
            continue
    
    # Final summary
    print(f"\n{'='*60}")
    print(f"📊 LOAD SUMMARY")
    print(f"{'='*60}")
    print(f"✅ Successfully loaded: {len(loaded_tables)}/{len(GOLD_TABLES)} tables")
    
    if loaded_tables:
        print(f"\n✅ Loaded Tables:")
        for tbl in loaded_tables:
            print(f"   - {tbl['table']}: {tbl['rows']:,} rows, {tbl['columns']} columns")
    
    if failed_tables:
        print(f"\n❌ Failed Tables:")
        for tbl in failed_tables:
            print(f"   - {tbl['table']}: {tbl['error']}")
    
    print(f"\n📍 Database: lakehouse")
    print(f"   Host: lakehouse-postgres:5432")
    print(f"   User: lakeuser")
    print(f"{'='*60}\n")
    
    # Raise error if any table failed
    if failed_tables:
        raise Exception(f"{len(failed_tables)} table(s) failed to load")
    
    return {
        "loaded": len(loaded_tables),
        "failed": len(failed_tables),
        "tables": [t["table"] for t in loaded_tables]
    }

# -----------------------------
# TASK: Verify Tables
# -----------------------------
def verify_postgres_tables():
    """Verify all tables were loaded correctly"""
    
    hook_pg = PostgresHook(postgres_conn_id="lakehouse_postgres")
    
    print(f"\n{'='*60}")
    print(f"🔍 Verifying Postgres Tables")
    print(f"{'='*60}\n")
    
    for table_config in GOLD_TABLES:
        table_name = table_config["table"]
        
        try:
            # Check if table exists
            check_query = f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = '{table_name}'
                );
            """
            exists = hook_pg.get_first(check_query)[0]
            
            if not exists:
                print(f"❌ Table '{table_name}' does not exist")
                continue
            
            # Get row count
            count_query = f"SELECT COUNT(*) FROM {table_name};"
            row_count = hook_pg.get_first(count_query)[0]
            
            # Get column info
            columns_query = f"""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = '{table_name}'
                ORDER BY ordinal_position;
            """
            columns = hook_pg.get_records(columns_query)
            
            print(f"✅ {table_name}:")
            print(f"   Rows: {row_count:,}")
            print(f"   Columns: {len(columns)}")
            print(f"   Schema: {', '.join([f'{col[0]}({col[1]})' for col in columns[:3]])}...")
            
            # Sample data
            sample_query = f"SELECT * FROM {table_name} LIMIT 3;"
            samples = hook_pg.get_records(sample_query)
            if samples:
                print(f"   Sample rows: {len(samples)}")
            print()
            
        except Exception as e:
            print(f"❌ Error verifying {table_name}: {e}\n")
    
    print(f"{'='*60}\n")

# -----------------------------
# DAG Definition
# -----------------------------
with DAG(
    dag_id="lakehouse_step4_load_postgres",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["lakehouse", "postgres", "warehouse"],
    description="Load all gold layer tables from MinIO to Postgres warehouse",
) as dag:
    
    t_load = PythonOperator(
        task_id="load_postgres",
        python_callable=load_gold_to_postgres,
    )
    
    t_verify = PythonOperator(
        task_id="verify_tables",
        python_callable=verify_postgres_tables,
    )
    
    t_load >> t_verify