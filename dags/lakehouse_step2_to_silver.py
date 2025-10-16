from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import os
import pandas as pd
import json

# -------------------------------
# CONFIG
# -------------------------------
BUCKET = "test-bucket"
RAW_PREFIX = "raw"
SILVER_PREFIX = "silver"
DQ_REPORTS_PREFIX = "dq_reports"

WORKDIR = "/tmp/airflow-work"
os.makedirs(WORKDIR, exist_ok=True)

# -------------------------------
# HELPERS
# -------------------------------
def _download(hook: S3Hook, key: str, local_path: str):
    """Download a file from MinIO (S3) to local temp directory"""
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    # Download into parent dir (Airflow S3Hook expects a directory)
    tmp_file = hook.download_file(
        key=key,
        bucket_name=BUCKET,
        local_path=os.path.dirname(local_path)
    )

    # Move/rename to desired local path
    os.replace(tmp_file, local_path)
    print(f"✓ downloaded s3://{BUCKET}/{key} -> {local_path}")

def _upload_dq_report(hook: S3Hook, report_data: dict, dataset: str, month: str):
    """Upload data quality report as JSON"""
    report_path = os.path.join(WORKDIR, f"dq_{dataset}_{month}.json")
    with open(report_path, 'w') as f:
        json.dump(report_data, f, indent=2, default=str)
    
    dq_key = f"{DQ_REPORTS_PREFIX}/{dataset}/{month}_dq_report.json"
    hook.load_file(report_path, key=dq_key, bucket_name=BUCKET, replace=True)
    print(f"✓ uploaded DQ report -> s3://{BUCKET}/{dq_key}")

# -------------------------------
# DATA QUALITY CHECKS
# -------------------------------
def run_data_quality_checks(df: pd.DataFrame, dataset: str, month: str) -> dict:
    """Run data quality checks and return report"""
    results = {
        "dataset": dataset,
        "month": month,
        "timestamp": datetime.now().isoformat(),
        "row_count": len(df),
        "checks": []
    }
    
    # Check 1: Row count validation
    if len(df) < 100:
        results["checks"].append({
            "check": "row_count",
            "status": "WARNING",
            "message": f"Low row count: {len(df)} rows"
        })
    else:
        results["checks"].append({
            "check": "row_count",
            "status": "PASS",
            "message": f"Row count OK: {len(df)} rows"
        })
    
    # Check 2: Null values in critical columns
    critical_cols = ['fare_amount', 'trip_distance']
    for col in critical_cols:
        if col in df.columns:
            null_count = df[col].isnull().sum()
            null_pct = (null_count / len(df)) * 100
            
            if null_pct > 5:
                results["checks"].append({
                    "check": f"null_check_{col}",
                    "status": "WARNING",
                    "message": f"High null percentage in {col}: {null_pct:.2f}%"
                })
            else:
                results["checks"].append({
                    "check": f"null_check_{col}",
                    "status": "PASS",
                    "message": f"Null percentage in {col}: {null_pct:.2f}%"
                })
    
    # Check 3: Fare amount range
    if 'fare_amount' in df.columns:
        avg_fare = df['fare_amount'].mean()
        max_fare = df['fare_amount'].max()
        min_fare = df['fare_amount'].min()
        
        if avg_fare < 0 or avg_fare > 500:
            results["checks"].append({
                "check": "fare_range",
                "status": "ERROR",
                "message": f"Unusual avg fare: ${avg_fare:.2f}"
            })
        else:
            results["checks"].append({
                "check": "fare_range",
                "status": "PASS",
                "message": f"Avg fare: ${avg_fare:.2f}, Min: ${min_fare:.2f}, Max: ${max_fare:.2f}"
            })
    
    # Check 4: Trip distance range
    if 'trip_distance' in df.columns:
        avg_distance = df['trip_distance'].mean()
        max_distance = df['trip_distance'].max()
        
        if max_distance > 200:
            results["checks"].append({
                "check": "distance_range",
                "status": "WARNING",
                "message": f"Very long trip detected: {max_distance:.2f} miles"
            })
        else:
            results["checks"].append({
                "check": "distance_range",
                "status": "PASS",
                "message": f"Avg distance: {avg_distance:.2f} miles, Max: {max_distance:.2f} miles"
            })
    
    # Check 5: Trip duration validation
    if 'trip_minutes' in df.columns:
        avg_duration = df['trip_minutes'].mean()
        max_duration = df['trip_minutes'].max()
        
        # Filter out negative durations
        negative_count = (df['trip_minutes'] < 0).sum()
        if negative_count > 0:
            results["checks"].append({
                "check": "negative_duration",
                "status": "WARNING",
                "message": f"Found {negative_count} trips with negative duration"
            })
        else:
            results["checks"].append({
                "check": "trip_duration",
                "status": "PASS",
                "message": f"Avg duration: {avg_duration:.2f} min, Max: {max_duration:.2f} min"
            })
    
    return results

# -------------------------------
# MAIN TRANSFORM
# -------------------------------
def transform_to_silver():
    hook = S3Hook(aws_conn_id="minio_conn")

    # Download reference zones (lookup table)
    print(f"\n{'='*60}")
    print(f" Downloading reference data")
    print(f"{'='*60}\n")
    
    zones_key = f"{RAW_PREFIX}/ref/taxi_zone_lookup.csv"
    zones_local = os.path.join(WORKDIR, "taxi_zone_lookup.csv")
    _download(hook, zones_key, zones_local)
    zones = pd.read_csv(zones_local)
    
    print(f"✓ Loaded {len(zones)} taxi zones\n")

    def process(dataset: str, month: str):
        print(f"\n{'='*60}")
        print(f" Processing {dataset.upper()} - {month}")
        print(f"{'='*60}\n")
        
        # --- Download raw parquet ---
        raw_key = f"{RAW_PREFIX}/{dataset}/{month}.parquet"
        raw_local = os.path.join(WORKDIR, f"{dataset}_{month}.parquet")
        _download(hook, raw_key, raw_local)

        df = pd.read_parquet(raw_local)
        initial_count = len(df)
        print(f" Loaded {initial_count:,} raw records")

        # --- Basic cleaning ---
        date_cols = [c for c in df.columns if "pickup_datetime" in c.lower() or "tpep_pickup_datetime" in c.lower() or "lpep_pickup_datetime" in c.lower()]
        drop_cols = [c for c in df.columns if "dropoff_datetime" in c.lower() or "tpep_dropoff_datetime" in c.lower() or "lpep_dropoff_datetime" in c.lower()]

        pickup_col = date_cols[0] if date_cols else None
        dropoff_col = drop_cols[0] if drop_cols else None

        if pickup_col and dropoff_col:
            df[pickup_col] = pd.to_datetime(df[pickup_col], errors="coerce")
            df[dropoff_col] = pd.to_datetime(df[dropoff_col], errors="coerce")
            df["trip_minutes"] = (df[dropoff_col] - df[pickup_col]).dt.total_seconds() / 60.0
            df["pickup_date"] = df[pickup_col].dt.date
            df["pickup_month"] = df[pickup_col].dt.to_period("M").astype(str)
            df["pickup_hour"] = df[pickup_col].dt.hour
            df["pickup_dayofweek"] = df[pickup_col].dt.dayofweek
            print(f"✓ Parsed datetime columns and created derived fields")

        # --- Filters (remove negative fare/trips if present) ---
        for col in ["fare_amount", "trip_distance", "total_amount"]:
            if col in df.columns:
                before = len(df)
                df = df[df[col].fillna(0) >= 0]
                filtered = before - len(df)
                if filtered > 0:
                    print(f" Filtered {filtered} records with negative {col}")

        # --- Join with taxi zones ---
        puid = None
        for cand in ["PULocationID", "pulocationid", "pickup_location_id"]:
            if cand in df.columns:
                puid = cand
                break

        if puid and "LocationID" in zones.columns:
            before_join = len(df)
            df = df.merge(
                zones[["LocationID", "Borough", "Zone"]],
                left_on=puid,
                right_on="LocationID",
                how="left",
            ).rename(columns={"Borough": "pickup_borough", "Zone": "pickup_zone"})
            print(f"✓ Joined with zone lookup (matched {before_join} records)")
            
            # Check how many didn't match
            unmatched = df['pickup_borough'].isnull().sum()
            if unmatched > 0:
                print(f"⚠ {unmatched} records without matching zone")

        final_count = len(df)
        filtered_total = initial_count - final_count
        print(f"\n Data Transformation Summary:")
        print(f"   Initial records: {initial_count:,}")
        print(f"   Final records: {final_count:,}")
        print(f"   Filtered out: {filtered_total:,} ({(filtered_total/initial_count*100):.2f}%)")

        # --- Run Data Quality Checks ---
        print(f"\n Running data quality checks...")
        dq_results = run_data_quality_checks(df, dataset, month)
        _upload_dq_report(hook, dq_results, dataset, month)
        
        # Print DQ summary
        print(f"\n Data Quality Report:")
        for check in dq_results["checks"]:
            status_emoji = "✅" if check["status"] == "PASS" else "⚠️" if check["status"] == "WARNING" else "❌"
            print(f"  {status_emoji} [{check['status']}] {check['check']}: {check['message']}")

        # --- Write silver parquet & upload ---
        out_local = os.path.join(WORKDIR, f"silver_{dataset}_{month}.parquet")
        df.to_parquet(out_local, index=False)

        silver_key = f"{SILVER_PREFIX}/{dataset}/{month}.parquet"
        hook.load_file(out_local, key=silver_key, bucket_name=BUCKET, replace=True)
        
        file_size_mb = os.path.getsize(out_local) / (1024 * 1024)
        print(f"\n Wrote silver layer:")
        print(f"   → s3://{BUCKET}/{silver_key}")
        print(f"   → {final_count:,} rows")
        print(f"   → {file_size_mb:.2f} MB")

    # Run for each dataset/month
    print(f"\n{'='*60}")
    print(f" Starting Silver Layer Transformation")
    print(f"{'='*60}")
    
    datasets_config = {
        "green": ["2022-01", "2022-08"],
        "yellow": ["2022-01", "2022-08"],
    }
    
    total_datasets = sum(len(months) for months in datasets_config.values())
    processed = 0
    failed = 0
    
    for dataset, months in datasets_config.items():
        for m in months:
            try:
                process(dataset, m)
                processed += 1
            except Exception as e:
                failed += 1
                print(f"\n Error processing {dataset} {m}: {e}")
                print(f"   Continuing with next dataset...\n")
    
    # Final summary
    print(f"\n{'='*60}")
    print(f" SILVER LAYER TRANSFORMATION COMPLETE")
    print(f"{'='*60}")
    print(f" Successfully processed: {processed}/{total_datasets}")
    print(f" Failed: {failed}/{total_datasets}")
    print(f"{'='*60}\n")
    
    if failed > 0:
        raise Exception(f"{failed} dataset(s) failed to process")

# -------------------------------
# DAG DEFINITION
# -------------------------------
with DAG(
    dag_id="lakehouse_step2_to_silver",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["lakehouse", "silver", "data-quality"],
    description="Transform raw data to silver layer with data quality checks",
) as dag:
    PythonOperator(
        task_id="to_silver",
        python_callable=transform_to_silver
    )