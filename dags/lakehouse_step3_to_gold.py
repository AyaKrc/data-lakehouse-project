from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import os
import pandas as pd

# -------------------------------
# CONFIG
# -------------------------------
BUCKET = "test-bucket"
SILVER_PREFIX = "silver"
GOLD_PREFIX = "gold"
WORKDIR = "/tmp/airflow-work-gold"
os.makedirs(WORKDIR, exist_ok=True)

# -------------------------------
# HELPERS
# -------------------------------
def _download(hook: S3Hook, key: str, local_path: str):
    """Download a file from MinIO (S3) to a specific local file path"""
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    # download_file expects a directory
    tmp_file = hook.download_file(
        key=key,
        bucket_name=BUCKET,
        local_path=os.path.dirname(local_path)
    )

    os.replace(tmp_file, local_path)
    print(f"✓ downloaded s3://{BUCKET}/{key} -> {local_path}")
    return local_path

# -------------------------------
# MAIN AGGREGATION
# -------------------------------
def aggregate_to_gold():
    """Create multiple gold layer aggregations"""
    hook = S3Hook(aws_conn_id="minio_conn")
    frames = []

    def read_silver(dataset, month):
        key = f"{SILVER_PREFIX}/{dataset}/{month}.parquet"
        local_path = os.path.join(WORKDIR, f"{dataset}_{month}.parquet")
        return pd.read_parquet(_download(hook, key, local_path))

    # Read all silver data
    print(f"\n{'='*60}")
    print(f"📥 Reading silver layer data")
    print(f"{'='*60}\n")
    
    datasets_config = {
        "green":  ["2022-01", "2022-08"],
        "yellow": ["2022-01", "2022-08"],
    }
    
    for dataset, months in datasets_config.items():
        for m in months:
            try:
                df = read_silver(dataset, m)
                df['dataset'] = dataset  # Add source identifier
                frames.append(df)
                print(f"  ✓ Loaded {dataset} {m}: {len(df):,} rows")
            except Exception as e:
                print(f"  ⚠ Skip {dataset} {m}: {e}")

    if not frames:
        raise RuntimeError("No silver data found")

    df = pd.concat(frames, ignore_index=True)
    print(f"\n📊 Total records combined: {len(df):,}")
    print(f"   Columns available: {len(df.columns)}")
    print(f"   Date range: {df['pickup_date'].min() if 'pickup_date' in df.columns else 'N/A'} to {df['pickup_date'].max() if 'pickup_date' in df.columns else 'N/A'}")

    # Validate expected columns
    month_col = "pickup_month" if "pickup_month" in df.columns else None
    borough_col = "pickup_borough" if "pickup_borough" in df.columns else None
    fare_col = "fare_amount" if "fare_amount" in df.columns else None

    if not all([month_col, borough_col, fare_col]):
        raise RuntimeError(f"Expected columns missing. Found: {df.columns.tolist()}")

    # ============================
    # AGG 1: Average Fare by Month & Borough
    # ============================
    print(f"\n{'='*60}")
    print(f"📈 Creating Aggregation 1: avg_fare_by_month_borough")
    print(f"{'='*60}")
    
    agg1 = (
        df.groupby([month_col, borough_col, 'dataset'], dropna=False)[fare_col]
          .agg(['mean', 'median', 'count'])
          .reset_index()
          .rename(columns={
              'mean': 'avg_fare',
              'median': 'median_fare',
              'count': 'trip_count'
          })
    )
    
    # Round fare values
    agg1['avg_fare'] = agg1['avg_fare'].round(2)
    agg1['median_fare'] = agg1['median_fare'].round(2)
    
    out_parquet = os.path.join(WORKDIR, "avg_fare_by_month_borough.parquet")
    out_csv = os.path.join(WORKDIR, "avg_fare_by_month_borough.csv")
    agg1.to_parquet(out_parquet, index=False)
    agg1.to_csv(out_csv, index=False)
    
    hook.load_file(out_parquet, key=f"{GOLD_PREFIX}/avg_fare_by_month_borough.parquet", 
                  bucket_name=BUCKET, replace=True)
    hook.load_file(out_csv, key=f"{GOLD_PREFIX}/avg_fare_by_month_borough.csv", 
                  bucket_name=BUCKET, replace=True)
    
    file_size_mb = os.path.getsize(out_parquet) / (1024 * 1024)
    print(f"✅ Created: avg_fare_by_month_borough")
    print(f"   → Rows: {len(agg1):,}")
    print(f"   → Parquet: {file_size_mb:.2f} MB")
    print(f"   → s3://{BUCKET}/{GOLD_PREFIX}/avg_fare_by_month_borough.*")

    # ============================
    # AGG 2: Hourly Trip Patterns
    # ============================
    hour_col = "pickup_hour" if "pickup_hour" in df.columns else None
    if hour_col and borough_col:
        print(f"\n{'='*60}")
        print(f"📈 Creating Aggregation 2: hourly_trip_patterns")
        print(f"{'='*60}")
        
        agg2 = (
            df.groupby([hour_col, borough_col], dropna=False)
              .agg({
                  fare_col: ['mean', 'count'],
                  'trip_minutes': 'mean' if 'trip_minutes' in df.columns else fare_col
              })
              .reset_index()
        )
        
        # Flatten column names
        agg2.columns = ['_'.join(col).strip('_') if col[1] else col[0] for col in agg2.columns.values]
        
        # Rename for clarity
        rename_dict = {}
        for col in agg2.columns:
            if 'fare_amount_mean' in col:
                rename_dict[col] = 'avg_fare'
            elif 'fare_amount_count' in col:
                rename_dict[col] = 'trip_count'
            elif 'trip_minutes_mean' in col:
                rename_dict[col] = 'avg_duration_minutes'
        
        if rename_dict:
            agg2 = agg2.rename(columns=rename_dict)
        
        # Round numeric values
        if 'avg_fare' in agg2.columns:
            agg2['avg_fare'] = agg2['avg_fare'].round(2)
        if 'avg_duration_minutes' in agg2.columns:
            agg2['avg_duration_minutes'] = agg2['avg_duration_minutes'].round(2)
        
        out_parquet = os.path.join(WORKDIR, "hourly_trip_patterns.parquet")
        agg2.to_parquet(out_parquet, index=False)
        hook.load_file(out_parquet, key=f"{GOLD_PREFIX}/hourly_trip_patterns.parquet", 
                      bucket_name=BUCKET, replace=True)
        
        print(f"✅ Created: hourly_trip_patterns")
        print(f"   → Rows: {len(agg2):,}")
        print(f"   → Peak hours: {agg2.nlargest(3, 'trip_count')['pickup_hour'].tolist() if 'trip_count' in agg2.columns else 'N/A'}")
        print(f"   → s3://{BUCKET}/{GOLD_PREFIX}/hourly_trip_patterns.parquet")

    # ============================
    # AGG 3: Daily Summary Statistics
    # ============================
    date_col = "pickup_date" if "pickup_date" in df.columns else None
    if date_col:
        print(f"\n{'='*60}")
        print(f"📈 Creating Aggregation 3: daily_summary")
        print(f"{'='*60}")
        
        agg3 = (
            df.groupby([date_col, 'dataset'])
              .agg({
                  fare_col: ['sum', 'mean', 'count'],
                  'trip_distance': 'sum' if 'trip_distance' in df.columns else fare_col,
                  'trip_minutes': 'mean' if 'trip_minutes' in df.columns else fare_col
              })
              .reset_index()
        )
        
        # Flatten column names
        agg3.columns = ['_'.join(col).strip('_') if col[1] else col[0] for col in agg3.columns.values]
        
        # Round numeric values
        numeric_cols = agg3.select_dtypes(include=['float64']).columns
        for col in numeric_cols:
            agg3[col] = agg3[col].round(2)
        
        out_parquet = os.path.join(WORKDIR, "daily_summary.parquet")
        agg3.to_parquet(out_parquet, index=False)
        hook.load_file(out_parquet, key=f"{GOLD_PREFIX}/daily_summary.parquet", 
                      bucket_name=BUCKET, replace=True)
        
        print(f"✅ Created: daily_summary")
        print(f"   → Rows: {len(agg3):,}")
        print(f"   → Date range: {agg3['pickup_date'].min()} to {agg3['pickup_date'].max()}")
        print(f"   → s3://{BUCKET}/{GOLD_PREFIX}/daily_summary.parquet")

    # ============================
    # AGG 4: Borough Rankings
    # ============================
    print(f"\n{'='*60}")
    print(f"📈 Creating Aggregation 4: borough_rankings")
    print(f"{'='*60}")
    
    agg4 = (
        df.groupby(borough_col, dropna=False)
          .agg({
              fare_col: ['mean', 'sum', 'count'],
              'trip_distance': 'sum' if 'trip_distance' in df.columns else fare_col,
              'trip_minutes': 'mean' if 'trip_minutes' in df.columns else fare_col
          })
          .reset_index()
    )
    
    # Flatten column names
    agg4.columns = ['_'.join(col).strip('_') if col[1] else col[0] for col in agg4.columns.values]
    
    # Rename for clarity
    rename_map = {
        'fare_amount_mean': 'avg_fare',
        'fare_amount_sum': 'total_revenue',
        'fare_amount_count': 'total_trips',
        'trip_distance_sum': 'total_distance',
        'trip_minutes_mean': 'avg_duration_minutes'
    }
    
    for old_col, new_col in rename_map.items():
        if old_col in agg4.columns:
            agg4 = agg4.rename(columns={old_col: new_col})
    
    # Add rankings
    if 'total_trips' in agg4.columns:
        agg4['rank_by_trips'] = agg4['total_trips'].rank(ascending=False, method='dense')
    if 'avg_fare' in agg4.columns:
        agg4['rank_by_fare'] = agg4['avg_fare'].rank(ascending=False, method='dense')
    
    # Round numeric values
    numeric_cols = agg4.select_dtypes(include=['float64']).columns
    for col in numeric_cols:
        agg4[col] = agg4[col].round(2)
    
    # Sort by total trips
    if 'total_trips' in agg4.columns:
        agg4 = agg4.sort_values('total_trips', ascending=False)
    
    out_parquet = os.path.join(WORKDIR, "borough_rankings.parquet")
    out_csv = os.path.join(WORKDIR, "borough_rankings.csv")
    agg4.to_parquet(out_parquet, index=False)
    agg4.to_csv(out_csv, index=False)
    
    hook.load_file(out_parquet, key=f"{GOLD_PREFIX}/borough_rankings.parquet", 
                  bucket_name=BUCKET, replace=True)
    hook.load_file(out_csv, key=f"{GOLD_PREFIX}/borough_rankings.csv", 
                  bucket_name=BUCKET, replace=True)
    
    print(f"✅ Created: borough_rankings")
    print(f"   → Rows: {len(agg4):,}")
    if 'total_trips' in agg4.columns:
        print(f"   → Top borough: {agg4.iloc[0]['pickup_borough']} ({agg4.iloc[0]['total_trips']:,.0f} trips)")
    print(f"   → s3://{BUCKET}/{GOLD_PREFIX}/borough_rankings.*")

    # Final summary
    print(f"\n{'='*60}")
    print(f"✅ GOLD LAYER AGGREGATIONS COMPLETE")
    print(f"{'='*60}")
    print(f"📊 Created 4 gold tables:")
    print(f"   1. avg_fare_by_month_borough ({len(agg1):,} rows)")
    if hour_col:
        print(f"   2. hourly_trip_patterns ({len(agg2):,} rows)")
    if date_col:
        print(f"   3. daily_summary ({len(agg3):,} rows)")
    print(f"   4. borough_rankings ({len(agg4):,} rows)")
    print(f"\n📍 Location: s3://{BUCKET}/{GOLD_PREFIX}/")
    print(f"{'='*60}\n")

# -------------------------------
# DAG
# -------------------------------
with DAG(
    dag_id="lakehouse_step3_to_gold",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["lakehouse", "gold", "aggregations"],
    description="Create multiple business-ready aggregations in gold layer",
) as dag:
    
    PythonOperator(
        task_id="to_gold",
        python_callable=aggregate_to_gold
    )