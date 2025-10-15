# dags/inspectorate/gold_inspectorate_licensing_metrics.py
from __future__ import annotations
import os, tempfile, json
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import List, Dict

from airflow import DAG
from airflow.operators.python import PythonOperator

# =================== ENV ===================
CATALOG_WAREHOUSE = os.environ["CATALOG_WAREHOUSE"]
CATALOG_S3_ENDPOINT = os.environ["CATALOG_S3_ENDPOINT"]
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

# ================= CONSTANTS ================
DIRECTORATE = "inspectorate"
DATASET = "imports"
LANE = "licensing"

SILVER_ROOT = f"silver/{DIRECTORATE}/{DATASET}/{LANE}"
GOLD_ROOT = f"gold/{DIRECTORATE}/{DATASET}/{LANE}/metrics_parquet"

# ================= DERIVED ==================
BUCKET = CATALOG_WAREHOUSE.replace("s3://", "").split("/")[0]
S3_ENDPOINT = CATALOG_S3_ENDPOINT

# ============ Arrow Processing =============
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pyarrow.fs as pafs

def _s3_fs() -> pafs.S3FileSystem:
    return pafs.S3FileSystem(
        access_key=os.environ.get("AWS_ACCESS_KEY_ID"),
        secret_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        endpoint_override=S3_ENDPOINT.replace("http://","").replace("https://",""),
        region=AWS_REGION,
        scheme=S3_ENDPOINT.split("://")[0],
    )

def _boto3():
    import boto3
    from botocore.client import Config
    return boto3.client(
        "s3", endpoint_url=S3_ENDPOINT, region_name=AWS_REGION,
        config=Config(signature_version="s3v4")
    )

def _list_silver_months(s3) -> List[str]:
    """List all year/month partitions in silver"""
    prefixes = []
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=f"{SILVER_ROOT}/", Delimiter="/")
    for prefix in resp.get('CommonPrefixes', []):
        if 'year=' in prefix['Prefix']:
            year = prefix['Prefix'].split('year=')[1].split('/')[0]
            # Get months for this year
            year_resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix['Prefix'], Delimiter="/")
            for month_prefix in year_resp.get('CommonPrefixes', []):
                if 'month=' in month_prefix['Prefix']:
                    month = month_prefix['Prefix'].split('month=')[1].split('/')[0]
                    prefixes.append(f"{year}-{month}")
    return sorted(prefixes)

def _get_processed_months(s3) -> List[str]:
    """Get already processed months from gold markers"""
    try:
        resp = s3.get_object(Bucket=BUCKET, Key=f"{GOLD_ROOT}/_processed/processed_months.parquet")
        tbl = pq.read_table(resp['Body'])
        return tbl['month'].to_pylist()
    except:
        return []

def _calculate_licensing_metrics(tbl: pa.Table, month: str) -> pa.Table:
    """Calculate licensing metrics and return as Arrow Table"""
    metrics_data = []
    
    # Total applications
    total_apps = tbl.num_rows
    
    # Applications by license type
    license_types = {}
    if 'NDAApplicationType' in tbl.schema.names:
        try:
            value_counts_result = pc.value_counts(tbl['NDAApplicationType'])
            # FIX: Use StructArray access correctly
            values_array = value_counts_result.field(0)  # First field is 'values'
            counts_array = value_counts_result.field(1)  # Second field is 'counts'
            
            for i in range(len(values_array)):
                license_types[str(values_array[i].as_py())] = counts_array[i].as_py()
        except Exception as e:
            print(f"Error calculating license types: {str(e)}")
    
    # Applications by business type
    business_types = {}
    if 'Business_Type' in tbl.schema.names:
        try:
            value_counts_result = pc.value_counts(tbl['Business_Type'])
            values_array = value_counts_result.field(0)
            counts_array = value_counts_result.field(1)
            
            for i in range(len(values_array)):
                business_types[str(values_array[i].as_py())] = counts_array[i].as_py()
        except Exception as e:
            print(f"Error calculating business types: {str(e)}")
    
    # TOT metrics
    tot_metrics = {}
    if all(col in tbl.schema.names for col in ['Date_of_Application', 'SA_Approval_Date']):
        try:
            app_dates = tbl['Date_of_Application']
            approval_dates = tbl['SA_Approval_Date']
            
            # Filter out records with missing dates
            valid_mask = pc.and_(
                pc.is_not_null(app_dates),
                pc.is_not_null(approval_dates)
            )
            valid_tbl = tbl.filter(valid_mask)
            
            if valid_tbl.num_rows > 0:
                # Calculate base TOT in days
                base_tot_seconds = (valid_tbl['SA_Approval_Date'] - valid_tbl['Date_of_Application']).cast(pa.int64())
                base_tot_days = pc.divide(base_tot_seconds, pc.scalar(86400000000.0))  # Convert to days
                
                # Adjust for queries if available
                if all(col in valid_tbl.schema.names for col in ['Query_Date', 'Qury_Resonse_Date']):
                    query_starts = valid_tbl['Query_Date']
                    query_ends = valid_tbl['Qury_Resonse_Date']
                    
                    # Calculate query duration (0 if no query)
                    has_query = pc.is_not_null(query_starts)
                    query_duration_seconds = pc.if_else(
                        has_query,
                        pc.if_else(
                            pc.is_not_null(query_ends),
                            (query_ends - query_starts).cast(pa.int64()),
                            pc.scalar(0, type=pa.int64())
                        ),
                        pc.scalar(0, type=pa.int64())
                    )
                    query_duration_days = pc.divide(query_duration_seconds, pc.scalar(86400000000.0))
                    
                    adjusted_tot = pc.max(pc.subtract(base_tot_days, query_duration_days), pc.scalar(0.0, type=pa.float64()))
                    tot_values = adjusted_tot.to_pylist()
                else:
                    tot_values = base_tot_days.to_pylist()
                
                # Calculate statistics
                if tot_values:
                    valid_tot_values = [v for v in tot_values if v is not None and v >= 0]
                    if valid_tot_values:
                        tot_metrics['avg_tot_days'] = sum(valid_tot_values) / len(valid_tot_values)
                        tot_metrics['max_tot_days'] = max(valid_tot_values)
                        tot_metrics['min_tot_days'] = min(valid_tot_values)
                        tot_metrics['applications_with_tot'] = len(valid_tot_values)
        except Exception as e:
            print(f"Error calculating TOT metrics: {str(e)}")
    
    # Efficiency metrics
    efficiency_metrics = {}
    try:
        # Approval rate (has SA approval date)
        if 'SA_Approval_Date' in tbl.schema.names:
            approved_count = pc.sum(pc.is_not_null(tbl['SA_Approval_Date'])).as_py()
            efficiency_metrics['approval_rate'] = float(approved_count) / tbl.num_rows if tbl.num_rows > 0 else 0.0
        
        # Query rate
        if 'Query_Date' in tbl.schema.names:
            queried_count = pc.sum(pc.is_not_null(tbl['Query_Date'])).as_py()
            efficiency_metrics['query_rate'] = float(queried_count) / tbl.num_rows if tbl.num_rows > 0 else 0.0
    except Exception as e:
        print(f"Error calculating efficiency metrics: {str(e)}")
    
    # Create metrics record
    metrics_record = {
        'month': month,
        'processing_date': datetime.utcnow().isoformat(),
        'total_applications': total_apps,
        'license_types': json.dumps(license_types),
        'business_types': json.dumps(business_types),
        'avg_tot_days': tot_metrics.get('avg_tot_days'),
        'max_tot_days': tot_metrics.get('max_tot_days'),
        'min_tot_days': tot_metrics.get('min_tot_days'),
        'applications_with_tot': tot_metrics.get('applications_with_tot', 0),
        'approval_rate': efficiency_metrics.get('approval_rate', 0.0),
        'query_rate': efficiency_metrics.get('query_rate', 0.0)
    }
    
    metrics_data.append(metrics_record)
    
    # Convert to Arrow Table
    schema = pa.schema([
        ('month', pa.string()),
        ('processing_date', pa.string()),
        ('total_applications', pa.int64()),
        ('license_types', pa.string()),
        ('business_types', pa.string()),
        ('avg_tot_days', pa.float64()),
        ('max_tot_days', pa.float64()),
        ('min_tot_days', pa.float64()),
        ('applications_with_tot', pa.int64()),
        ('approval_rate', pa.float64()),
        ('query_rate', pa.float64())
    ])
    
    return pa.Table.from_pylist(metrics_data, schema=schema)

def process_licensing_metrics():
    """Main function to process licensing metrics"""
    s3 = _boto3()
    fs = _s3_fs()
    
    # Ensure gold directory exists
    s3.put_object(Bucket=BUCKET, Key=f"{GOLD_ROOT}/.keep", Body=b"")
    s3.put_object(Bucket=BUCKET, Key=f"{GOLD_ROOT}/_processed/.keep", Body=b"")
    
    # Get available and processed months
    available_months = _list_silver_months(s3)
    processed_months = _get_processed_months(s3)
    
    print(f"Available months: {available_months}")
    print(f"Already processed: {processed_months}")
    
    # Process new months
    new_months = [m for m in available_months if m not in processed_months]
    
    if not new_months:
        print("No new months to process")
        return
    
    print(f"Processing new months: {new_months}")
    
    all_metrics_tables = []
    processed_this_run = []
    
    for month in new_months:
        print(f"Processing licensing metrics for {month}")
        try:
            year, month_num = month.split('-')
            prefix = f"{SILVER_ROOT}/year={year}/month={month_num}/"
            
            # List all files for this month
            files = []
            paginator = s3.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
                for obj in page.get('Contents', []):
                    if obj['Key'].endswith('.parquet'):
                        files.append(obj['Key'])
            
            if not files:
                print(f"No files found for {month}")
                continue
            
            # Read and combine all files for the month
            tables = []
            for file_key in files:
                try:
                    s3path = f"{BUCKET}/{file_key}"
                    tbl = pq.read_table(s3path, filesystem=fs)
                    
                    # Only keep columns needed for metrics
                    keep_cols = ['Date_of_Application', 'Date_of_Payment', 'Manager_Review_Date', 
                                'Director_Review_Date', 'SA_Approval_Date', 'Query_Date', 
                                'Qury_Resonse_Date', 'NDAApplicationType', 'Business_Type']
                    available_cols = [col for col in keep_cols if col in tbl.schema.names]
                    tbl = tbl.select(available_cols)
                    tables.append(tbl)
                except Exception as e:
                    print(f"Error reading {file_key}: {str(e)}")
                    continue
            
            if not tables:
                print(f"No valid tables for {month}")
                continue
                
            # Combine all tables for the month
            combined = pa.concat_tables(tables)
            
            if combined.num_rows == 0:
                print(f"No rows in combined table for {month}")
                continue
            
            print(f"Processing {combined.num_rows} rows for {month}")
            
            # Calculate metrics
            metrics_table = _calculate_licensing_metrics(combined, month)
            all_metrics_tables.append(metrics_table)
            processed_this_run.append(month)
            print(f"Successfully processed {month}")
            
        except Exception as e:
            print(f"Error processing {month}: {str(e)}")
            import traceback
            traceback.print_exc()
            continue
    
    if all_metrics_tables:
        # Write metrics to partitioned Parquet files
        for metrics_table in all_metrics_tables:
            month = metrics_table['month'][0].as_py()
            year, month_num = month.split('-')
            
            # Write to partitioned location
            output_prefix = f"{GOLD_ROOT}/year={year}/month={month_num}/"
            
            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmpf:
                tmp_path = Path(tmpf.name)
            
            try:
                pq.write_table(metrics_table, tmp_path, compression='snappy')
                
                # Upload to S3
                s3.upload_file(str(tmp_path), BUCKET, f"{output_prefix}metrics.parquet")
                print(f"Saved metrics for {month} to {output_prefix}metrics.parquet")
            finally:
                tmp_path.unlink(missing_ok=True)
        
        # Update processed months
        updated_processed = processed_months + processed_this_run
        
        # Save processed months as Parquet
        processed_table = pa.Table.from_arrays(
            [pa.array(updated_processed)], 
            names=['month']
        )
        
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmpf:
            tmp_path = Path(tmpf.name)
        
        try:
            pq.write_table(processed_table, tmp_path, compression='snappy')
            s3.upload_file(str(tmp_path), BUCKET, f"{GOLD_ROOT}/_processed/processed_months.parquet")
            print(f"Updated processed months marker with {len(updated_processed)} months")
        finally:
            tmp_path.unlink(missing_ok=True)
        
        print(f"Processed {len(all_metrics_tables)} months. Metrics saved as Parquet files")
        
    else:
        print("No metrics calculated for new months")

# =================== DAG ====================
with DAG(
    dag_id="gold_inspectorate_licensing_metrics_v8",
    start_date=datetime(2025, 9, 1),
    schedule_interval="0 2 * * *",  # Daily at 2 AM
    catchup=False,
    default_args={"owner": "airflow"},
    tags=["gold", "inspectorate", "licensing", "metrics", "parquet"],
) as dag:
    process_task = PythonOperator(
        task_id="process_licensing_metrics",
        python_callable=process_licensing_metrics
    )
