# dags/inspectorate/gold_inspectorate_vc_metrics.py
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
LANE = "vc"

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

def _calculate_vc_metrics(tbl: pa.Table, month: str) -> pa.Table:
    """Calculate VC metrics and return as Arrow Table"""
    metrics_data = []
    
    # Total certificates
    total_certificates = tbl.num_rows
    
    # Volume metrics - by VC type
    vc_types = {}
    if 'VCType' in tbl.schema.names:
        try:
            value_counts_result = pc.value_counts(tbl['VCType'])
            values_array = value_counts_result.field(0)
            counts_array = value_counts_result.field(1)
            
            for i in range(len(values_array)):
                vc_types[str(values_array[i].as_py())] = counts_array[i].as_py()
        except Exception as e:
            print(f"Error calculating VC types: {str(e)}")
    
    # Volume metrics - by import reason
    import_reasons = {}
    if 'importationReason' in tbl.schema.names:
        try:
            value_counts_result = pc.value_counts(tbl['importationReason'])
            values_array = value_counts_result.field(0)
            counts_array = value_counts_result.field(1)
            
            for i in range(len(values_array)):
                import_reasons[str(values_array[i].as_py())] = counts_array[i].as_py()
        except Exception as e:
            print(f"Error calculating import reasons: {str(e)}")
    
    # Volume metrics - by current stage
    stages = {}
    if 'current_stage' in tbl.schema.names:
        try:
            value_counts_result = pc.value_counts(tbl['current_stage'])
            values_array = value_counts_result.field(0)
            counts_array = value_counts_result.field(1)
            
            for i in range(len(values_array)):
                stages[str(values_array[i].as_py())] = counts_array[i].as_py()
        except Exception as e:
            print(f"Error calculating stages: {str(e)}")
    
    # TOT metrics - receipt to billing
    tot_metrics = {}
    if all(col in tbl.schema.names for col in ['date_received', 'submission_date']):
        try:
            receipt_dates = tbl['date_received']
            submission_dates = tbl['submission_date']
            
            valid_mask = pc.and_(
                pc.is_not_null(receipt_dates),
                pc.is_not_null(submission_dates)
            )
            valid_tbl = tbl.filter(valid_mask)
            
            if valid_tbl.num_rows > 0:
                receipt_to_billing_seconds = (valid_tbl['submission_date'] - valid_tbl['date_received']).cast(pa.int64())
                receipt_to_billing_days = pc.divide(receipt_to_billing_seconds, pc.scalar(86400000000.0))
                values = receipt_to_billing_days.to_pylist()
                valid_values = [v for v in values if v is not None and v >= 0]
                if valid_values:
                    tot_metrics['avg_receipt_to_billing_days'] = sum(valid_values) / len(valid_values)
        except Exception as e:
            print(f"Error calculating receipt to billing: {str(e)}")
    
    # TOT metrics - payment to decision (excluding query periods)
    if all(col in tbl.schema.names for col in ['submission_date', 'date_released']):
        try:
            submission_dates = tbl['submission_date']
            release_dates = tbl['date_released']
            
            valid_mask = pc.and_(
                pc.is_not_null(submission_dates),
                pc.is_not_null(release_dates)
            )
            valid_tbl = tbl.filter(valid_mask)
            
            if valid_tbl.num_rows > 0:
                payment_to_decision_seconds = (valid_tbl['date_released'] - valid_tbl['submission_date']).cast(pa.int64())
                payment_to_decision_days = pc.divide(payment_to_decision_seconds, pc.scalar(86400000000.0))
                
                # Adjust for queries if available
                if all(col in valid_tbl.schema.names for col in ['queried_on', 'QueryRespondedOn']):
                    query_starts = valid_tbl['queried_on']
                    query_ends = valid_tbl['QueryRespondedOn']
                    
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
                    
                    adjusted_tot = pc.max(pc.subtract(payment_to_decision_days, query_duration_days), pc.scalar(0.0, type=pa.float64()))
                    values = adjusted_tot.to_pylist()
                else:
                    values = payment_to_decision_days.to_pylist()
                
                valid_values = [v for v in values if v is not None and v >= 0]
                if valid_values:
                    tot_metrics['avg_payment_to_decision_days'] = sum(valid_values) / len(valid_values)
                    tot_metrics['certificates_with_decision'] = len(valid_values)
        except Exception as e:
            print(f"Error calculating payment to decision: {str(e)}")
    
    # Business metrics - value analysis
    business_metrics = {}
    if 'total_value' in tbl.schema.names:
        try:
            values = tbl['total_value']
            non_null_values = values.filter(pc.is_not_null(values))
            
            if non_null_values.length > 0:
                values_list = non_null_values.to_pylist()
                business_metrics['total_verification_value'] = sum(values_list)
                business_metrics['avg_value_per_certificate'] = sum(values_list) / len(values_list)
        except Exception as e:
            print(f"Error calculating value metrics: {str(e)}")
    
    # Efficiency metrics
    if 'date_released' in tbl.schema.names:
        try:
            approved_count = pc.sum(pc.is_not_null(tbl['date_released'])).as_py()
            business_metrics['approval_rate'] = float(approved_count) / tbl.num_rows if tbl.num_rows > 0 else 0.0
        except Exception as e:
            print(f"Error calculating approval rate: {str(e)}")
    
    if 'query_ref' in tbl.schema.names:
        try:
            queried_count = pc.sum(pc.is_not_null(tbl['query_ref'])).as_py()
            business_metrics['query_rate'] = float(queried_count) / tbl.num_rows if tbl.num_rows > 0 else 0.0
        except Exception as e:
            print(f"Error calculating query rate: {str(e)}")
    
    # Import/Export by reason analysis
    import_export_by_reason = {}
    if 'importationReason' in tbl.schema.names:
        try:
            # Group by import reason for business analysis
            reason_counts = pc.value_counts(tbl['importationReason'])
            values_array = reason_counts.field(0)
            counts_array = reason_counts.field(1)
            
            for i in range(len(values_array)):
                import_export_by_reason[str(values_array[i].as_py())] = counts_array[i].as_py()
        except Exception as e:
            print(f"Error calculating import/export by reason: {str(e)}")
    
    # Create metrics record
    metrics_record = {
        'month': month,
        'processing_date': datetime.utcnow().isoformat(),
        'total_certificates': total_certificates,
        'vc_types': json.dumps(vc_types),
        'import_reasons': json.dumps(import_reasons),
        'stages': json.dumps(stages),
        'avg_receipt_to_billing_days': tot_metrics.get('avg_receipt_to_billing_days'),
        'avg_payment_to_decision_days': tot_metrics.get('avg_payment_to_decision_days'),
        'certificates_with_decision': tot_metrics.get('certificates_with_decision', 0),
        'total_verification_value': business_metrics.get('total_verification_value'),
        'avg_value_per_certificate': business_metrics.get('avg_value_per_certificate'),
        'approval_rate': business_metrics.get('approval_rate', 0.0),
        'query_rate': business_metrics.get('query_rate', 0.0),
        'import_export_by_reason': json.dumps(import_export_by_reason)
    }
    
    metrics_data.append(metrics_record)
    
    # Convert to Arrow Table
    schema = pa.schema([
        ('month', pa.string()),
        ('processing_date', pa.string()),
        ('total_certificates', pa.int64()),
        ('vc_types', pa.string()),
        ('import_reasons', pa.string()),
        ('stages', pa.string()),
        ('avg_receipt_to_billing_days', pa.float64()),
        ('avg_payment_to_decision_days', pa.float64()),
        ('certificates_with_decision', pa.int64()),
        ('total_verification_value', pa.float64()),
        ('avg_value_per_certificate', pa.float64()),
        ('approval_rate', pa.float64()),
        ('query_rate', pa.float64()),
        ('import_export_by_reason', pa.string())
    ])
    
    return pa.Table.from_pylist(metrics_data, schema=schema)

def process_vc_metrics():
    """Main function to process VC metrics"""
    s3 = _boto3()
    fs = _s3_fs()
    
    # Ensure gold directory exists
    s3.put_object(Bucket=BUCKET, Key=f"{GOLD_ROOT}/.keep", Body=b"")
    s3.put_object(Bucket=BUCKET, Key=f"{GOLD_ROOT}/_processed/.keep", Body=b"")
    
    # Get available and processed months
    available_months = _list_silver_months(s3)
    processed_months = _get_processed_months(s3)
    
    print(f"Available VC months: {available_months}")
    print(f"Already processed VC: {processed_months}")
    
    # Process new months
    new_months = [m for m in available_months if m not in processed_months]
    
    if not new_months:
        print("No new VC months to process")
        return
    
    print(f"Processing new VC months: {new_months}")
    
    all_metrics_tables = []
    processed_this_run = []
    
    for month in new_months:
        print(f"Processing VC metrics for {month}")
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
                print(f"No VC files found for {month}")
                continue
            
            # Read and combine all files for the month
            tables = []
            for file_key in files:
                try:
                    s3path = f"{BUCKET}/{file_key}"
                    tbl = pq.read_table(s3path, filesystem=fs)
                    
                    # Only keep columns needed for VC metrics (based on Silver schema)
                    keep_cols = [
                        'submission_date', 'date_received', 'date_released', 'VCType', 
                        'importationReason', 'current_stage', 'total_value', 'query_ref', 
                        'queried_on', 'QueryRespondedOn', 'brand_name', 'Supplier', 
                        'year', 'month'
                    ]
                    available_cols = [col for col in keep_cols if col in tbl.schema.names]
                    tbl = tbl.select(available_cols)
                    tables.append(tbl)
                except Exception as e:
                    print(f"Error reading VC file {file_key}: {str(e)}")
                    continue
            
            if not tables:
                print(f"No valid VC tables for {month}")
                continue
                
            # Combine all tables for the month
            combined = pa.concat_tables(tables)
            
            if combined.num_rows == 0:
                print(f"No rows in combined VC table for {month}")
                continue
            
            print(f"Processing {combined.num_rows} VC rows for {month}")
            
            # Calculate metrics
            metrics_table = _calculate_vc_metrics(combined, month)
            all_metrics_tables.append(metrics_table)
            processed_this_run.append(month)
            print(f"Successfully processed VC {month}")
            
        except Exception as e:
            print(f"Error processing VC {month}: {str(e)}")
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
                print(f"Saved VC metrics for {month} to {output_prefix}metrics.parquet")
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
            print(f"Updated VC processed months marker with {len(updated_processed)} months")
        finally:
            tmp_path.unlink(missing_ok=True)
        
        print(f"Processed {len(all_metrics_tables)} VC months. Metrics saved as Parquet files")
        
    else:
        print("No VC metrics calculated for new months")

# =================== DAG ====================
with DAG(
    dag_id="gold_inspectorate_vc_metrics_v8",
    start_date=datetime(2025, 9, 1),
    schedule_interval="0 4 * * *",  # Daily at 4 AM
    catchup=False,
    default_args={"owner": "airflow"},
    tags=["gold", "inspectorate", "vc", "metrics", "parquet"],
) as dag:
    process_task = PythonOperator(
        task_id="process_vc_metrics",
        python_callable=process_vc_metrics
    )
