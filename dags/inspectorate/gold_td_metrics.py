# dags/inspectorate/gold_inspectorate_td_metrics.py
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
LANE = "td"

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

def _calculate_td_metrics(tbl: pa.Table, month: str) -> pa.Table:
    """Calculate TD metrics and return as Arrow Table"""
    metrics_data = []
    
    # Total declarations
    total_declarations = tbl.num_rows
    
    # Volume metrics - by shipment mode
    shipment_modes = {}
    if 'ShipmentMode' in tbl.schema.names:
        try:
            value_counts_result = pc.value_counts(tbl['ShipmentMode'])
            values_array = value_counts_result.field(0)
            counts_array = value_counts_result.field(1)
            
            for i in range(len(values_array)):
                shipment_modes[str(values_array[i].as_py())] = counts_array[i].as_py()
        except Exception as e:
            print(f"Error calculating shipment modes: {str(e)}")
    
    # Volume metrics - by shipment category
    shipment_categories = {}
    if 'ShipmentCategory' in tbl.schema.names:
        try:
            value_counts_result = pc.value_counts(tbl['ShipmentCategory'])
            values_array = value_counts_result.field(0)
            counts_array = value_counts_result.field(1)
            
            for i in range(len(values_array)):
                shipment_categories[str(values_array[i].as_py())] = counts_array[i].as_py()
        except Exception as e:
            print(f"Error calculating shipment categories: {str(e)}")
    
    # Volume metrics - top products (brand names)
    top_products = {}
    if 'brand_name' in tbl.schema.names:
        try:
            value_counts_result = pc.value_counts(tbl['brand_name'])
            values_array = value_counts_result.field(0)
            counts_array = value_counts_result.field(1)
            
            # Take top 10 products by count
            for i in range(min(10, len(values_array))):
                top_products[str(values_array[i].as_py())] = counts_array[i].as_py()
        except Exception as e:
            print(f"Error calculating top products: {str(e)}")
    
    # TOT metrics - submission to release (excluding query periods)
    tot_metrics = {}
    if all(col in tbl.schema.names for col in ['SubmissionDate', 'date_released']):
        try:
            submission_dates = tbl['SubmissionDate']
            release_dates = tbl['date_released']
            
            # Filter valid records
            valid_mask = pc.and_(
                pc.is_not_null(submission_dates),
                pc.is_not_null(release_dates)
            )
            valid_tbl = tbl.filter(valid_mask)
            
            if valid_tbl.num_rows > 0:
                # Calculate base TOT in days
                tot_seconds = (valid_tbl['date_released'] - valid_tbl['SubmissionDate']).cast(pa.int64())
                tot_days = pc.divide(tot_seconds, pc.scalar(86400000000.0))
                
                # Adjust for queries if available
                if all(col in valid_tbl.schema.names for col in ['queried_on', 'responded_on']):
                    query_starts = valid_tbl['queried_on']
                    query_ends = valid_tbl['responded_on']
                    
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
                    
                    adjusted_tot = pc.max(pc.subtract(tot_days, query_duration_days), pc.scalar(0.0, type=pa.float64()))
                    tot_values = adjusted_tot.to_pylist()
                else:
                    tot_values = tot_days.to_pylist()
                
                if tot_values:
                    valid_tot_values = [v for v in tot_values if v is not None and v >= 0]
                    if valid_tot_values:
                        tot_metrics['avg_processing_days'] = sum(valid_tot_values) / len(valid_tot_values)
                        tot_metrics['max_processing_days'] = max(valid_tot_values)
                        tot_metrics['declarations_with_tot'] = len(valid_tot_values)
        except Exception as e:
            print(f"Error calculating TOT metrics: {str(e)}")
    
    # Value metrics
    value_metrics = {}
    if 'total_value' in tbl.schema.names:
        try:
            values = tbl['total_value']
            non_null_values = values.filter(pc.is_not_null(values))
            
            if non_null_values.length > 0:
                values_list = non_null_values.to_pylist()
                value_metrics['total_declared_value'] = sum(values_list)
                value_metrics['avg_value_per_declaration'] = sum(values_list) / len(values_list)
                value_metrics['declarations_with_value'] = len(values_list)
        except Exception as e:
            print(f"Error calculating value metrics: {str(e)}")
    
    # Import/Export analysis (infer from ShipmentCategory)
    import_export_stats = {}
    if 'ShipmentCategory' in tbl.schema.names:
        try:
            # Simple inference - you might need more sophisticated logic
            import_count = pc.sum(pc.equal(tbl['ShipmentCategory'], 'Import')).as_py()
            export_count = pc.sum(pc.equal(tbl['ShipmentCategory'], 'Export')).as_py()
            import_export_stats = {
                'import_count': import_count,
                'export_count': export_count,
                'import_export_ratio': import_count / export_count if export_count > 0 else float('inf')
            }
        except Exception as e:
            print(f"Error calculating import/export stats: {str(e)}")
    
    # Create metrics record
    metrics_record = {
        'month': month,
        'processing_date': datetime.utcnow().isoformat(),
        'total_declarations': total_declarations,
        'shipment_modes': json.dumps(shipment_modes),
        'shipment_categories': json.dumps(shipment_categories),
        'top_products': json.dumps(top_products),
        'avg_processing_days': tot_metrics.get('avg_processing_days'),
        'max_processing_days': tot_metrics.get('max_processing_days'),
        'declarations_with_tot': tot_metrics.get('declarations_with_tot', 0),
        'total_declared_value': value_metrics.get('total_declared_value'),
        'avg_value_per_declaration': value_metrics.get('avg_value_per_declaration'),
        'declarations_with_value': value_metrics.get('declarations_with_value', 0),
        'import_count': import_export_stats.get('import_count', 0),
        'export_count': import_export_stats.get('export_count', 0),
        'import_export_ratio': import_export_stats.get('import_export_ratio', 0.0)
    }
    
    metrics_data.append(metrics_record)
    
    # Convert to Arrow Table
    schema = pa.schema([
        ('month', pa.string()),
        ('processing_date', pa.string()),
        ('total_declarations', pa.int64()),
        ('shipment_modes', pa.string()),
        ('shipment_categories', pa.string()),
        ('top_products', pa.string()),
        ('avg_processing_days', pa.float64()),
        ('max_processing_days', pa.float64()),
        ('declarations_with_tot', pa.int64()),
        ('total_declared_value', pa.float64()),
        ('avg_value_per_declaration', pa.float64()),
        ('declarations_with_value', pa.int64()),
        ('import_count', pa.int64()),
        ('export_count', pa.int64()),
        ('import_export_ratio', pa.float64())
    ])
    
    return pa.Table.from_pylist(metrics_data, schema=schema)

def process_td_metrics():
    """Main function to process TD metrics"""
    s3 = _boto3()
    fs = _s3_fs()
    
    # Ensure gold directory exists
    s3.put_object(Bucket=BUCKET, Key=f"{GOLD_ROOT}/.keep", Body=b"")
    s3.put_object(Bucket=BUCKET, Key=f"{GOLD_ROOT}/_processed/.keep", Body=b"")
    
    # Get available and processed months
    available_months = _list_silver_months(s3)
    processed_months = _get_processed_months(s3)
    
    print(f"Available TD months: {available_months}")
    print(f"Already processed TD: {processed_months}")
    
    # Process new months
    new_months = [m for m in available_months if m not in processed_months]
    
    if not new_months:
        print("No new TD months to process")
        return
    
    print(f"Processing new TD months: {new_months}")
    
    all_metrics_tables = []
    processed_this_run = []
    
    for month in new_months:
        print(f"Processing TD metrics for {month}")
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
                print(f"No TD files found for {month}")
                continue
            
            # Read and combine all files for the month
            tables = []
            for file_key in files:
                try:
                    s3path = f"{BUCKET}/{file_key}"
                    tbl = pq.read_table(s3path, filesystem=fs)
                    
                    # Only keep columns needed for TD metrics (based on Silver schema)
                    keep_cols = [
                        'SubmissionDate', 'date_released', 'ShipmentDate', 
                        'ShipmentMode', 'ShipmentCategory', 'brand_name', 
                        'Consignee', 'total_value', 'query_ref', 'queried_on', 
                        'responded_on', 'year', 'month'
                    ]
                    available_cols = [col for col in keep_cols if col in tbl.schema.names]
                    tbl = tbl.select(available_cols)
                    tables.append(tbl)
                except Exception as e:
                    print(f"Error reading TD file {file_key}: {str(e)}")
                    continue
            
            if not tables:
                print(f"No valid TD tables for {month}")
                continue
                
            # Combine all tables for the month
            combined = pa.concat_tables(tables)
            
            if combined.num_rows == 0:
                print(f"No rows in combined TD table for {month}")
                continue
            
            print(f"Processing {combined.num_rows} TD rows for {month}")
            
            # Calculate metrics
            metrics_table = _calculate_td_metrics(combined, month)
            all_metrics_tables.append(metrics_table)
            processed_this_run.append(month)
            print(f"Successfully processed TD {month}")
            
        except Exception as e:
            print(f"Error processing TD {month}: {str(e)}")
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
                print(f"Saved TD metrics for {month} to {output_prefix}metrics.parquet")
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
            print(f"Updated TD processed months marker with {len(updated_processed)} months")
        finally:
            tmp_path.unlink(missing_ok=True)
        
        print(f"Processed {len(all_metrics_tables)} TD months. Metrics saved as Parquet files")
        
    else:
        print("No TD metrics calculated for new months")

# =================== DAG ====================
with DAG(
    dag_id="gold_inspectorate_td_metrics_v8",
    start_date=datetime(2025, 9, 1),
    schedule_interval="0 3 * * *",  # Daily at 3 AM
    catchup=False,
    default_args={"owner": "airflow"},
    tags=["gold", "inspectorate", "td", "metrics", "parquet"],
) as dag:
    process_task = PythonOperator(
        task_id="process_td_metrics",
        python_callable=process_td_metrics
    )
    