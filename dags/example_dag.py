from __future__ import annotations
import os
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# Read from env already provided in your compose/.env
CATALOG_WAREHOUSE = os.environ["CATALOG_WAREHOUSE"]        
CATALOG_S3_ENDPOINT = os.environ["CATALOG_S3_ENDPOINT"]    
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

# Constants
BRONZE_PREFIX = os.environ.get("BRONZE_PREFIX", "bronze/landing")
FILENAME = "IMPORT_LICENCE_REPORT.xlsx"                               # <- exact file name requested

# Derived
assert CATALOG_WAREHOUSE.startswith("s3://"), "CATALOG_WAREHOUSE must look like s3://bucket[/...]"
BUCKET = CATALOG_WAREHOUSE.replace("s3://", "").split("/")[0]
S3_ENDPOINT = CATALOG_S3_ENDPOINT

DATA_DIR = Path("/opt/airflow/data")
TMP_DIR = Path("/opt/airflow/tmp")
TMP_DIR.mkdir(parents=True, exist_ok=True)

def ensure_bronze_prefix():
    import boto3
    from botocore.client import Config
    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        region_name=AWS_REGION,
        config=Config(signature_version="s3v4"),
    )
    prefix = BRONZE_PREFIX.rstrip("/") + "/"
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix, MaxKeys=1)
    if resp.get("KeyCount", 0) == 0:
        s3.put_object(Bucket=BUCKET, Key=prefix + ".keep", Body=b"")
        print(f"[Bronze] Created prefix marker s3://{BUCKET}/{prefix}.keep")
    else:
        print(f"[Bronze] Prefix exists: s3://{BUCKET}/{prefix}")

def convert_and_upload():
    import pandas as pd
    import boto3
    from botocore.client import Config

    src = DATA_DIR / FILENAME
    if not src.exists():
        raise FileNotFoundError(f"Expected Excel at {src}")

    df = pd.read_excel(src, engine="openpyxl")
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]

    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    out = TMP_DIR / f"{src.stem}_{ts}.parquet"
    df.to_parquet(out, index=False)

    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        region_name=AWS_REGION,
        config=Config(signature_version="s3v4"),
    )
    key = f"{BRONZE_PREFIX.rstrip('/')}/{out.name}"
    s3.upload_file(str(out), BUCKET, key)
    print(f"[Bronze] Uploaded {out.name} → s3://{BUCKET}/{key}")

with DAG(
    dag_id="bronze_excel_to_bronze",
    start_date=datetime(2025, 9, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": "airflow"},
    tags=["bronze", "parquet", "minio", "no-spark"],
) as dag:
    t0 = PythonOperator(task_id="ensure_bronze_prefix", python_callable=ensure_bronze_prefix)
    t1 = PythonOperator(task_id="convert_and_upload", python_callable=convert_and_upload)
    t0 >> t1
