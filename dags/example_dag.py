# dags/bronze_excel_to_bronze.py
from __future__ import annotations
import os
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# ---- ENV (provided via docker-compose .env) ----
CATALOG_WAREHOUSE = os.environ["CATALOG_WAREHOUSE"]          # e.g. s3://warehouse
CATALOG_S3_ENDPOINT = os.environ["CATALOG_S3_ENDPOINT"]      # e.g. http://minio:9000
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

# ---- CONSTANTS ----
BRONZE_PREFIX = os.environ.get("BRONZE_PREFIX", "bronze/landing")
FILENAME = "IMPORT_LICENCE_REPORT.xlsx"  # exact file name

# ---- DERIVED ----
assert CATALOG_WAREHOUSE.startswith("s3://"), "CATALOG_WAREHOUSE must look like s3://bucket[/...]"
BUCKET = CATALOG_WAREHOUSE.replace("s3://", "").split("/")[0]
S3_ENDPOINT = CATALOG_S3_ENDPOINT

DATA_DIR = Path("/opt/airflow/data")
TMP_DIR = Path("/opt/airflow/tmp")
TMP_DIR.mkdir(parents=True, exist_ok=True)


def ensure_bronze_prefix():
    """Ensure s3://<bucket>/bronze/landing/ exists (visible in MinIO Console)."""
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
    """Read Excel → stabilize dtypes → write Parquet → upload to bronze."""
    import pandas as pd
    import boto3
    from botocore.client import Config

    src = DATA_DIR / FILENAME
    if not src.exists():
        raise FileNotFoundError(f"Expected Excel at {src}")

    # Load
    df = pd.read_excel(src, engine="openpyxl")

    # Normalize column names
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]

    # Stabilize types so pyarrow doesn't choke on mixed columns
    #   - parse likely date columns
    for c in df.columns:
        lc = c.lower()
        if "date" in lc or lc.endswith("_at") or lc.endswith("_on"):
            df[c] = pd.to_datetime(df[c], errors="coerce")
    #   - coerce numeric-looking text; remaining objects -> pandas 'string' (Arrow => string)
    for c in df.columns:
        if df[c].dtype == "object":
            df[c] = pd.to_numeric(df[c], errors="ignore")
        if df[c].dtype == "object":
            df[c] = df[c].astype("string")

    # Parquet filename with UTC timestamp
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    out = TMP_DIR / f"{src.stem}_{ts}.parquet"
    df.to_parquet(out, index=False)  # engine=pyarrow by default

    # Upload to bronze
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

    t0 = PythonOperator(
        task_id="ensure_bronze_prefix",
        python_callable=ensure_bronze_prefix,
    )

    t1 = PythonOperator(
        task_id="convert_and_upload",
        python_callable=convert_and_upload,
    )

    t0 >> t1
    