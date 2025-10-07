# dags/bronze_inspectorate_ingest.py
from __future__ import annotations
import os
import re
import json
import tempfile
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# ---------- ENV ----------
CATALOG_WAREHOUSE = os.environ["CATALOG_WAREHOUSE"]          # e.g., s3://warehouse
CATALOG_S3_ENDPOINT = os.environ["CATALOG_S3_ENDPOINT"]      # e.g., http://minio:9000
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
AIRFLOW_DATA_DIR = os.environ.get("AIRFLOW_DATA_DIR", "/opt/airflow/data")  # inbox mount

# ---------- CONSTANTS ----------
DIRECTORATE = "inspectorate"
BRONZE_ROOT = f"bronze/{DIRECTORATE}"

# ---------- DERIVED ----------
assert CATALOG_WAREHOUSE.startswith("s3://"), "CATALOG_WAREHOUSE must look like s3://bucket[/...]"
BUCKET = CATALOG_WAREHOUSE.replace("s3://", "").split("/")[0]
S3_ENDPOINT = CATALOG_S3_ENDPOINT
INBOX_DIR = Path(AIRFLOW_DATA_DIR)  # read-only inbox is fine

# ---------- HELPERS ----------
def _norm(name: str) -> str:
    return re.sub(r"[^a-z0-9_]+", "_", name.strip().lower()).strip("_")

def _dataset_from_filename(p: Path) -> str:
    # strip any accidental ".processed" marker in stem if present
    stem = p.stem.replace(".processed", "")
    return _norm(stem) or "misc"

def _processed_marker_key(src: Path) -> str:
    # Single marker object per workbook to mark it processed
    # e.g. _processed_markers/inspectorate/IMPORT_LICENCE_REPORT.xlsx.done
    return f"_processed_markers/{DIRECTORATE}/{src.name}.done"

def _is_already_processed(s3, bucket: str, key: str) -> bool:
    # Works with MinIO: list for exact prefix
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=key, MaxKeys=1)
    return resp.get("KeyCount", 0) > 0

# ---------- TASKS ----------
def ensure_directorate_prefixes():
    import boto3
    from botocore.client import Config

    s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT, region_name=AWS_REGION,
                      config=Config(signature_version="s3v4"))

    base = f"{BRONZE_ROOT}/"
    if s3.list_objects_v2(Bucket=BUCKET, Prefix=base, MaxKeys=1).get("KeyCount", 0) == 0:
        s3.put_object(Bucket=BUCKET, Key=base + ".keep", Body=b"")
        print(f"[Bronze] Created prefix marker s3://{BUCKET}/{base}.keep")
    else:
        print(f"[Bronze] Prefix exists: s3://{BUCKET}/{base}")

def ingest_inspectorate_folder():
    import pandas as pd
    import boto3
    from botocore.client import Config

    s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT, region_name=AWS_REGION,
                      config=Config(signature_version="s3v4"))

    # Pick up top-level Excel files not previously marked processed (we don't rename/touch inbox)
    excel_paths = list(INBOX_DIR.glob("*.xlsx")) + list(INBOX_DIR.glob("*.xls"))
    if not excel_paths:
        print(f"[Ingest] No Excel files in {INBOX_DIR}")
        return

    run_day = datetime.utcnow().strftime("%Y%m%d")
    print(f"[Ingest] Found {len(excel_paths)} workbook(s): {[p.name for p in excel_paths]}")

    for src in sorted(excel_paths):
        marker_key = _processed_marker_key(src)
        if _is_already_processed(s3, BUCKET, marker_key):
            print(f"[Skip] Already processed (marker exists): s3://{BUCKET}/{marker_key}")
            continue

        # Open workbook
        try:
            # openpyxl handles xlsx; .xls will be handled by pandas via xlrd-like engines if present
            xls = pd.ExcelFile(src, engine="openpyxl") if src.suffix.lower() == ".xlsx" else pd.ExcelFile(src)
        except Exception as e:
            print(f"[Skip] Cannot open workbook {src.name}: {e}")
            continue

        dataset = _dataset_from_filename(src)
        uploaded_keys = []
        sheets_ok = 0

        for sheet in xls.sheet_names:
            sheet_norm = _norm(sheet) or "sheet"
            try:
                df = xls.parse(sheet_name=sheet)
            except Exception as e:
                print(f"[Warn] read_sheet failed {src.name}::{sheet}: {e}")
                continue

            # Normalize columns
            df.columns = [_norm(str(c)) for c in df.columns]

            # Stabilize types
            for c in df.columns:
                lc = c.lower()
                if "date" in lc or lc.endswith("_at") or lc.endswith("_on"):
                    # Leave format unspecified; coerce problematic entries to NaT
                    df[c] = pd.to_datetime(df[c], errors="coerce")

            for c in df.columns:
                if df[c].dtype == "object":
                    df[c] = pd.to_numeric(df[c], errors="ignore")
                if df[c].dtype == "object":
                    df[c] = df[c].astype("string")

            # Write parquet to a guaranteed temp path
            ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
            with tempfile.NamedTemporaryFile(suffix=".parquet",
                                             prefix=f"{DIRECTORATE}_{dataset}_{sheet_norm}_{ts}_",
                                             delete=False) as tmpf:
                tmp_path = Path(tmpf.name)

            try:
                df.to_parquet(tmp_path, index=False)  # pyarrow engine by default
            except Exception as e:
                print(f"[Warn] to_parquet failed {src.name}::{sheet}: {e}")
                tmp_path.unlink(missing_ok=True)
                continue

            # Upload under bronze/inspectorate/<dataset>/<sheet>/<yyyymmdd>/
            day_prefix = f"{BRONZE_ROOT}/{dataset}/{sheet_norm}/{run_day}/"
            if s3.list_objects_v2(Bucket=BUCKET, Prefix=day_prefix, MaxKeys=1).get("KeyCount", 0) == 0:
                s3.put_object(Bucket=BUCKET, Key=day_prefix + ".keep", Body=b"")

            key = day_prefix + tmp_path.name
            s3.upload_file(str(tmp_path), BUCKET, key)
            uploaded_keys.append(key)
            sheets_ok += 1
            print(f"[Bronze] {src.name}::{sheet} → s3://{BUCKET}/{key}")

            tmp_path.unlink(missing_ok=True)

        # Mark as processed in S3 if any sheet succeeded
        if sheets_ok > 0:
            payload = {
                "file": src.name,
                "directorate": DIRECTORATE,
                "dataset": dataset,
                "run_day": run_day,
                "uploaded_keys": uploaded_keys,
                "marked_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
            s3.put_object(
                Bucket=BUCKET,
                Key=marker_key,
                Body=json.dumps(payload).encode("utf-8"),
                ContentType="application/json",
            )
            print(f"[Marked] s3://{BUCKET}/{marker_key}")
        else:
            print(f"[Ingest] No sheets written for {src.name}; left as-is (no marker).")

# ---------- DAG ----------
with DAG(
    dag_id="bronze_inspectorate_ingest",
    start_date=datetime(2025, 9, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": "airflow"},
    tags=["bronze", "inspectorate", "parquet", "no-spark"],
) as dag:
    t0 = PythonOperator(
        task_id="ensure_directorate_prefixes",
        python_callable=ensure_directorate_prefixes,
    )
    t1 = PythonOperator(
        task_id="ingest_inspectorate_folder",
        python_callable=ingest_inspectorate_folder,
    )

    t0 >> t1
