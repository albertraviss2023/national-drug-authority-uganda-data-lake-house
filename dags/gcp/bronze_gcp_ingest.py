# dags/bronze_dps_gcp_ingest.py new
from __future__ import annotations
import os, re, json, tempfile
from datetime import datetime, datetime as _dt
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# ---------- ENV ----------
CATALOG_WAREHOUSE = os.environ["CATALOG_WAREHOUSE"]          # e.g., s3://warehouse
CATALOG_S3_ENDPOINT = os.environ["CATALOG_S3_ENDPOINT"]      # e.g., http://minio:9000
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
AIRFLOW_DATA_DIR = os.environ.get("AIRFLOW_DATA_DIR", "/opt/airflow/data")  # repo mount

# ---------- CONSTANTS ----------
DIRECTORATE = "dps"
DATASET = "gcp"                                              # fixed dataset slug for this DAG
BRONZE_ROOT = f"bronze/{DIRECTORATE}/{DATASET}"              # bronze/dps/gcp
INBOX_DIR = Path(AIRFLOW_DATA_DIR) / DIRECTORATE / DATASET   # /opt/airflow/data/dps/gcp

# ---------- DERIVED ----------
assert CATALOG_WAREHOUSE.startswith("s3://"), "CATALOG_WAREHOUSE must look like s3://bucket[/...]"
BUCKET = CATALOG_WAREHOUSE.replace("s3://", "").split("/")[0]
S3_ENDPOINT = CATALOG_S3_ENDPOINT

# ---------- HELPERS ----------
def _norm(name: str) -> str:
    return re.sub(r"[^a-z0-9_]+", "_", str(name).strip().lower()).strip("_")

def _dedupe_columns(cols):
    """
    Ensure unique column names. If duplicates after normalization, append _1, _2, ...
    """
    seen = {}
    out = []
    for c in cols:
        base = c or "col"
        if base not in seen:
            seen[base] = 1
            out.append(base)
        else:
            idx = seen[base]
            seen[base] = idx + 1
            out.append(f"{base}_{idx}")
    return out

def _processed_marker_key(src: Path) -> str:
    # One marker object per workbook to prevent reprocessing
    # e.g. _processed_markers/dps/gcp/GCP_DATABASE.xlsx.done
    return f"_processed_markers/{DIRECTORATE}/{DATASET}/{src.name}.done"

def _is_already_processed(s3, bucket: str, key: str) -> bool:
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=key, MaxKeys=1)
    return resp.get("KeyCount", 0) > 0

def _safe_to_datetime(series, *, dayfirst: bool = True):
    """
    Convert a pandas Series to datetime safely:
      - If already datetime dtype, return as-is.
      - Only attempt to parse scalar-like values (str/int/float/datetime).
      - On any parsing exception, fall back to string dtype.
    If 'series' is not a Series (e.g., DataFrame due to duplicate columns), return it unchanged.
    """
    import pandas as pd
    from pandas import Series
    from pandas.api.types import is_datetime64_any_dtype

    if not isinstance(series, Series):
        # Shouldn't happen after _dedupe_columns, but be defensive.
        return series

    if is_datetime64_any_dtype(series):
        return series

    def _scalar(v):
        return (v is None) or isinstance(v, (str, int, float, _dt))

    s2 = series.where(series.map(_scalar), None)
    try:
        return pd.to_datetime(s2, errors="coerce", dayfirst=dayfirst)
    except Exception as e:
        print(f"[DateParseSkip] column '{series.name}': {e}; leaving as string")
        return series.astype("string")

# ---------- TASKS ----------
def ensure_bronze_prefix():
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

def ingest_dps_gcp_folder():
    import pandas as pd
    import boto3
    from botocore.client import Config

    s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT, region_name=AWS_REGION,
                      config=Config(signature_version="s3v4"))

    # Pick up Excel files placed in /opt/airflow/data/dps/gcp
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
            # openpyxl handles .xlsx; for .xls, leave engine unset (requires appropriate dependency if used)
            xls = pd.ExcelFile(src, engine="openpyxl") if src.suffix.lower() == ".xlsx" else pd.ExcelFile(src)
        except Exception as e:
            print(f"[Skip] Cannot open workbook {src.name}: {e}")
            continue

        uploaded_keys, sheets_ok = [], 0

        for sheet in xls.sheet_names:
            sheet_norm = _norm(sheet) or "sheet"
            try:
                df = xls.parse(sheet_name=sheet)
            except Exception as e:
                print(f"[Warn] read_sheet failed {src.name}::{sheet}: {e}")
                continue

            # Normalize column names and ensure uniqueness
            df.columns = [_norm(c) for c in df.columns]
            df.columns = _dedupe_columns(df.columns)

            # 1) Dates (safe parsing for scalar-like values only)
            for c in df.columns:
                lc = c.lower()
                if "date" in lc or lc.endswith("_at") or lc.endswith("_on"):
                    df[c] = _safe_to_datetime(df[c], dayfirst=True)

            # 2) Numerics then strings (skip columns that already became datetime)
            for c in df.columns:
                if str(df[c].dtype).startswith("datetime"):
                    continue
                if df[c].dtype == "object":
                    df[c] = pd.to_numeric(df[c], errors="ignore")
                if df[c].dtype == "object":
                    df[c] = df[c].astype("string")

            # Write parquet to a guaranteed temp path
            ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
            with tempfile.NamedTemporaryFile(
                suffix=".parquet",
                prefix=f"{DIRECTORATE}_{DATASET}_{sheet_norm}_{ts}_",
                delete=False
            ) as tmpf:
                tmp_path = Path(tmpf.name)

            try:
                df.to_parquet(tmp_path, index=False, compression="snappy")  # explicit compression
            except Exception as e:
                print(f"[Warn] to_parquet failed {src.name}::{sheet}: {e}")
                tmp_path.unlink(missing_ok=True)
                continue

            # Upload under bronze/dps/gcp/<sheet>/<yyyymmdd>/
            day_prefix = f"{BRONZE_ROOT}/{sheet_norm}/{run_day}/"
            if s3.list_objects_v2(Bucket=BUCKET, Prefix=day_prefix, MaxKeys=1).get("KeyCount", 0) == 0:
                s3.put_object(Bucket=BUCKET, Key=day_prefix + ".keep", Body=b"")

            key = day_prefix + tmp_path.name
            s3.upload_file(str(tmp_path), BUCKET, key)
            uploaded_keys.append(key)
            sheets_ok += 1
            print(f"[Bronze] {src.name}::{sheet} → s3://{BUCKET}/{key}")

            tmp_path.unlink(missing_ok=True)

        # S3 processed marker (idempotency) if anything uploaded
        if sheets_ok > 0:
            payload = {
                "file": src.name,
                "directorate": DIRECTORATE,
                "dataset": DATASET,
                "run_day": run_day,
                "uploaded_keys": uploaded_keys,
                "marked_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
            s3.put_object(Bucket=BUCKET, Key=marker_key,
                          Body=json.dumps(payload).encode("utf-8"),
                          ContentType="application/json")
            print(f"[Marked] s3://{BUCKET}/{marker_key}")
        else:
            print(f"[Ingest] No sheets written for {src.name}; left as-is (no marker).")

# ---------- DAG ----------
with DAG(
    dag_id="bronze_dps_gcp_ingest",
    start_date=datetime(2025, 9, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": "airflow"},
    tags=["bronze", "dps", "gcp", "parquet", "no-spark"],
) as dag:
    t0 = PythonOperator(task_id="ensure_bronze_prefix", python_callable=ensure_bronze_prefix)
    t1 = PythonOperator(task_id="ingest_dps_gcp_folder", python_callable=ingest_dps_gcp_folder)
    t0 >> t1
