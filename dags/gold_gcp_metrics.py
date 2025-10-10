# dags/gold_gcp_metrics.py
from __future__ import annotations
import os, re, json, tempfile, shutil
from datetime import datetime
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator

# ---------- ENV ----------
CATALOG_WAREHOUSE = os.environ["CATALOG_WAREHOUSE"]          # e.g., s3://warehouse
CATALOG_S3_ENDPOINT = os.environ["CATALOG_S3_ENDPOINT"]      # e.g., http://minio:9000
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
FORCE_SILVER_DAY = os.environ.get("GCP_SILVER_RUN_DAY", "").strip()  # optional YYYYMMDD

# ---------- CONSTANTS ----------
DIRECTORATE = "dps"
DATASET = "gcp"
SILVER_ROOT = f"silver/{DIRECTORATE}/{DATASET}"   # silver/dps/gcp/**/<day>/*.parquet
GOLD_ROOT   = f"gold/{DIRECTORATE}/{DATASET}"     # gold/dps/gcp/<table>/<day>/*.parquet

# ---------- DERIVED ----------
assert CATALOG_WAREHOUSE.startswith("s3://")
BUCKET = CATALOG_WAREHOUSE.replace("s3://", "").split("/")[0]
S3_ENDPOINT = CATALOG_S3_ENDPOINT

# ---------- S3 HELPERS ----------
def _boto3():
    import boto3
    from botocore.client import Config
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        region_name=AWS_REGION,
        config=Config(signature_version="s3v4")
    )

def _list_all_silver_parquet_keys(s3, day: str | None = None):
    keys = []
    cont = None
    while True:
        kw = dict(Bucket=BUCKET, Prefix=f"{SILVER_ROOT}/")
        if cont:
            kw["ContinuationToken"] = cont
        resp = s3.list_objects_v2(**kw)
        for obj in resp.get("Contents", []):
            k = obj["Key"]
            if not k.endswith(".parquet"):
                continue
            if day and f"/{day}/" not in k:
                continue
            keys.append(k)
        if resp.get("IsTruncated"):
            cont = resp.get("NextContinuationToken")
        else:
            break
    return keys

def _download_keys_to_tmp(s3, keys):
    tmpdir = Path(tempfile.mkdtemp(prefix="gcp_silver_"))
    for k in keys:
        local = tmpdir / k.split("/")[-1]
        s3.download_file(BUCKET, k, str(local))
    return tmpdir

def _ensure_gold_prefixes():
    s3 = _boto3()
    for sub in (
        "snapshot",
        "overview",
        "monthly_totals",
        "monthly_by_status",
        "fy_totals",
        "by_pi",
        "by_site",
    ):
        base = f"{GOLD_ROOT}/{sub}/"
        if s3.list_objects_v2(Bucket=BUCKET, Prefix=base, MaxKeys=1).get("KeyCount", 0) == 0:
            s3.put_object(Bucket=BUCKET, Key=base + ".keep", Body=b"")
            print(f"[Gold] Created prefix marker s3://{BUCKET}/{base}.keep")

def _latest_silver_day(s3) -> str | None:
    days = set()
    cont = None
    while True:
        kw = dict(Bucket=BUCKET, Prefix=f"{SILVER_ROOT}/")
        if cont:
            kw["ContinuationToken"] = cont
        resp = s3.list_objects_v2(**kw)
        for obj in resp.get("Contents", []):
            k = obj["Key"]
            m = re.search(r"/(\d{8})/", k)
            if m:
                days.add(m.group(1))
        if resp.get("IsTruncated"):
            cont = resp.get("NextContinuationToken")
        else:
            break
    return max(days) if days else None

# ---------- TASK ----------
def compute_kpis_from_silver():
    import duckdb
    import pandas as pd
    s3 = _boto3()
    run_day = FORCE_SILVER_DAY or _latest_silver_day(s3)
    if not run_day:
        print("[Gold] No silver partitions found."); return

    keys = _list_all_silver_parquet_keys(s3, day=run_day)
    if not keys:
        print(f"[Gold] No silver parquet files for day={run_day}"); return

    tmpdir = _download_keys_to_tmp(s3, keys)
    print(f"[Local] Downloaded {len(keys)} silver parquet(s) to {tmpdir}")

    try:
        duckdb.sql("INSTALL json; LOAD json;")
        duckdb.read_parquet(f"{tmpdir}/*.parquet", union_by_name=True).create_view("silver")

        # <<< this line fixes your error >>>
        duckdb.sql("DROP VIEW IF EXISTS silver_norm")

        duckdb.sql("""
            CREATE OR REPLACE VIEW silver_norm AS
            WITH base AS (
              SELECT
                CAST(title AS VARCHAR)        AS title,
                CAST(cta_number AS VARCHAR)   AS cta_number,
                CAST(pi AS VARCHAR)           AS pi,
                CAST(sites AS VARCHAR)        AS sites,
                CAST(justification_for_inspection AS VARCHAR) AS justification_for_inspection,
                CAST(inspectors AS VARCHAR)   AS inspectors,
                CAST(source_sheet AS VARCHAR) AS source_sheet,
                CAST(actual_date AS TIMESTAMP) AS actual_date,
                CASE
                  WHEN lower(trim(CAST(compliance_status AS VARCHAR))) IN
                       ('compliant','yes','y','true','1','pass','passed','ok','meets','meets requirements','c')
                    THEN 'compliant'
                  WHEN CAST(compliance_status AS VARCHAR) IS NULL OR trim(CAST(compliance_status AS VARCHAR)) = ''
                    THEN NULL
                  ELSE 'non_compliant'
                END AS compliance_status,
                CAST(inspection_id AS VARCHAR) AS inspection_id
              FROM silver
            ),
            with_sheet_fy AS (
              SELECT
                *,
                REGEXP_EXTRACT(source_sheet, '(?:^|[^0-9])(?:(?:fy)[-_]?)?(\\d{4})[_-]?(\\d{2,4})', 1) AS fy_a,
                REGEXP_EXTRACT(source_sheet, '(?:^|[^0-9])(?:(?:fy)[-_]?)?(\\d{4})[_-]?(\\d{2,4})', 2) AS fy_b_raw
              FROM base
            ),
            derived AS (
              SELECT
                title, cta_number, pi, sites, justification_for_inspection, inspectors, source_sheet,
                actual_date, compliance_status, inspection_id,
                CASE
                  WHEN fy_a IS NOT NULL AND fy_b_raw IS NOT NULL THEN
                    CASE
                      WHEN length(fy_b_raw)=2 THEN concat(fy_a, '_', concat(substr(fy_a,1,2), fy_b_raw))
                      ELSE concat(fy_a, '_', fy_b_raw)
                    END
                  WHEN actual_date IS NULL THEN NULL
                  WHEN EXTRACT(MONTH FROM actual_date) >= 7
                    THEN concat(EXTRACT(YEAR FROM actual_date)::INT, '_', (EXTRACT(YEAR FROM actual_date)::INT + 1))
                  ELSE concat((EXTRACT(YEAR FROM actual_date)::INT - 1), '_', EXTRACT(YEAR FROM actual_date)::INT)
                END AS fiscal_year,
                CAST(date_trunc('month', actual_date) AS DATE) AS report_month
              FROM with_sheet_fy
            )
            SELECT * FROM derived
        """)

        # KPIs
        overview = duckdb.sql("""
            SELECT
              COUNT(*)::BIGINT AS total_inspections,
              COUNT(*) FILTER (WHERE compliance_status='compliant')::BIGINT     AS compliant_count,
              COUNT(*) FILTER (WHERE compliance_status='non_compliant')::BIGINT AS non_compliant_count
            FROM silver_norm
        """).df()

        snapshot = overview.copy()
        tot  = int(snapshot["total_inspections"].iloc[0]) if len(snapshot) else 0
        comp = int(snapshot["compliant_count"].iloc[0]) if len(snapshot) else 0
        snapshot["compliance_rate"] = (comp / tot) if tot else 0.0

        monthly_totals = duckdb.sql("""
            SELECT report_month AS month, COUNT(*)::BIGINT AS total_inspections
            FROM silver_norm
            WHERE report_month IS NOT NULL
            GROUP BY 1 ORDER BY 1
        """).df()

        monthly_by_status = duckdb.sql("""
            SELECT report_month AS month, compliance_status, COUNT(*)::BIGINT AS inspections
            FROM silver_norm
            WHERE report_month IS NOT NULL AND compliance_status IN ('compliant','non_compliant')
            GROUP BY 1,2 ORDER BY 1,2
        """).df()

        fy_totals = duckdb.sql("""
            SELECT fiscal_year, COUNT(*)::BIGINT AS total_inspections
            FROM silver_norm
            WHERE fiscal_year IS NOT NULL
            GROUP BY 1 ORDER BY 1
        """).df()

        by_pi = duckdb.sql("""
            SELECT COALESCE(NULLIF(TRIM(pi), ''), 'Unknown') AS pi,
                   COUNT(*)::BIGINT AS inspections
            FROM silver_norm
            GROUP BY 1
            ORDER BY inspections DESC, pi
        """).df()

        by_site = duckdb.sql("""
            WITH exploded AS (
              SELECT TRIM(site) AS site
              FROM (
                SELECT UNNEST(regexp_split_to_array(COALESCE(sites, ''), ',|;')) AS site
                FROM silver_norm
              )
              WHERE TRIM(site) <> ''
            )
            SELECT site, COUNT(*)::BIGINT AS inspections
            FROM exploded
            GROUP BY 1
            ORDER BY inspections DESC, site
        """).df()

        # Write to S3 (gold)
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        _ensure_gold_prefixes()

        def _upload_df(df: pd.DataFrame, subdir: str, name: str):
            out = Path(tempfile.mkstemp(prefix=f"{subdir}__", suffix=".parquet")[1])
            df.to_parquet(out, index=False)
            key_prefix = f"{GOLD_ROOT}/{subdir}/{run_day}/"
            _boto3().put_object(Bucket=BUCKET, Key=key_prefix + ".keep", Body=b"")
            key = f"{key_prefix}{name}_{ts}.parquet"
            _boto3().upload_file(str(out), BUCKET, key)
            out.unlink(missing_ok=True)
            print(f"[Gold] Wrote {len(df):,} rows → s3://{BUCKET}/{key}")

        _upload_df(snapshot,          "snapshot",           "snapshot")
        _upload_df(overview,          "overview",           "overview")
        _upload_df(monthly_totals,    "monthly_totals",     "monthly_totals")
        _upload_df(monthly_by_status, "monthly_by_status",  "monthly_by_status")
        _upload_df(fy_totals,         "fy_totals",          "fy_totals")
        _upload_df(by_pi,             "by_pi",              "by_pi")
        _upload_df(by_site,           "by_site",            "by_site")

        marker_key = f"_processed_markers/gold/{DIRECTORATE}/{DATASET}/{run_day}.done"
        payload = {"run_day": run_day, "written_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"), "inputs": keys}
        _boto3().put_object(Bucket=BUCKET, Key=marker_key,
                            Body=json.dumps(payload).encode("utf-8"),
                            ContentType="application/json")
        print(f"[Marked] s3://{BUCKET}/{marker_key}")
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


# ---------- DAG ----------
with DAG(
    dag_id="gold_dps_gcp_kpis",
    start_date=datetime(2025, 9, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": "airflow"},
    tags=["gold", "dps", "gcp", "kpis", "duckdb", "no-spark"],
) as dag:
    PythonOperator(task_id="compute_kpis_from_silver", python_callable=compute_kpis_from_silver)
