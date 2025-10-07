# dags/silver_dps_gcp_fact.py
from __future__ import annotations
import os, re, json, tempfile, hashlib
from datetime import datetime, datetime as _dt
from pathlib import Path
from typing import Dict, List, Optional

from airflow import DAG
from airflow.operators.python import PythonOperator

# ---------- ENV ----------
CATALOG_WAREHOUSE = os.environ["CATALOG_WAREHOUSE"]          # e.g., s3://warehouse
CATALOG_S3_ENDPOINT = os.environ["CATALOG_S3_ENDPOINT"]      # e.g., http://minio:9000
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
# Force a specific bronze day (YYYYMMDD). If unset -> autodetect latest.
FORCE_RUN_DAY = os.environ.get("GCP_SILVER_RUN_DAY", "").strip()

# ---------- CONSTANTS ----------
DIRECTORATE = "dps"
DATASET = "gcp"
BRONZE_ROOT = f"bronze/{DIRECTORATE}/{DATASET}"              # bronze/dps/gcp/<sheet>/<day>/*.parquet
SILVER_ROOT = f"silver/{DIRECTORATE}/{DATASET}"              # silver/dps/gcp/_union/<day>/*.parquet

# ---------- DERIVED ----------
assert CATALOG_WAREHOUSE.startswith("s3://"), "CATALOG_WAREHOUSE must look like s3://bucket[/...]"
BUCKET = CATALOG_WAREHOUSE.replace("s3://", "").split("/")[0]
S3_ENDPOINT = CATALOG_S3_ENDPOINT

# ---------- SCHEMA / CLEANING HELPERS ----------
def _norm(name: str) -> str:
    return re.sub(r"[^a-z0-9_]+", "_", str(name).strip().lower()).strip("_")

def _dedupe_columns(cols: List[str]) -> List[str]:
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

# canonical header mapping
_CANON_MAP: Dict[str, str] = {
    "title": "title",
    "cta_number": "cta_number",
    "cta_no": "cta_number",
    "cta": "cta_number",
    "pi": "pi",
    "principal_investigator": "pi",
    "sites": "sites",
    "site": "sites",
    "justification_for_inspection": "justification_for_inspection",
    "justification": "justification_for_inspection",
    "inspectors": "inspectors",
    "inspector": "inspectors",
    "date_of_report": "date_of_report",
    "date": "date_of_report",
    "report_date": "date_of_report",
    "compliance_status": "compliance_status",
    "status": "compliance_status",
    "capa_verified": "capa_verified",
    "capa_verfied": "capa_verified",
    "capa": "capa_verified",
    "date_of_capa_verification": "date_of_capa_verification",
    "capa_verification_date": "date_of_capa_verification",
}

# final silver columns (fact-like base + derived + lineage)
_SILVER_COLS = [
    # business
    "title",
    "cta_number",
    "pi",
    "sites",
    "justification_for_inspection",
    "inspectors",
    "date_of_report",
    "compliance_status",
    "capa_verified",
    "date_of_capa_verification",
    # derived
    "fiscal_year",
    "fiscal_quarter",
    "report_year_month",
    "inspection_id",
    # lineage
    "source_sheet",
    "source_file",
    "bronze_run_day",
    "silver_marked_at",
]

_STATUS_MAP = {
    "compliant": "compliant",
    "minor": "minor",
    "major": "major",
    "critical": "critical",
    "crit": "critical",
    "non_compliant": "major",
    "noncompliant": "major",
    "na": "unknown",
    "n_a": "unknown",
    "n_a_": "unknown",
    "": "unknown",
}

def _canonize_columns(df):
    cols = [_norm(c) for c in df.columns]
    cols = _dedupe_columns(cols)
    cols = [_CANON_MAP.get(c, c) for c in cols]
    df.columns = cols
    return df

def _safe_to_datetime(series, *, dayfirst=True):
    import pandas as pd
    from pandas import Series
    from pandas.api.types import is_datetime64_any_dtype
    if not isinstance(series, Series):
        return series
    if is_datetime64_any_dtype(series):
        return series
    def _scalar(v):
        return (v is None) or isinstance(v, (str, int, float, _dt))
    s2 = series.where(series.map(_scalar), None)
    try:
        return pd.to_datetime(s2, errors="coerce", dayfirst=dayfirst, utc=True)
    except Exception as e:
        print("[DateParseSkip] column '%s': %s; leaving as string" % (series.name, e))
        return series.astype("string")

def _clean_strings(df):
    for c in df.columns:
        if df[c].dtype == "object" or str(df[c].dtype) == "string":
            df[c] = df[c].astype("string").str.strip().str.replace(r"\s+", " ", regex=True)
            df[c] = df[c].replace({"": None})
    return df

def _to_bool(series):
    import pandas as pd
    true_vals = {"y","yes","true","1","t","verified","done"}
    false_vals = {"n","no","false","0","f","pending","not_verified","nv"}
    s = series.astype("string").str.strip().str.lower()
    return s.map(lambda v: (True if v in true_vals else (False if v in false_vals else pd.NA)))

def _norm_status(series):
    import pandas as pd
    s = series.astype("string").str.strip().str.lower().str.replace(" ", "_")
    def _map(v):
        if pd.isna(v):
            return "unknown"
        v = str(v)
        if v in _STATUS_MAP:
            return _STATUS_MAP[v]
        return v if v in {"compliant", "minor", "major", "critical", "unknown"} else "unknown"
    return s.map(_map)

# Uganda-style FY (Jul 1 – Jun 30)
def _derive_fy(dt: Optional[_dt], fallback_sheet: Optional[str]) -> Optional[str]:
    # Prefer from actual date_of_report
    if isinstance(dt, _dt):
        y = dt.year
        m = dt.month
        if m >= 7:
            return f"{y}_{y+1}"
        else:
            return f"{y-1}_{y}"
    # Fallback: parse from sheet name like fy_2017_18, fy2019_2020, fy_24_25, etc.
    if fallback_sheet:
        s = fallback_sheet.lower().replace(" ", "_")
        m = re.search(r"fy[_\-]?(\d{2,4})[_\-]?(\d{2,4})", s)
        if m:
            a, b = m.group(1), m.group(2)
            a = int(a if len(a) == 4 else ("20" + a) if int(a) < 70 else ("19" + a))
            b = int(b if len(b) == 4 else ("20" + b) if int(b) < 70 else ("19" + b))
            return f"{a}_{b}"
    return None

def _derive_fq(dt: Optional[_dt]) -> Optional[str]:
    if not isinstance(dt, _dt):
        return None
    # FY Q1 = Jul–Sep, Q2 = Oct–Dec, Q3 = Jan–Mar, Q4 = Apr–Jun
    y = dt.year
    m = dt.month
    if m >= 7:
        fy = y
        q = 1 if m <= 9 else 2 if m <= 12 else None
    else:
        fy = y - 1
        q = 3 if m <= 3 else 4
    return f"FY{str(fy)[-2:]}Q{q}"

def _report_ym(dt: Optional[_dt]) -> Optional[str]:
    if not isinstance(dt, _dt):
        return None
    return dt.strftime("%Y-%m")

def _inspection_id(row) -> str:
    # stable SHA1 over a small business key (lowercased strings)
    parts = [
        str(row.get("cta_number") or "").strip().lower(),
        str(row.get("pi") or "").strip().lower(),
        str(row.get("title") or "").strip().lower(),
        str(row.get("date_of_report") or ""),
    ]
    return hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()

# ---------- S3 HELPERS ----------
def _boto3():
    import boto3
    from botocore.client import Config
    return boto3.client("s3", endpoint_url=S3_ENDPOINT, region_name=AWS_REGION,
                        config=Config(signature_version="s3v4"))

def _list_sheets(s3):
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=f"{BRONZE_ROOT}/", Delimiter="/")
    prefixes = [x["Prefix"] for x in resp.get("CommonPrefixes", [])]
    return [p.split("/")[-2] for p in prefixes]

def _list_days_for_sheet(s3, sheet: str):
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=f"{BRONZE_ROOT}/{sheet}/", Delimiter="/")
    prefixes = [x["Prefix"] for x in resp.get("CommonPrefixes", [])]
    return [p.split("/")[-2] for p in prefixes]

def _latest_day(s3) -> Optional[str]:
    if FORCE_RUN_DAY:
        return FORCE_RUN_DAY
    days = set()
    for sheet in _list_sheets(s3):
        for d in _list_days_for_sheet(s3, sheet):
            if re.fullmatch(r"\d{8}", d):
                days.add(d)
    return max(days) if days else None

def _list_parquet_keys_for_day(s3, day: str):
    keys = []
    for sheet in _list_sheets(s3):
        prefix = f"{BRONZE_ROOT}/{sheet}/{day}/"
        cont = None
        while True:
            kw = dict(Bucket=BUCKET, Prefix=prefix)
            if cont:
                kw["ContinuationToken"] = cont
            resp = s3.list_objects_v2(**kw)
            for obj in resp.get("Contents", []):
                if obj["Key"].endswith(".parquet"):
                    keys.append((sheet, obj["Key"]))
            if resp.get("IsTruncated"):
                cont = resp.get("NextContinuationToken")
            else:
                break
    return keys

def _download_to_tmp(s3, key: str) -> Path:
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
    tmp.close()
    s3.download_file(BUCKET, key, tmp.name)
    return Path(tmp.name)

def _silver_union_prefix(day: str) -> str:
    return f"{SILVER_ROOT}/_union/{day}/"

def _silver_marker_key(day: str) -> str:
    return f"_processed_markers/silver/{DIRECTORATE}/{DATASET}/{day}.done"

# ---------- TASKS ----------
def ensure_silver_prefix():
    s3 = _boto3()
    base = f"{SILVER_ROOT}/_union/"
    if s3.list_objects_v2(Bucket=BUCKET, Prefix=base, MaxKeys=1).get("KeyCount", 0) == 0:
        s3.put_object(Bucket=BUCKET, Key=base + ".keep", Body=b"")
        print(f"[Silver] Created union prefix marker s3://{BUCKET}/{base}.keep")
    else:
        print(f"[Silver] Prefix exists: s3://{BUCKET}/{base}")

def bronze_to_silver_gcp_fact():
    import pandas as pd

    s3 = _boto3()
    day = _latest_day(s3)
    if not day:
        print("[Silver] No bronze partition to process.")
        return

    marker = _silver_marker_key(day)
    if s3.list_objects_v2(Bucket=BUCKET, Prefix=marker, MaxKeys=1).get("KeyCount", 0) > 0:
        print(f"[Skip] Silver already processed for day {day}: s3://{BUCKET}/{marker}")
        return

    pairs = _list_parquet_keys_for_day(s3, day)
    if not pairs:
        print(f"[Silver] No parquet files under bronze/dps/gcp/*/{day}")
        return

    print(f"[Silver] Processing (union) day={day}, files={len(pairs)}")

    frames = []
    sheet_counts = {}

    for sheet, key in pairs:
        p = _download_to_tmp(s3, key)
        try:
            df = pd.read_parquet(p)
        except Exception as e:
            print(f"[Warn] Could not read parquet {key}: {e}")
            p.unlink(missing_ok=True)
            continue

        # Canonicalize headers
        df = _canonize_columns(df)

        # Dates (safe)
        for c in list(df.columns):
            lc = c.lower()
            if (
                lc in {"date_of_report", "date_of_capa_verification"} or
                "date" in lc or lc.endswith("_at") or lc.endswith("_on")
            ):
                df[c] = _safe_to_datetime(df[c], dayfirst=True)

        # Objects -> numeric if possible, else string
        for c in df.columns:
            if str(df[c].dtype).startswith("datetime"):
                continue
            if df[c].dtype == "object":
                df[c] = pd.to_numeric(df[c], errors="ignore")
            if df[c].dtype == "object":
                df[c] = df[c].astype("string")
        df = _clean_strings(df)

        # Field standardization
        if "capa_verified" in df.columns:
            df["capa_verified"] = _to_bool(df["capa_verified"])
        if "compliance_status" in df.columns:
            df["compliance_status"] = _norm_status(df["compliance_status"])

        # Ensure required columns exist
        for col in _SILVER_COLS:
            if col not in df.columns:
                df[col] = None

        # Derived
        # pick FY from date if possible, otherwise from sheet name
        fy_vals = []
        fq_vals = []
        ym_vals = []
        for idx, dt in enumerate(df["date_of_report"]):
            fy = _derive_fy(dt, sheet)
            fy_vals.append(fy)
            fq_vals.append(_derive_fq(dt) if fy else None)
            ym_vals.append(_report_ym(dt))
        df["fiscal_year"] = fy_vals
        df["fiscal_quarter"] = fq_vals
        df["report_year_month"] = ym_vals

        # lineage
        df["source_sheet"] = sheet
        df["source_file"] = key.split("/")[-1]
        df["bronze_run_day"] = day
        df["silver_marked_at"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

        # keep only our schema columns (order)
        df = df[_SILVER_COLS]

        # Drop rows that are empty on key fields
        keep_mask = df[["title", "cta_number", "pi", "date_of_report"]].notna().any(axis=1)
        df = df[keep_mask]

        # Add inspection_id (after filtering)
        df["inspection_id"] = df.apply(_inspection_id, axis=1)

        frames.append(df)
        sheet_counts[sheet] = int(sheet_counts.get(sheet, 0) + len(df))

        p.unlink(missing_ok=True)

    if not frames:
        print("[Silver] Nothing to write after cleaning.")
        return

    out_df = pd.concat(frames, ignore_index=True)

    # de-dup by business key; keep earliest by date/title
    dedup_keys = ["inspection_id"]
    out_df = out_df.sort_values(["date_of_report", "title"], na_position="last") \
                   .drop_duplicates(subset=dedup_keys, keep="first")

    # Write union output
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    union_prefix = _silver_union_prefix(day)
    if s3.list_objects_v2(Bucket=BUCKET, Prefix=union_prefix, MaxKeys=1).get("KeyCount", 0) == 0:
        s3.put_object(Bucket=BUCKET, Key=union_prefix + ".keep", Body=b"")

    with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet",
                                     prefix=f"{DIRECTORATE}_{DATASET}_silver_{day}_{ts}_") as tmpf:
        union_path = Path(tmpf.name)

    try:
        out_df.to_parquet(union_path, index=False, compression="snappy")
    except Exception:
        union_path.unlink(missing_ok=True)
        raise

    union_key = f"{union_prefix}{union_path.name}"
    s3.upload_file(str(union_path), BUCKET, union_key)
    print(f"[Silver] UNION wrote {len(out_df):,} rows → s3://{BUCKET}/{union_key}")
    union_path.unlink(missing_ok=True)

    # Marker with manifest
    marker = _silver_marker_key(day)
    payload = {
        "day": day,
        "rows_total": int(len(out_df)),
        "sheets": sheet_counts,   # {"fy_2018_2019": 123, ...}
        "union_key": union_key,
        "marked_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    s3.put_object(Bucket=BUCKET, Key=marker, Body=json.dumps(payload).encode("utf-8"),
                  ContentType="application/json")
    print(f"[Marked] s3://{BUCKET}/{marker}")

# ---------- DAG ----------
with DAG(
    dag_id="silver_dps_gcp_fact",
    start_date=datetime(2025, 9, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": "airflow"},
    tags=["silver", "dps", "gcp", "fact", "parquet", "no-spark"],
) as dag:

    t0 = PythonOperator(task_id="ensure_silver_prefix", python_callable=ensure_silver_prefix)
    t1 = PythonOperator(task_id="bronze_to_silver_gcp_fact", python_callable=bronze_to_silver_gcp_fact)
    t0 >> t1
