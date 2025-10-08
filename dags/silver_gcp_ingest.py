# dags/silver_dps_gcp_mirror.py
from __future__ import annotations
import os, re, json, tempfile, hashlib
from datetime import datetime, datetime as _dt
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from airflow import DAG
from airflow.operators.python import PythonOperator

# ---------- ENV ----------
CATALOG_WAREHOUSE = os.environ["CATALOG_WAREHOUSE"]          # e.g., s3://warehouse
CATALOG_S3_ENDPOINT = os.environ["CATALOG_S3_ENDPOINT"]      # e.g., http://minio:9000
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
FORCE_RUN_DAY = os.environ.get("GCP_SILVER_RUN_DAY", "").strip()  # optional YYYYMMDD (backfill)

# ---------- CONSTANTS ----------
DIRECTORATE = "dps"
DATASET = "gcp"
BRONZE_ROOT = f"bronze/{DIRECTORATE}/{DATASET}"              # bronze/dps/gcp/<sheet>/<day>/*.parquet
SILVER_ROOT = f"silver/{DIRECTORATE}/{DATASET}"              # silver/dps/gcp/<sheet>/<day>/*.parquet

# ---------- DERIVED ----------
assert CATALOG_WAREHOUSE.startswith("s3://"), "CATALOG_WAREHOUSE must look like s3://bucket[/...]"
BUCKET = CATALOG_WAREHOUSE.replace("s3://", "").split("/")[0]
S3_ENDPOINT = CATALOG_S3_ENDPOINT

# ---------- Cleaning helpers ----------
def _norm(name: str) -> str:
    return re.sub(r"[^a-z0-9_]+", "_", str(name).strip().lower()).strip("_")

def _dedupe_columns(cols: List[str]) -> List[str]:
    seen, out = {}, []
    for c in cols:
        base = c or "col"
        n = seen.get(base, 0)
        out.append(base if n == 0 else f"{base}_{n}")
        seen[base] = n + 1
    return out

_CANON_MAP: Dict[str, str] = {
    "title":"title",
    "cta_number":"cta_number","cta_no":"cta_number","cta":"cta_number",
    "pi":"pi","principal_investigator":"pi",
    "sites":"sites","site":"sites","trial_sites":"sites",
    "justification_for_inspection":"justification_for_inspection","justification":"justification_for_inspection",
    "inspectors":"inspectors","inspector":"inspectors",
    "date_of_report":"date_of_report","date":"date_of_report","report_date":"date_of_report",
    "compliance_status":"compliance_status","status":"compliance_status",
    "capa_verified":"capa_verified","capa_verfied":"capa_verified","capa":"capa_verified",
    "date_of_capa_verification":"date_of_capa_verification","capa_verification_date":"date_of_capa_verification",
    # inspection date aliases -> actual_date
    "inspection_date":"actual_date","inspection_dates":"actual_date",
    "date_of_inspection":"actual_date","actual_date":"actual_date","actual_dates":"actual_date",
}

_SILVER_COLS = [
    "title","cta_number","pi","sites","justification_for_inspection","inspectors",
    "actual_date","date_of_report","compliance_status","capa_verified","date_of_capa_verification",
    "report_year_month","inspection_id",
    "source_sheet","source_file","bronze_run_day","silver_marked_at",
]

def _canonize_columns(df):
    cols = [_norm(c) for c in df.columns]
    df.columns = [_CANON_MAP.get(c, c) for c in _dedupe_columns(cols)]
    return df

def _maybe_promote_header(df):
    cols = [str(c).lower() for c in df.columns]
    unnamed_like = sum(1 for c in cols if c.startswith("unnamed") or c.startswith("col") or c.strip() in {"", "nan"})
    if unnamed_like >= max(3, int(0.5 * len(cols))):
        header_idx = None
        for i in range(min(5, len(df))):
            if df.iloc[i].notna().sum() >= max(3, int(0.5 * len(cols))):
                header_idx = i; break
        if header_idx is not None:
            new_cols = [_norm(v) for v in df.iloc[header_idx].astype("string").fillna("").tolist()]
            new_cols = _dedupe_columns([_CANON_MAP.get(c, c) for c in new_cols])
            df = df.iloc[header_idx+1:].reset_index(drop=True)
            df.columns = new_cols
            print(f"[Header] Promoted row {header_idx} to header")
    return df

def _safe_to_datetime(series, *, dayfirst=True):
    import pandas as pd
    from pandas.api.types import is_datetime64_any_dtype, is_integer_dtype, is_float_dtype, is_object_dtype, is_string_dtype
    if is_datetime64_any_dtype(series):
        try:  return series.dt.tz_convert("UTC")
        except Exception:
            try:  return series.dt.tz_localize("UTC")
            except Exception: return series
    if is_integer_dtype(series) or is_float_dtype(series):
        return pd.to_datetime(series, errors="coerce", unit="D", origin="1899-12-30", utc=True)
    if is_object_dtype(series) or is_string_dtype(series):
        return pd.to_datetime(series, errors="coerce", dayfirst=dayfirst, utc=True)
    try:
        return pd.to_datetime(series, errors="coerce", utc=True)
    except Exception:
        return pd.to_datetime(series.astype("string"), errors="coerce", utc=True)

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

def _to_binary_status(series):
    import pandas as pd
    s = series.astype("string").str.strip().str.lower()
    s = s.str.replace(r"[\-/]", " ", regex=True).str.replace(r"\s+", " ", regex=True).str.strip()
    compliant = {"compliant","yes","y","ok","pass","passed","meets","meets requirements","c"}
    noncompliant = {"non compliant","noncompliant","non_compliant","no","n","fail","failed","does not meet","nc"}
    def decide(v):
        if v is None or v == "" or v in {"na","n/a","null","none"}: return None
        lv = v.lower()
        if lv in compliant: return "compliant"
        if lv in noncompliant or lv.startswith("non "): return "non_compliant"
        return "non_compliant"  # binary default
    return s.map(decide)

def _report_ym_series(s):
    import pandas as pd
    if "datetime64" not in str(s.dtype):
        return pd.Series([None] * len(s), index=s.index)
    try:
        naive = s.dt.tz_convert(None)
    except Exception:
        naive = s.dt.tz_localize(None)
    out = naive.dt.strftime("%Y-%m")
    return out.where(~naive.isna(), None)

def _build_inspection_id(df):
    import pandas as pd
    for col in ["cta_number","pi","title","actual_date","date_of_report"]:
        if col not in df.columns: df[col] = pd.NA

    cta = df["cta_number"].astype("string").str.strip().str.lower().fillna("")
    pi  = df["pi"].astype("string").str.strip().str.lower().fillna("")
    ttl = df["title"].astype("string").str.strip().str.lower().fillna("")

    def _date_to_str(s):
        if "datetime64" in str(s.dtype):
            try: naive = s.dt.tz_convert(None)
            except Exception:
                naive = s.dt.tz_localize(None)
            return naive.dt.strftime("%Y-%m-%d").fillna("")
        s2 = _safe_to_datetime(s, dayfirst=True)
        try: naive = s2.dt.tz_convert(None)
        except Exception:
            naive = s2.dt.tz_localize(None)
        return naive.dt.strftime("%Y-%m-%d").fillna("")

    ad = _date_to_str(df["actual_date"])
    rd = _date_to_str(df["date_of_report"])
    date_pick = ad.where(ad != "", rd)
    keys = cta + "|" + pi + "|" + ttl + "|" + date_pick
    return keys.map(lambda s: hashlib.sha1(s.encode("utf-8")).hexdigest())

# ---------- S3 helpers ----------
def _boto3():
    import boto3
    from botocore.client import Config
    return boto3.client("s3", endpoint_url=S3_ENDPOINT, region_name=AWS_REGION,
                        config=Config(signature_version="s3v4"))

def _list_sheets(s3) -> List[str]:
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=f"{BRONZE_ROOT}/", Delimiter="/")
    return [x["Prefix"].split("/")[-2] for x in resp.get("CommonPrefixes", [])]

def _list_days_for_sheet(s3, sheet: str) -> List[str]:
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=f"{BRONZE_ROOT}/{sheet}/", Delimiter="/")
    return [x["Prefix"].split("/")[-2] for x in resp.get("CommonPrefixes", [])]

def _latest_day(s3) -> Optional[str]:
    if FORCE_RUN_DAY: return FORCE_RUN_DAY
    days = set()
    for sheet in _list_sheets(s3):
        for d in _list_days_for_sheet(s3, sheet):
            if re.fullmatch(r"\d{8}", d): days.add(d)
    return max(days) if days else None

def _list_parquet_pairs_for_day(s3, day: str) -> List[Tuple[str,str,str]]:
    """Return (sheet, day, bronze_key) for each parquet under bronze."""
    pairs = []
    for sheet in _list_sheets(s3):
        prefix = f"{BRONZE_ROOT}/{sheet}/{day}/"
        cont = None
        while True:
            kw = dict(Bucket=BUCKET, Prefix=prefix)
            if cont: kw["ContinuationToken"] = cont
            resp = s3.list_objects_v2(**kw)
            for obj in resp.get("Contents", []):
                key = obj["Key"]
                if key.endswith(".parquet"):
                    pairs.append((sheet, day, key))
            if resp.get("IsTruncated"): cont = resp.get("NextContinuationToken")
            else: break
    return pairs

def _download_to_tmp(s3, key: str) -> Path:
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet"); tmp.close()
    s3.download_file(BUCKET, key, tmp.name)
    return Path(tmp.name)

def _mk_out_key(sheet: str, day: str, bronze_filename: str) -> str:
    base = bronze_filename[:-8] if bronze_filename.lower().endswith(".parquet") else bronze_filename
    return f"{SILVER_ROOT}/{sheet}/{day}/{base}_clean.parquet"

def _file_marker_key(sheet: str, day: str, bronze_key: str) -> str:
    # one marker per bronze object, placed under a stable namespace
    digest = hashlib.sha1(bronze_key.encode("utf-8")).hexdigest()
    return f"_processed_markers/silver/{DIRECTORATE}/{DATASET}/{sheet}/{day}/{digest}.done"

# ---------- Tasks ----------
def ensure_silver_prefix():
    s3 = _boto3()
    base = f"{SILVER_ROOT}/"
    if s3.list_objects_v2(Bucket=BUCKET, Prefix=base, MaxKeys=1).get("KeyCount", 0) == 0:
        s3.put_object(Bucket=BUCKET, Key=base + ".keep", Body=b"")
        print(f"[Silver] Created base prefix s3://{BUCKET}/{base}.keep")
    else:
        print(f"[Silver] Prefix exists: s3://{BUCKET}/{base}")

def bronze_to_silver_gcp_mirror():
    import pandas as pd

    s3 = _boto3()
    day = _latest_day(s3)
    if not day:
        print("[Silver] No bronze partition to process."); return

    pairs = _list_parquet_pairs_for_day(s3, day)
    if not pairs:
        print(f"[Silver] No parquet files under {BRONZE_ROOT}/*/{day}"); return

    print(f"[Silver] Processing day={day} (ONE-to-ONE mirror of bronze structure)")
    total_out = 0

    for sheet, d, bronze_key in pairs:
        src_file = bronze_key.split("/")[-1]
        out_key = _mk_out_key(sheet, d, src_file)
        marker_key = _file_marker_key(sheet, d, bronze_key)

        # idempotency: skip if marker OR output already exists
        if s3.list_objects_v2(Bucket=BUCKET, Prefix=marker_key, MaxKeys=1).get("KeyCount", 0) > 0:
            print(f"[Skip] Marker exists → {bronze_key}"); continue
        if s3.list_objects_v2(Bucket=BUCKET, Prefix=out_key, MaxKeys=1).get("KeyCount", 0) > 0:
            print(f"[Skip] Output exists → s3://{BUCKET}/{out_key}"); 
            # still write a marker to stop further re-evals
            s3.put_object(Bucket=BUCKET, Key=marker_key, Body=b"{}", ContentType="application/json")
            continue

        # read bronze
        p = _download_to_tmp(s3, bronze_key)
        try:
            raw = pd.read_parquet(p)
        except Exception as e:
            print(f"[Warn] Could not read {bronze_key}: {e}")
            p.unlink(missing_ok=True); continue
        finally:
            p.unlink(missing_ok=True)

        # cleaning pipeline
        df = _maybe_promote_header(raw)
        df = _canonize_columns(df)

        # parse dates (incl. actual_date/report/capa dates)
        for c in list(df.columns):
            lc = c.lower()
            if lc in {"actual_date","date_of_report","date_of_capa_verification"} or \
               "date" in lc or lc.endswith("_at") or lc.endswith("_on"):
                df[c] = _safe_to_datetime(df[c], dayfirst=True)

        # preferred actual_date
        if "actual_date" not in df.columns:
            df["actual_date"] = None
        if "date_of_report" in df.columns:
            df["actual_date"] = df["actual_date"].where(~df["actual_date"].isna(), df["date_of_report"])

        # types + strings
        for c in df.columns:
            if str(df[c].dtype).startswith("datetime"): continue
            if df[c].dtype == "object":
                df[c] = pd.to_numeric(df[c], errors="ignore")
            if df[c].dtype == "object":
                df[c] = df[c].astype("string")
        df = _clean_strings(df)

        # binary status + CAPA
        if "compliance_status" in df.columns:
            df["compliance_status"] = _to_binary_status(df["compliance_status"])
        if "capa_verified" in df.columns:
            df["capa_verified"] = _to_bool(df["capa_verified"])

        # ensure columns
        for col in _SILVER_COLS:
            if col not in df.columns: df[col] = None

        # derived
        df["report_year_month"] = _report_ym_series(df["actual_date"])

        # lineage
        df["source_sheet"] = sheet
        df["source_file"] = src_file
        df["bronze_run_day"] = d
        df["silver_marked_at"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

        # order + filter empties
        df = df[_SILVER_COLS]
        keep_mask = df[["title","cta_number","pi","actual_date","date_of_report"]].notna().any(axis=1)
        df = df[keep_mask].copy()

        # inspection_id (vectorized)
        df["inspection_id"] = _build_inspection_id(df)

        # de-dupe within file
        df = df.sort_values(["actual_date","title"], na_position="last") \
               .drop_duplicates(subset=["inspection_id"], keep="first")

        # ensure target prefix exists
        out_prefix = f"{SILVER_ROOT}/{sheet}/{d}/"
        if s3.list_objects_v2(Bucket=BUCKET, Prefix=out_prefix, MaxKeys=1).get("KeyCount", 0) == 0:
            s3.put_object(Bucket=BUCKET, Key=out_prefix + ".keep", Body=b"")

        # write using STABLE filename (no timestamp)
        with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as tmpf:
            out_path = Path(tmpf.name)
        try:
            df.to_parquet(out_path, index=False, compression="snappy")
            s3.upload_file(str(out_path), BUCKET, out_key)
        finally:
            out_path.unlink(missing_ok=True)

        total_out += len(df)
        print(f"[Silver] Wrote {len(df):,} → s3://{BUCKET}/{out_key}")

        # marker
        manifest = {
            "bronze_key": bronze_key,
            "sheet": sheet,
            "day": d,
            "output": out_key,
            "rows": int(len(df)),
            "marked_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        s3.put_object(Bucket=BUCKET, Key=marker_key, Body=json.dumps(manifest).encode("utf-8"),
                      ContentType="application/json")

    print(f"[Silver] Done. Rows written total: {total_out:,}")

# ---------- DAG ----------
with DAG(
    dag_id="silver_dps_gcp_mirror",
    start_date=datetime(2025, 9, 1),
    schedule=None,      # manual / triggered only
    catchup=False,
    default_args={"owner": "airflow"},
    tags=["silver", "dps", "gcp", "mirror", "no-spark"],
) as dag:
    t0 = PythonOperator(task_id="ensure_silver_prefix", python_callable=ensure_silver_prefix)
    t1 = PythonOperator(task_id="bronze_to_silver_gcp_mirror", python_callable=bronze_to_silver_gcp_mirror)
    t0 >> t1
