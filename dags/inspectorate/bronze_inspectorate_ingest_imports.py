# dags/bronze_inspectorate_imports_ingest.py
from __future__ import annotations
import os, re, json, tempfile, warnings
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# ---------- ENV ----------
CATALOG_WAREHOUSE = os.environ["CATALOG_WAREHOUSE"]          # e.g., s3://warehouse
CATALOG_S3_ENDPOINT = os.environ["CATALOG_S3_ENDPOINT"]      # e.g., http://minio:9000
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
AIRFLOW_DATA_DIR = os.environ.get("AIRFLOW_DATA_DIR", "/opt/airflow/data")  # repo mount

# ---------- CONSTANTS ----------
DIRECTORATE = "inspectorate"

# ---------- DERIVED ----------
assert CATALOG_WAREHOUSE.startswith("s3://"), "CATALOG_WAREHOUSE must look like s3://bucket[/...]"
BUCKET = CATALOG_WAREHOUSE.replace("s3://", "").split("/")[0]
S3_ENDPOINT = CATALOG_S3_ENDPOINT

warnings.filterwarnings("ignore", message="Could not infer format")

# ---------- HELPERS ----------
def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9_]+", "_", str(s).strip().lower()).strip("_")

def _dedupe_columns(cols):
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

def _schema_for(dataset: str):
    # Return TARGET, DATE, NUM, ALIASES (ALIASES keys normalized by _norm)
    if dataset == "td":
        TARGET = [
            "id","tracking_no","reference_no","SubmissionDate","query_ref","query_remark","queried_on","responded_on",
            "NDAApplicationType","DeclarantType","buzzType","premise_reg_no","premise_name","Manufacturer","consignee_id",
            "ConsigneeType","Consignee","ShipmentCategory","ShipmentMode","transportDocument","transport_document_number",
            "ShipmentDate","expected_arrival_date","clearingAgent","pack_list_number","product_id","brand_name",
            "permitbrand_name","manufacturer_name","atc_code_id","name","gmdn_code","approved_qty","qty_shipped",
            "total_weight","batch_qty","batch_units","hs_code_id","hs_code_description","supplementary_value","pack_size",
            "dosageFor","BatchNo","product_expiry_date","product_manufacturing_date","units_for_quantity","UNITofQuantity",
            "pack_price","currency_id","Currency","vc_no","date_released","workflow_stage_name","created_by","no_of_packs",
        ]
        DATE = {
            "SubmissionDate","queried_on","responded_on","ShipmentDate","expected_arrival_date",
            "product_expiry_date","product_manufacturing_date","date_released",
        }
        NUM = {
            "approved_qty","qty_shipped","total_weight","batch_qty","supplementary_value",
            "pack_size","pack_price","no_of_packs",
        }
        ALIASES = {
            "shipmentctegory":"ShipmentCategory",
        }
        return TARGET, DATE, NUM, ALIASES

    if dataset == "vc":
        TARGET = [
            "id","submission_date","tracking_no","reference_no","query_ref","query_remark","queried_on","QueryRespondedOn",
            "NDAApplicationType","premise_name","DomesticManufacturer","buzzType","VCType","ConsigneeStatus","consigneeName",
            "ShipmentCtegory","shipment_date","importation_reason_id","government_grant_id","name",
            "importationReason","reason_for_authorisation","sender_receiver_id","Supplier","country_id","SupplierCountry",
            "brand_name","total_value","current_stage","date_received","date_released","name_2","created_by",
        ]
        DATE = {"submission_date","queried_on","QueryRespondedOn","shipment_date","date_received","date_released"}
        NUM  = {"importation_reason_id","government_grant_id","total_value","country_id"}
        ALIASES = {
            "queryrespondedon":"QueryRespondedOn",
            "shipmentcategory":"ShipmentCtegory",
            "shipmentctegory":"ShipmentCtegory",
            "consigneestatus":"ConsigneeStatus",
            "consigneename":"consigneeName",
            "suppliercountry":"SupplierCountry",
            "importationreason":"importationReason",
            "reasonforauthorisation":"reason_for_authorisation",
            "name_1":"name_2","name1":"name_2","name_01":"name_2","name_2":"name_2",
        }
        return TARGET, DATE, NUM, ALIASES

    if dataset == "licensing":
        TARGET = [
            "Date of Application","Date of Invoicng","Date of Payment","Manager Review Date","Director Review Date",
            "SA Approval Date","Query Number","Query Reason","Query Date","Qury Resonse Date","TurnAroundTime",
            "NDAApplicationType","LicenceType","Business Type","invoice_amount","Fees Paid",
        ]
        DATE = {
            "Date of Application","Date of Invoicng","Date of Payment","Manager Review Date","Director Review Date",
            "SA Approval Date","Query Date","Qury Resonse Date",
        }
        NUM  = {"invoice_amount","Fees Paid","TurnAroundTime"}
        ALIASES = {
            "dateofinvoicing":"Date of Invoicng",
            "queryresponsedate":"Qury Resonse Date",
            "queryresponse_date":"Qury Resonse Date",
            "licencetype":"LicenceType",
            # handle trailing spaces seen in sample: "Date of Invoicng "
            "date_of_invoicng":"Date of Invoicng",
        }
        return TARGET, DATE, NUM, ALIASES

    raise ValueError(f"Unknown dataset: {dataset}")

# --- cleaning helpers (FR/EN numbers, dates, NAs) ---
import pandas as pd, numpy as np
from pandas.api.types import is_datetime64_any_dtype, is_datetime64tz_dtype, is_integer_dtype, is_float_dtype

NA_LITERALS = {"\\n","\\N","na","n/a","null",""}

def _clean_na(x):
    if x is None or (isinstance(x,float) and np.isnan(x)):
        return pd.NA
    s = str(x).strip()
    return pd.NA if _norm(s) in NA_LITERALS else s

_CURRENCY_RE = re.compile(r"[^\d,.\-]")

def _to_float_series(s: pd.Series) -> pd.Series:
    s = s.astype("string").map(lambda v: "" if pd.isna(v) else str(v).replace("\xa0"," ").strip())
    s = s.map(lambda v: _CURRENCY_RE.sub("", v))
    def _norm_num(v: str) -> str:
        if v in {"", "-", ".", ",", "-.", "-,"}:
            return ""
        has_c = "," in v; has_d = "." in v
        if has_c and has_d:
            v = v.replace(",", "")       # 1,234.56 -> 1234.56
        elif has_c and not has_d:
            v = v.replace(",", ".")      # 10,25 -> 10.25
        return v
    s = s.map(_norm_num)
    out = pd.to_numeric(s, errors="coerce")
    return out.astype("Float64")

def _to_date_series(s: pd.Series) -> pd.Series:
    if is_datetime64_any_dtype(s) or is_datetime64tz_dtype(s):
        return s
    s_str = s.astype("string").map(lambda v: None if pd.isna(v) else str(v).strip())
    parsed = pd.to_datetime(s_str, errors="coerce", dayfirst=True)
    if parsed.isna().sum() == len(parsed):
        num = pd.to_numeric(s_str, errors="coerce")
        if is_integer_dtype(num) or is_float_dtype(num) or num.notna().any():
            parsed2 = pd.to_datetime(num, errors="coerce", unit="D", origin="1899-12-30")
            parsed = parsed.fillna(parsed2)
    return parsed

def _marker_key(dataset: str, src: Path, inbox_root: Path) -> str:
    rel = src.relative_to(inbox_root).as_posix()
    rel_norm = re.sub(r"[^a-zA-Z0-9._/-]+", "_", rel)
    return f"_processed_markers/{DIRECTORATE}/imports/{dataset}/{rel_norm}.done"

def _is_already_processed(s3, bucket: str, key: str) -> bool:
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=key, MaxKeys=1)
    return resp.get("KeyCount", 0) > 0

def _enforce_schema(df: pd.DataFrame, TARGET, DATE, NUM, ALIASES):
    # Clean NA-ish tokens
    for c in df.columns:
        if df[c].dtype == "object":
            df[c] = df[c].map(_clean_na)

    # Build maps
    tgt_by_norm = {_norm(c): c for c in TARGET}
    alias_map = dict(ALIASES)  # keys already normalized

    def _normalize_source_name(col: str) -> str:
        base = _norm(col)
        for suf in (".1",".2",".01","_1","_2"):
            if base.endswith(_norm(suf)):
                base = base.replace(_norm(suf), "")
        return base

    rename, seen = {}, set()
    for c in df.columns:
        n = _norm(c); base = _normalize_source_name(c)
        target = None
        if n in tgt_by_norm: target = tgt_by_norm[n]
        elif base in tgt_by_norm: target = tgt_by_norm[base]
        elif n in alias_map: target = alias_map[n]
        elif base in alias_map: target = alias_map[base]
        if target:
            if target in seen:
                k = 2
                while True:
                    cand = f"{target}_{k}"
                    if cand in TARGET and cand not in seen:
                        target = cand; break
                    k += 1
                    if k > 10: break
            rename[c] = target; seen.add(target)

    df = df.rename(columns=rename)

    for c in TARGET:
        if c not in df.columns:
            df[c] = pd.NA

    df = df[TARGET]

    for c in TARGET:
        if c in DATE:
            df[c] = _to_date_series(df[c])
    for c in TARGET:
        if c in NUM:
            df[c] = _to_float_series(df[c])
    for c in TARGET:
        if c not in DATE and c not in NUM and not str(df[c].dtype).startswith("datetime"):
            df[c] = df[c].astype("string")

    return df

# ---------- CORE INGEST ----------
def ensure_prefixes(dataset: str):
    import boto3
    from botocore.client import Config
    s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT, region_name=AWS_REGION,
                      config=Config(signature_version="s3v4"))
    base = f"bronze/{DIRECTORATE}/imports/{dataset}/"
    if s3.list_objects_v2(Bucket=BUCKET, Prefix=base, MaxKeys=1).get("KeyCount", 0) == 0:
        s3.put_object(Bucket=BUCKET, Key=base + ".keep", Body=b"")
        print(f"[Bronze] Created prefix marker s3://{BUCKET}/{base}.keep")
    else:
        print(f"[Bronze] Prefix exists: s3://{BUCKET}/{base}")

def list_excels(inbox: Path) -> list[Path]:
    if not inbox.exists(): return []
    out = []
    for p in list(inbox.rglob("*.xlsx")) + list(inbox.rglob("*.xls")):
        name = p.name
        if name.startswith("~$") or name.startswith(".") or name.endswith(".tmp"):
            continue
        out.append(p)
    return out

def ingest_dataset_folder(dataset: str):
    import pandas as pd
    import boto3
    from botocore.client import Config

    INBOX_DIR = Path(AIRFLOW_DATA_DIR) / DIRECTORATE / "imports" / dataset
    BRONZE_ROOT = f"bronze/{DIRECTORATE}/imports/{dataset}"

    s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT, region_name=AWS_REGION,
                      config=Config(signature_version="s3v4"))

    paths = list_excels(INBOX_DIR)
    if not paths:
        print(f"[Ingest] No Excel files in {INBOX_DIR}")
        return

    TARGET, DATE, NUM, ALIASES = _schema_for(dataset)
    run_day = datetime.utcnow().strftime("%Y%m%d")

    print(f"[Ingest] {dataset}: Found {len(paths)} workbook(s): {[p.name for p in paths]}")

    for src in sorted(paths):
        marker_key = _marker_key(dataset, src, INBOX_DIR)
        if _is_already_processed(s3, BUCKET, marker_key):
            print(f"[Skip] Already processed: s3://{BUCKET}/{marker_key}")
            continue

        # Open workbook
        try:
            xls = pd.ExcelFile(src, engine="openpyxl") if src.suffix.lower()==".xlsx" else pd.ExcelFile(src)
        except Exception as e:
            print(f"[Skip] Cannot open workbook {src.name}: {e}")
            continue

        uploaded_keys, sheets_ok = [], 0

        # Allow TARGET and ALIASES for fast usecols
        wanted_norm = {_norm(c) for c in TARGET}
        alias_norms = set(ALIASES.keys())
        accepted_norms = wanted_norm | alias_norms
        def _usecols(c: str) -> bool:
            return _norm(c) in accepted_norms

        for sheet in xls.sheet_names:
            sheet_norm = _norm(sheet) or "sheet"

            # Fast read with usecols
            try:
                df = pd.read_excel(xls, sheet_name=sheet, usecols=_usecols, dtype="string")
                cols_read = list(map(str, df.columns))
                print(f"[Read] {src.name}::{sheet}: cols={len(cols_read)} -> {cols_read[:8]}{'...' if len(cols_read)>8 else ''}")
            except Exception as e:
                df = None
                print(f"[Warn] read_excel(usecols) failed {src.name}::{sheet}: {e}")

            # Fallback: full read
            if df is None or df.shape[1] == 0:
                try:
                    df = xls.parse(sheet_name=sheet, dtype="string")
                    cols_full = list(map(str, df.columns))
                    print(f"[Read-Fallback] {src.name}::{sheet}: full_cols={len(cols_full)} -> {cols_full[:8]}{'...' if len(cols_full)>8 else ''}")
                except Exception as ee:
                    print(f"[Warn] read_sheet fallback failed {src.name}::{sheet}: {ee}")
                    continue

            # Normalize column names (strip whitespace differences) + dedupe
            df.columns = [c.strip() if isinstance(c, str) else c for c in df.columns]
            df.columns = _dedupe_columns(df.columns)

            # Enforce lane schema + conversions
            df = _enforce_schema(df, TARGET, DATE, NUM, ALIASES)

            if df.shape[1] == 0:
                print(f"[Skip] No target columns after enforcement {src.name}::{sheet}")
                continue
            if df.shape[0] == 0:
                print(f"[Skip] 0 rows after enforcement {src.name}::{sheet}")
                continue

            # Local parquet -> upload with boto3 (same pattern as GCP DAG)
            ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
            key_prefix = f"{BRONZE_ROOT}/{run_day}/"
            if s3.list_objects_v2(Bucket=BUCKET, Prefix=key_prefix, MaxKeys=1).get("KeyCount", 0) == 0:
                s3.put_object(Bucket=BUCKET, Key=key_prefix + ".keep", Body=b"")

            file_stub = f"{_norm(src.stem)}_{sheet_norm}_{ts}"
            s3_key = f"{key_prefix}{file_stub}.parquet"

            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmpf:
                tmp_path = Path(tmpf.name)
            try:
                df.to_parquet(tmp_path, index=False, compression="snappy")
                s3.upload_file(str(tmp_path), BUCKET, s3_key)
                uploaded_keys.append(s3_key)
                sheets_ok += 1
                print(f"[Bronze] {src.name}::{sheet} → s3://{BUCKET}/{s3_key} rows={df.shape[0]} cols={df.shape[1]}")
            except Exception as e:
                print(f"[Warn] to_parquet/upload failed {src.name}::{sheet}: {e}")
            finally:
                try: tmp_path.unlink(missing_ok=True)
                except Exception: pass

        # Mark processed if anything uploaded
        if sheets_ok > 0:
            payload = {
                "file": src.name,
                "directorate": DIRECTORATE,
                "dataset": dataset,
                "run_day": run_day,
                "uploaded_keys": uploaded_keys,
                "marked_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
            s3.put_object(Bucket=BUCKET, Key=marker_key,
                          Body=json.dumps(payload).encode("utf-8"),
                          ContentType="application/json")
            print(f"[Marked] s3://{BUCKET}/{marker_key}")
        else:
            print(f"[Ingest] No sheets written for {src.name}; left unmarked.")

# ---------- DAG FACTORY (three DAGs) ----------
def make_ingest_dag(dataset: str):
    INBOX = Path(AIRFLOW_DATA_DIR) / DIRECTORATE / "imports" / dataset
    BRONZE_ROOT = f"bronze/{DIRECTORATE}/imports/{dataset}"

    with DAG(
        dag_id=f"bronze_imports_{dataset}_ingest",
        start_date=datetime(2025, 9, 1),
        schedule=None,
        catchup=False,
        default_args={"owner":"airflow"},
        tags=["bronze","inspectorate","imports",dataset,"parquet","no-spark"],
    ) as dag:
        t0 = PythonOperator(task_id="ensure_prefixes",
                            python_callable=lambda: ensure_prefixes(dataset))
        t1 = PythonOperator(task_id="ingest_folder",
                            python_callable=lambda: ingest_dataset_folder(dataset))
        t0 >> t1
        return dag

bronze_imports_td_ingest         = make_ingest_dag("td")
bronze_imports_vc_ingest         = make_ingest_dag("vc")
bronze_imports_licensing_ingest  = make_ingest_dag("licensing")
