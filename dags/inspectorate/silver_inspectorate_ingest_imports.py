# dags/inspectorate/silver_inspectorate_imports_arrow_v2.py
from __future__ import annotations
import os, re, json, tempfile, hashlib, warnings
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import List, Optional, Dict

from airflow import DAG
from airflow.operators.python import PythonOperator

# =================== ENV ===================
CATALOG_WAREHOUSE = os.environ["CATALOG_WAREHOUSE"]          # e.g., s3://warehouse
CATALOG_S3_ENDPOINT = os.environ["CATALOG_S3_ENDPOINT"]      # e.g., http://minio:9000
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
FORCE_RUN_DAY = os.environ.get("INSPECTORATE_SILVER_RUN_DAY", "").strip()

# ================= CONSTANTS ================
DIRECTORATE = "inspectorate"
DATASET = "imports"
LANES = ["td", "vc", "licensing"]

BRONZE_ROOT = f"bronze/{DIRECTORATE}/{DATASET}"    # bronze/inspectorate/imports/<lane>/<YYYYMMDD>/*.parquet
SILVER_ROOT = f"silver/{DIRECTORATE}/{DATASET}"    # silver/inspectorate/imports/<lane>/year=YYYY/month=MM/*.parquet

# ================= DERIVED ==================
assert CATALOG_WAREHOUSE.startswith("s3://"), "CATALOG_WAREHOUSE must look like s3://bucket[/...]"
BUCKET = CATALOG_WAREHOUSE.replace("s3://", "").split("/")[0]
S3_ENDPOINT = CATALOG_S3_ENDPOINT

warnings.filterwarnings("ignore", message="Could not infer format")

# ================= S3 helpers ===============
def _boto3():
    import boto3
    from botocore.client import Config
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        region_name=AWS_REGION,
        config=Config(signature_version="s3v4"),
    )

def _ensure_prefix(s3, prefix: str):
    if s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix, MaxKeys=1).get("KeyCount", 0) == 0:
        s3.put_object(Bucket=BUCKET, Key=prefix + ".keep", Body=b"")

def _list_days_for_lane(s3, lane: str) -> List[str]:
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=f"{BRONZE_ROOT}/{lane}/", Delimiter="/")
    return [x["Prefix"].split("/")[-2] for x in resp.get("CommonPrefixes", []) if re.fullmatch(r"\d{8}", x["Prefix"].split("/")[-2])]

def _latest_day_for_lane(s3, lane: str) -> Optional[str]:
    if FORCE_RUN_DAY:
        return FORCE_RUN_DAY
    days = _list_days_for_lane(s3, lane)
    return max(days) if days else None

def _list_parquet_keys_for_lane_day(s3, lane: str, day: str) -> List[str]:
    prefix = f"{BRONZE_ROOT}/{lane}/{day}/"
    keys, token = [], None
    while True:
        kw = dict(Bucket=BUCKET, Prefix=prefix, MaxKeys=1000)
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        for obj in resp.get("Contents", []):
            k = obj["Key"]
            if k.endswith(".parquet"):
                keys.append(k)  # bucket-less key
        token = resp.get("NextContinuationToken")
        if not token:
            break
    return keys

def _marker_key(lane: str, day: str, bronze_key: str) -> str:
    import hashlib
    digest = hashlib.sha1(bronze_key.encode("utf-8")).hexdigest()
    return f"_processed_markers/silver/{DIRECTORATE}/{DATASET}/{lane}/{day}/{digest}.done"

# ============ Arrow (no pandas, no dask) ============
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pyarrow.fs as pafs

# ---- borrow: idempotent outputs & tight cleaning scope ----
NA_TOKENS = pa.array(["\\n", "\\N", "na", "n/a", "null", ""], type=pa.string())

LANE_PROJECTIONS: Dict[str, List[str]] = {
    "licensing": [
        "Date of Application","Date of Invoicng","Date of Payment",
        "Manager Review Date","Director Review Date","SA Approval Date",
        "Query Number","Query Reason","Query Date","Qury Resonse Date",
        "TurnAroundTime","NDAApplicationType","LicenceType","Business Type",
        "invoice_amount","Fees Paid",
    ],
    "vc": [
        "id","submission_date","tracking_no","reference_no","query_ref","query_remark",
        "queried_on","QueryRespondedOn","NDAApplicationType","premise_name","DomesticManufacturer",
        "buzzType","VCType","ConsigneeStatus","consigneeName","ShipmentCtegory","shipment_date",
        "importation_reason_id","government_grant_id","name","importationReason","reason_for_authorisation",
        "sender_receiver_id","Supplier","country_id","SupplierCountry","brand_name","total_value",
        "current_stage","date_received","date_released","name_2","created_by",
    ],
    "td": [
        "id","tracking_no","reference_no","SubmissionDate","query_ref","query_remark","queried_on","responded_on",
        "NDAApplicationType","DeclarantType","buzzType","premise_reg_no","premise_name","Manufacturer",
        "consignee_id","ConsigneeType","Consignee","ShipmentCategory","ShipmentMode","transportDocument",
        "transport_document_number","ShipmentDate","expected_arrival_date","clearingAgent","pack_list_number",
        "product_id","brand_name","permitbrand_name","manufacturer_name","atc_code_id","name","gmdn_code",
        "approved_qty","qty_shipped","total_weight","batch_qty","batch_units","hs_code_id","hs_code_description",
        "supplementary_value","pack_size","dosageFor","BatchNo","product_expiry_date","product_manufacturing_date",
        "units_for_quantity","UNITofQuantity","pack_price","currency_id","Currency","vc_no","date_released",
        "workflow_stage_name","created_by","no_of_packs",
    ],
}

LANE_DATE_COLS: Dict[str, List[str]] = {
    "licensing": [
        "Date of Application","Date of Invoicng","Date of Payment",
        "Manager Review Date","Director Review Date","SA Approval Date",
        "Query Date","Qury Resonse Date",
    ],
    "vc": [
        "submission_date","queried_on","QueryRespondedOn","shipment_date","date_received","date_released",
    ],
    "td": [
        "SubmissionDate","ShipmentDate","expected_arrival_date","date_released",
        "product_expiry_date","product_manufacturing_date",
    ],
}

LANE_DT_PICK: Dict[str, List[str]] = {
    "licensing": ["Date of Application","Date of Payment","Manager Review Date","Director Review Date","SA Approval Date"],
    "vc": ["date_released","date_received","submission_date","shipment_date"],
    "td": ["ShipmentDate","SubmissionDate"],
}

def _s3_fs() -> pafs.S3FileSystem:
    return pafs.S3FileSystem(
        access_key=os.environ.get("AWS_ACCESS_KEY_ID"),
        secret_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        endpoint_override=S3_ENDPOINT.replace("http://","").replace("https://",""),
        region=AWS_REGION,
        scheme=S3_ENDPOINT.split("://")[0],
    )

def _read_table_minimal(key: str) -> pa.Table:
    s3path = f"{BUCKET}/{key}"
    fs = _s3_fs()
    return pq.read_table(s3path, filesystem=fs)

def _project(tbl: pa.Table, want: List[str]) -> pa.Table:
    if not want:
        return tbl
    cols = [c for c in want if c in tbl.schema.names]
    return tbl.select(cols) if cols else tbl

def _clean_string_nulls_all(tbl: pa.Table) -> pa.Table:
    for field in tbl.schema:
        if pa.types.is_string(field.type) or pa.types.is_large_string(field.type):
            col = tbl[field.name]
            trimmed = pc.utf8_trim_whitespace(col)
            lowered = pc.utf8_lower(trimmed)
            mask = pc.is_in(lowered, value_set=NA_TOKENS)
            # FIX: use pa.scalar for typed null
            null_scalar = pa.scalar(None, type=col.type)
            cleaned = pc.if_else(mask, null_scalar, trimmed)
            tbl = tbl.set_column(tbl.schema.get_field_index(field.name), field.name, cleaned)
    return tbl

def _parse_dates_utc(tbl: pa.Table, cols: List[str]) -> pa.Table:
    for c in cols:
        if c not in tbl.schema.names:
            continue
        arr = tbl[c]
        ty = arr.type
        out = None

        if pa.types.is_timestamp(ty):
            if ty.tz is None:
                out = pc.assume_timezone(arr, "UTC")
            elif ty.tz != "UTC":
                out = pc.convert_timezone(arr, "UTC")
            else:
                out = arr

        elif pa.types.is_string(ty) or pa.types.is_large_string(ty):
            iso_dt = pc.strptime(arr, format="%Y-%m-%d %H:%M:%S", unit="us", error_is_null=True)
            mask = pc.is_null(iso_dt)
            iso_d = pc.strptime(arr, format="%Y-%m-%d", unit="us", error_is_null=True)
            out = pc.if_else(mask, iso_d, iso_dt)
            mask2 = pc.is_null(out)
            fr_dt = pc.strptime(arr, format="%d/%m/%Y %H:%M", unit="us", error_is_null=True)
            out = pc.if_else(mask2, fr_dt, out)
            mask3 = pc.is_null(out)
            fr_d = pc.strptime(arr, format="%d/%m/%Y", unit="us", error_is_null=True)
            out = pc.if_else(mask3, fr_d, out)
            out = pc.assume_timezone(out, "UTC")

        elif pa.types.is_integer(ty) or pa.types.is_floating(ty):
            # Excel serials: (serial - 25569) days from 1970-01-01
            seconds = pc.multiply(pc.subtract(pc.cast(arr, pa.float64()), pc.scalar(25569.0)), pc.scalar(86400.0))
            micros = pc.multiply(seconds, pc.scalar(1_000_000.0))
            out = pc.cast(pc.round(micros), pa.timestamp("us", tz="UTC"))

        if out is not None:
            tbl = tbl.set_column(tbl.schema.get_field_index(c), c, out)
    return tbl

def _derive_year_month(tbl: pa.Table, candidates: List[str]) -> pa.Table:
    """Derive year and month columns for partitioning"""
    pick = next((c for c in candidates if c in tbl.schema.names), None)
    if pick is None:
        # Add null year/month columns if no date column found
        if "year" not in tbl.schema.names:
            tbl = tbl.append_column("year", pa.nulls(tbl.num_rows, type=pa.int32()))
        if "month" not in tbl.schema.names:
            tbl = tbl.append_column("month", pa.nulls(tbl.num_rows, type=pa.int32()))
        return tbl

    arr = tbl[pick]
    if not pa.types.is_timestamp(arr.type):
        tbl = _parse_dates_utc(tbl, [pick])
        arr = tbl[pick]

    # Extract year and month
    year_arr = pc.year(arr)
    month_arr = pc.month(arr)
    
    # Replace or add year/month columns
    if "year" in tbl.schema.names:
        tbl = tbl.set_column(tbl.schema.get_field_index("year"), "year", year_arr)
    else:
        tbl = tbl.append_column("year", year_arr)
        
    if "month" in tbl.schema.names:
        tbl = tbl.set_column(tbl.schema.get_field_index("month"), "month", month_arr)
    else:
        tbl = tbl.append_column("month", month_arr)

    # Filter out future dates (year > current year or same year but month > current month)
    current_year = datetime.now().year
    current_month = datetime.now().month
    
    year_cond = pc.less_equal(tbl["year"], pa.scalar(current_year, type=pa.int32()))
    
    # For current year, only include months up to current month
    current_year_cond = pc.and_(
        pc.equal(tbl["year"], pa.scalar(current_year, type=pa.int32())),
        pc.less_equal(tbl["month"], pa.scalar(current_month, type=pa.int32()))
    )
    
    # For previous years, include all months
    previous_years_cond = pc.less(tbl["year"], pa.scalar(current_year, type=pa.int32()))
    
    mask_not_future = pc.or_(current_year_cond, previous_years_cond)
    tbl = tbl.filter(mask_not_future)
    
    return tbl

def _write_per_month(s3, tbl: pa.Table, lane: str, base_prefix: str):
    """Write data partitioned by year/month for better query performance"""
    if tbl.num_rows == 0 or "year" not in tbl.schema.names or "month" not in tbl.schema.names:
        return
        
    # Filter out rows with null year/month
    non_null = tbl.filter(
        pc.and_(
            pc.invert(pc.is_null(tbl["year"])),
            pc.invert(pc.is_null(tbl["month"]))
        )
    )
    
    if non_null.num_rows == 0:
        return

    # Get unique year/month combinations
    year_month_combinations = []
    years = pc.unique(non_null["year"]).to_pylist()
    for year in years:
        year_tbl = non_null.filter(pc.equal(non_null["year"], pa.scalar(year, type=pa.int32())))
        months = pc.unique(year_tbl["month"]).to_pylist()
        for month in months:
            year_month_combinations.append((year, month))

    for year, month in year_month_combinations:
        # Filter data for this year/month combination
        part = non_null.filter(
            pc.and_(
                pc.equal(non_null["year"], pa.scalar(year, type=pa.int32())),
                pc.equal(non_null["month"], pa.scalar(month, type=pa.int32()))
            )
        )
        
        if part.num_rows == 0:
            continue
            
        # Create partition directory structure
        prefix = f"{base_prefix}/year={year:04d}/month={month:02d}/"
        _ensure_prefix(s3, prefix)
        
        # Generate filename with timestamp to avoid overwrites
        timestamp = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
        fname = f"{lane}_silver_{year:04d}_{month:02d}_{timestamp}.parquet"

        # Write to temporary file and upload
        with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as tmpf:
            tmp_path = Path(tmpf.name)
        try:
            pq.write_table(part, tmp_path, compression="snappy")
            s3.upload_file(str(tmp_path), BUCKET, f"{prefix}{fname}")
            print(f"[{lane}] Wrote {part.num_rows} rows to year={year:04d}/month={month:02d}/")
        finally:
            tmp_path.unlink(missing_ok=True)

# ================== TASKS ===================
def ensure_silver_prefix():
    s3 = _boto3()
    base = f"{SILVER_ROOT}/"
    _ensure_prefix(s3, base)
    print(f"[Silver] Base prefix ensured: s3://{BUCKET}/{base}")

def curate_all_lanes_arrow():
    s3 = _boto3()
    _ensure_prefix(s3, f"{SILVER_ROOT}/")

    for lane in LANES:
        day = _latest_day_for_lane(s3, lane)
        if not day:
            print(f"[{lane}] No bronze partition found. Skipping."); continue

        keys = _list_parquet_keys_for_lane_day(s3, lane, day)
        if not keys:
            print(f"[{lane}] No parquet files under {BRONZE_ROOT}/{lane}/{day}. Skipping."); continue

        want = LANE_PROJECTIONS.get(lane, [])
        silver_base = f"{SILVER_ROOT}/{lane}"

        print(f"[{lane}] Arrow pass day={day} on {len(keys)} file(s).")

        for k in keys:
            marker = _marker_key(lane, day, k)
            if s3.list_objects_v2(Bucket=BUCKET, Prefix=marker, MaxKeys=1).get("KeyCount", 0) > 0:
                print(f"[{lane}] Skip (already marked): {k}"); continue

            tbl = _read_table_minimal(k)
            tbl = _project(tbl, want)
            tbl = _clean_string_nulls_all(tbl)
            tbl = _parse_dates_utc(tbl, LANE_DATE_COLS.get(lane, []))
            tbl = _derive_year_month(tbl, LANE_DT_PICK.get(lane, []))
            _write_per_month(s3, tbl, lane, silver_base)

            s3.put_object(
                Bucket=BUCKET,
                Key=marker,
                Body=json.dumps({
                    "bronze_key": k,
                    "lane": lane,
                    "day": day,
                    "marked_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
                }).encode("utf-8"),
                ContentType="application/json"
            )
            print(f"[{lane}] {k} → processed & marked.")

        print(f"[{lane}] Finished day={day}.")

# =================== DAG ====================
with DAG(
    dag_id="silver_inspectorate_imports_arrow_deepseek",
    start_date=datetime(2025, 9, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": "airflow"},
    tags=["silver", "inspectorate", "imports", "arrow", "no-pandas", "no-dask", "monthly-partition"],
) as dag:
    t0 = PythonOperator(task_id="ensure_silver_prefix", python_callable=ensure_silver_prefix)
    t1 = PythonOperator(task_id="curate_all_lanes_arrow", python_callable=curate_all_lanes_arrow)
    t0 >> t1