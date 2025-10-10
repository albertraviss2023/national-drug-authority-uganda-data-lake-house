# app.py
import os
from pathlib import Path
from typing import Tuple, List, Optional

import pandas as pd
import streamlit as st
import altair as alt

# ---------- ENV ----------
CATALOG_WAREHOUSE = os.environ.get("CATALOG_WAREHOUSE", "s3://warehouse")
CATALOG_S3_ENDPOINT = os.environ.get("CATALOG_S3_ENDPOINT", "http://minio:9000")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
BUCKET = CATALOG_WAREHOUSE.replace("s3://", "").split("/")[0]

GOLD_BASE = f"{BUCKET}/gold/dps/gcp"

# ---------- THEME ----------
ORG_NAME = "National Drug Authority"
PRIMARY = "#007C3E"  # NDA green
ACCENT = "#159C56"
DANGER = "#B00020"
TEXT_DARK = "#1b1b1b"

st.set_page_config(page_title="GCP KPIs — NDA", page_icon="✅", layout="wide")

# ---------- STYLES ----------
st.markdown(
    f"""
    <style>
      html, body, [class*="css"] {{ font-size: 16px !important; }}
      .block-container {{ padding-top: 0.6rem; }}
      .hdr {{ display:flex; align-items:center; gap:18px; margin-bottom: 4px; }}
      .hdr-title {{
        color:{PRIMARY}; font-weight: 900; line-height: 1.1;
        font-size: clamp(24px, 4.2vw, 40px);
      }}
      .hdr-sub {{ color:#555; margin-top:-3px; }}
      .metric-card {{
        background:{PRIMARY}; color:#fff; border-radius:16px;
        padding:18px 16px; box-shadow:0 8px 20px rgba(0,0,0,.08);
      }}
      .metric-card h3 {{ margin:0; font-size:14px; font-weight:600; opacity:.95; }}
      .metric-card .big {{ margin-top:2px; font-size:34px; font-weight:900; }}
      .metric-card .sub {{ margin-top:2px; font-size:13px; opacity:.95; }}
      table thead th {{ font-size: 14px !important; }}
      td, th {{ font-size: 14px !important; }}
    </style>
    """,
    unsafe_allow_html=True,
)

# ---------- HEADER ----------
logo_path = Path("logo.png")
c1, c2 = st.columns([0.12, 0.88])
with c1:
    if logo_path.exists():
        st.image(str(logo_path), use_container_width=True)
with c2:
    st.markdown(
        f'<div class="hdr"><div class="hdr-title">{ORG_NAME}</div></div>',
        unsafe_allow_html=True,
    )
    st.markdown('<div class="hdr-sub">GCP Inspections — Gold KPIs</div>', unsafe_allow_html=True)
st.divider()

# ---------- S3 HELPERS ----------
@st.cache_resource(show_spinner=False)
def get_fs():
    import s3fs
    return s3fs.S3FileSystem(
        client_kwargs={"endpoint_url": CATALOG_S3_ENDPOINT, "region_name": AWS_REGION}
    )

@st.cache_data(show_spinner=False)
def list_days(fs) -> List[str]:
    # Prefer snapshot to enumerate run days; fallback to monthly_totals
    snap = fs.glob(f"{GOLD_BASE}/snapshot/*")
    days = [Path(p).name for p in snap if Path(p).name.isdigit()]
    if not days:
        mt = fs.glob(f"{GOLD_BASE}/monthly_totals/*")
        days = [Path(p).name for p in mt if Path(p).name.isdigit()]
    return sorted(set(days), reverse=True)

@st.cache_data(show_spinner=False)
def read_gold(fs, subdir: str, day: str) -> pd.DataFrame:
    # gold/dps/gcp/<subdir>/<day>/*.parquet
    pats = fs.glob(f"{GOLD_BASE}/{subdir}/{day}/*.parquet")
    frames: List[pd.DataFrame] = []
    for p in pats:
        try:
            with fs.open(p, "rb") as f:
                frames.append(pd.read_parquet(f))
        except Exception as e:
            st.warning(f"Failed to read {p}: {e}")
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

# ---------- LOAD ----------
fs = get_fs()
days = list_days(fs)
if not days:
    st.error("No gold KPI partitions found under gold/dps/gcp.")
    st.stop()

sel_day = st.selectbox(
    "Gold partition (run_day)", days, index=0, help="From gold/dps/gcp/*/<YYYYMMDD>/"
)

# Prefer marts/snapshot; fallback if missing
snapshot        = read_gold(fs, "snapshot", sel_day)
mart_trends     = read_gold(fs, "mart_trends", sel_day)
mart_pi         = read_gold(fs, "mart_pi", sel_day)
mart_sites      = read_gold(fs, "mart_sites", sel_day)
mart_timeliness = read_gold(fs, "mart_timeliness", sel_day)

monthly_totals    = read_gold(fs, "monthly_totals", sel_day) if mart_trends.empty else pd.DataFrame()
monthly_by_status = read_gold(fs, "monthly_by_status", sel_day) if mart_trends.empty else pd.DataFrame()
by_pi             = read_gold(fs, "by_pi", sel_day) if mart_pi.empty else pd.DataFrame()
by_site           = read_gold(fs, "by_site", sel_day) if mart_sites.empty else pd.DataFrame()
fy_totals         = read_gold(fs, "fy_totals", sel_day)

# ---------- NORMALIZE TYPES ----------
def _coerce_month_col(df: pd.DataFrame, col: str) -> pd.DataFrame:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
    return df

if not mart_trends.empty:
    _coerce_month_col(mart_trends, "month")
else:
    _coerce_month_col(monthly_totals, "month")
    _coerce_month_col(monthly_by_status, "month")

# ---------- KPI CARDS ----------
def metric_card(title: str, value: str, note: Optional[str] = None, tooltip: Optional[str] = None):
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    st.markdown(f'<h3 title="{tooltip or ""}">{title}</h3>', unsafe_allow_html=True)
    st.markdown(f'<div class="big">{value}</div>', unsafe_allow_html=True)
    if note:
        st.markdown(f'<div class="sub">{note}</div>', unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

k1, k2, k3, k4 = st.columns(4)

if not snapshot.empty:
    # Expect columns: total_inspections, compliant_count, non_compliant_count, compliance_rate
    tot = int(snapshot.get("total_inspections", pd.Series([0])).iloc[0])
    comp = int(snapshot.get("compliant_count", pd.Series([0])).iloc[0])
    ncomp = int(snapshot.get("non_compliant_count", pd.Series([0])).iloc[0])
    cr = snapshot.get("compliance_rate", pd.Series([comp / tot if tot else 0])).iloc[0]
    try:
        cr = float(cr)
    except Exception:
        cr = 0.0
    if cr > 1:  # if stored as percent
        cr = cr / 100.0
else:
    # Fallback from monthly tables
    if not monthly_totals.empty:
        tot = int(monthly_totals["total_inspections"].sum())
    else:
        tot = 0
    if not monthly_by_status.empty:
        comp = int(
            monthly_by_status.loc[
                monthly_by_status["compliance_status"] == "compliant", "inspections"
            ].sum()
        )
        ncomp = int(
            monthly_by_status.loc[
                monthly_by_status["compliance_status"] == "non_compliant", "inspections"
            ].sum()
        )
    elif not mart_trends.empty:
        comp = int(mart_trends.get("compliant", pd.Series([0])).sum())
        ncomp = int(mart_trends.get("non_compliant", pd.Series([0])).sum())
    else:
        comp = ncomp = 0
    cr = (comp / tot) if tot else 0

with k1:
    metric_card("Total Inspections", f"{tot:,}", "All periods", "Count of all inspections")
with k2:
    metric_card("Compliant", f"{comp:,}", "All periods", "Inspections with compliance_status = compliant")
with k3:
    metric_card(
        "Non-Compliant",
        f"{ncomp:,}",
        "All periods",
        "Inspections with compliance_status = non_compliant",
    )
with k4:
    metric_card("Compliance Rate", f"{cr*100:.1f}%", "Compliant / Total", "Share of inspections that are compliant")

st.divider()

# ---------- FILTERS ----------
status_options = ["compliant", "non_compliant"]
pi_options = (
    sorted(set(mart_pi["pi"].dropna().astype(str)))
    if not mart_pi.empty
    else (sorted(set(by_pi["pi"].dropna().astype(str))) if not by_pi.empty else [])
)
site_options = (
    sorted(set(mart_sites["site"].dropna().astype(str)))
    if not mart_sites.empty
    else (sorted(set(by_site["site"].dropna().astype(str))) if not by_site.empty else [])
)

fc1, fc2, fc3 = st.columns(3)
with fc1:
    sel_status = st.multiselect("Compliance Status", status_options, default=status_options)
with fc2:
    sel_pi = st.multiselect("Principal Investigator (PI)", pi_options[:50], help="Type to search", default=[])
with fc3:
    sel_site = st.multiselect("Site", site_options[:50], help="Type to search", default=[])

# ---------- TRENDS ----------
st.subheader("Trend — inspections over time")

if not mart_trends.empty:
    trend = mart_trends.copy()
    base = trend.rename(columns={"month": "Month"}).sort_values("Month")

    total_bar = (
        alt.Chart(base)
        .mark_bar(opacity=0.55, color=PRIMARY)
        .encode(x=alt.X("Month:T", title="Month"), y=alt.Y("total_inspections:Q", title="Total inspections"))
    )

    # compliant/non_compliant lines
    value_cols = [c for c in ["compliant", "non_compliant"] if c in base.columns]
    line_data = base.melt(id_vars=["Month"], value_vars=value_cols, var_name="status", value_name="inspections")
    if sel_status:
        line_data = line_data[line_data["status"].isin(sel_status)]

    line = (
        alt.Chart(line_data)
        .mark_line(point=True)
        .encode(
            x="Month:T",
            y=alt.Y("inspections:Q", title=""),
            color=alt.Color("status:N", scale=alt.Scale(domain=["compliant", "non_compliant"], range=[ACCENT, DANGER])),
            tooltip=["Month:T", "status:N", "inspections:Q"],
        )
    )

    st.altair_chart((total_bar + line).resolve_scale(y="independent"), use_container_width=True)

else:
    # Fallback from monthly tables
    if monthly_totals.empty and monthly_by_status.empty:
        st.info("No trend data available.")
    else:
        base = monthly_totals.rename(columns={"month": "Month"}).sort_values("Month")
        total_bar = (
            alt.Chart(base)
            .mark_bar(opacity=0.55, color=PRIMARY)
            .encode(x=alt.X("Month:T", title="Month"), y=alt.Y("total_inspections:Q", title="Total inspections"))
        )
        if not monthly_by_status.empty:
            lines = monthly_by_status.copy().rename(columns={"month": "Month"})
            if sel_status:
                lines = lines[lines["compliance_status"].isin(sel_status)]
            line = (
                alt.Chart(lines)
                .mark_line(point=True)
                .encode(
                    x="Month:T",
                    y=alt.Y("inspections:Q", title=""),
                    color=alt.Color(
                        "compliance_status:N",
                        scale=alt.Scale(domain=["compliant", "non_compliant"], range=[ACCENT, DANGER]),
                    ),
                    tooltip=["Month:T", "compliance_status:N", "inspections:Q"],
                )
            )
            st.altair_chart((total_bar + line).resolve_scale(y="independent"), use_container_width=True)
        else:
            st.altair_chart(total_bar, use_container_width=True)

st.divider()

# ---------- TOP PIs ----------
st.subheader("Top PIs")
pi_df = mart_pi if not mart_pi.empty else by_pi
if not pi_df.empty:
    if sel_pi:
        pi_df = pi_df[pi_df["pi"].astype(str).isin(sel_pi)]
    top_pi = (
        pi_df.rename(columns={"inspections": "Inspections", "pi": "PI"})
        .sort_values("Inspections", ascending=False)
        .head(20)
    )
    st.dataframe(top_pi, use_container_width=True)
else:
    st.info("No PI data available.")

# ---------- TOP Sites ----------
st.subheader("Top Sites")
site_df = mart_sites if not mart_sites.empty else by_site
if not site_df.empty:
    if sel_site:
        site_df = site_df[site_df["site"].astype(str).isin(sel_site)]
    top_site = (
        site_df.rename(columns={"inspections": "Inspections", "site": "Site"})
        .sort_values("Inspections", ascending=False)
        .head(20)
    )
    st.dataframe(top_site, use_container_width=True)
else:
    st.info("No Site data available.")

st.divider()

# ---------- Timeliness (if produced) ----------
if not mart_timeliness.empty:
    st.subheader("Report timeliness")

    # Coerce month column
    for c in ["report_month", "month"]:
        if c in mart_timeliness.columns:
            mart_timeliness[c] = pd.to_datetime(mart_timeliness[c], errors="coerce", utc=True)

    # Harmonize to 'Month'
    if "report_month" in mart_timeliness.columns:
        mart_timeliness = mart_timeliness.rename(columns={"report_month": "Month"})
    elif "month" in mart_timeliness.columns:
        mart_timeliness = mart_timeliness.rename(columns={"month": "Month"})

    keep = ["on_time", "late"]
    have = [c for c in keep if c in mart_timeliness.columns]
    if have:
        melted = mart_timeliness.melt(id_vars=["Month"], value_vars=have, var_name="timeliness", value_name="count")
        chart = (
            alt.Chart(melted)
            .mark_bar()
            .encode(
                x=alt.X("Month:T", title="Month"),
                y=alt.Y("count:Q", title="Reports"),
                color=alt.Color("timeliness:N", scale=alt.Scale(domain=["on_time", "late"], range=[ACCENT, DANGER])),
                tooltip=["Month:T", "timeliness:N", "count:Q"],
            )
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("Timeliness columns not found.")

# ---------- FY table (troubleshooting) ----------
if not fy_totals.empty:
    st.subheader("FY totals")
    disp = fy_totals.rename(columns={"fiscal_year": "FY", "total_inspections": "Inspections"})
    st.dataframe(disp.sort_values("FY"), use_container_width=True)

st.caption(f"Data source: s3://{GOLD_BASE} • partition: {sel_day}")
