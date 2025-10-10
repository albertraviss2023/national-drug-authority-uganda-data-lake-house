# app.py (polished)
import os
from pathlib import Path
from typing import List, Optional
import pandas as pd
import streamlit as st
import altair as alt

# ====== ENV ======
CATALOG_WAREHOUSE = os.environ.get("CATALOG_WAREHOUSE", "s3://warehouse")
CATALOG_S3_ENDPOINT = os.environ.get("CATALOG_S3_ENDPOINT", "http://minio:9000")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

assert CATALOG_WAREHOUSE.startswith("s3://")
BUCKET = CATALOG_WAREHOUSE.replace("s3://", "").split("/")[0]
GOLD_ROOT = f"{BUCKET}/gold/dps/gcp"

# ====== THEME / BRAND ======
ORG = "National Drug Authority"
PRIMARY = "#007C3E"     # NDA green
PRIMARY_DARK = "#045f31"
ACCENT = "#159C56"
DANGER = "#B00020"
TEXT = "#1b1b1b"
MUTED = "#6b7280"
BG = "#f8fafb"

st.set_page_config(page_title="GCP KPIs — NDA", page_icon="✅", layout="wide")

# Altair theme
def nda_theme():
    return {
        "config": {
            "padding": 4,
            "background": "white",
            "axis": {
                "labelFontSize": 12, "titleFontSize": 12,
                "grid": True, "gridColor": "#ecedf0", "tickColor": "#d8dbe2",
                "labelColor": TEXT, "titleColor": MUTED
            },
            "legend": {"labelFontSize": 12, "titleFontSize": 12, "orient": "top-right"},
            "title": {"fontSize": 18, "font": "Inter, Segoe UI, Arial", "color": TEXT},
            "view": {"stroke": "transparent"}
        }
    }
alt.themes.register("nda_theme", nda_theme)
alt.themes.enable("nda_theme")

# Global CSS
st.markdown(
    f"""
    <style>
      html, body, [class*="css"] {{
        background: {BG};
      }}
      .block-container {{
        padding-top: 1.2rem;   /* avoid clipping header */
      }}

      .hdr {{
        margin: 0 0 12px 0; display: flex; align-items: center; gap: 16px;
      }}
      .hdr img {{
        border-radius: 8px;
      }}
      .hdr-title {{
        color: {PRIMARY}; font-weight: 900; line-height: 1.05;
        font-size: clamp(28px, 3.8vw, 44px);
        letter-spacing: .2px;
      }}
      .hdr-sub {{ color: {MUTED}; margin-top:-4px; }}

      .kpi {{
        background: linear-gradient(135deg, {PRIMARY} 0%, {PRIMARY_DARK} 100%);
        border-radius: 16px; color: white; padding: 14px 16px;
        box-shadow: 0 6px 18px rgba(0,0,0,.08);
        min-height: 100px; display: flex; flex-direction: column; justify-content: center;
      }}
      .kpi-title {{ font-size: 12px; font-weight: 700; opacity:.95; margin: 0 0 4px 0; letter-spacing:.3px; }}
      .kpi-value {{ font-size: 34px; font-weight: 900; margin: 0; line-height: 1; }}
      .kpi-note  {{ font-size: 12px; opacity:.97; margin-top: 6px; }}

      .pill {{
        background: white; border: 1px solid #e5e7eb; border-radius: 999px;
        padding: 6px 10px; font-size: 12px; color: {MUTED};
        display:inline-flex; gap: 6px; align-items:center;
      }}

      .section-title {{ font-weight: 800; font-size: 16px; color:{TEXT}; margin-bottom: 4px; }}
      .muted {{ color:{MUTED}; font-size: 12px; }}
    </style>
    """,
    unsafe_allow_html=True,
)

# ====== S3 helpers ======
@st.cache_resource(show_spinner=False)
def get_fs():
    import s3fs
    from botocore.exceptions import ClientError, EndpointConnectionError

    key = os.environ.get("AWS_ACCESS_KEY_ID") or "minioadmin"
    secret = os.environ.get("AWS_SECRET_ACCESS_KEY") or "minioadmin"
    region = os.environ.get("AWS_REGION", "us-east-1")
    endpoints = [os.environ.get("CATALOG_S3_ENDPOINT"), "http://127.0.0.1:9000", "http://localhost:9000"]

    last_err = None
    for ep in [e for e in endpoints if e]:
        try:
            fs = s3fs.S3FileSystem(
                key=key, secret=secret, anon=False,
                client_kwargs={"endpoint_url": ep, "region_name": region},
                config_kwargs={"s3": {"addressing_style": "path", "signature_version": "s3v4"}},
            )
            _ = fs.ls(BUCKET)
            return fs
        except (EndpointConnectionError, ClientError, OSError) as e:
            last_err = e
    raise RuntimeError(f"Could not connect to MinIO. Last error: {last_err}")

def _extract_days(paths: List[str]) -> List[str]:
    out = []
    for p in paths:
        parts = Path(p).parts
        for ix in range(len(parts) - 1, 0, -1):
            name = parts[ix]
            if name.isdigit() and len(name) == 8:
                out.append(name)
                break
    return sorted(set(out), reverse=True)

@st.cache_data(show_spinner=False)
def latest_run_day(_fs) -> Optional[str]:
    for sub in ["monthly_by_status", "monthly_totals", "fy_totals", "overview"]:
        paths = _fs.glob(f"{GOLD_ROOT}/{sub}/*/*.parquet")
        days = _extract_days(paths)
        if days:
            return days[0]
    return None

@st.cache_data(show_spinner=False)
def read_gold(_fs, subdir: str, day: str) -> pd.DataFrame:
    pats = _fs.glob(f"{GOLD_ROOT}/{subdir}/{day}/*.parquet")
    frames: List[pd.DataFrame] = []
    for p in pats:
        try:
            with _fs.open(p, "rb") as f:
                frames.append(pd.read_parquet(f))
        except Exception as e:
            st.warning(f"Failed to read {p}: {e}")
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

# ====== Header ======
logo = Path("logo.png")
c1, c2 = st.columns([0.10, 0.90])
with c1:
    if logo.exists():
        st.image(str(logo), use_container_width=True)
with c2:
    st.markdown(f'<div class="hdr"><div class="hdr-title">{ORG}</div></div>', unsafe_allow_html=True)
    st.markdown('<div class="hdr-sub">GCP Inspections — Gold KPIs</div>', unsafe_allow_html=True)
st.divider()

# ====== Load latest gold ======
fs = get_fs()
day = latest_run_day(fs)
if not day:
    st.error("No gold KPI partitions found under gold/dps/gcp.")
    st.stop()

monthly_by_status = read_gold(fs, "monthly_by_status", day)
monthly_totals    = read_gold(fs, "monthly_totals", day)
fy_totals         = read_gold(fs, "fy_totals", day)
overview          = read_gold(fs, "overview", day)
by_pi             = read_gold(fs, "by_pi", day)
by_site           = read_gold(fs, "by_site", day)

# ====== Helpers ======
def add_fy_from_month(df: pd.DataFrame, month_col: str) -> pd.DataFrame:
    if month_col not in df.columns:
        return df
    out = df.copy()
    m = pd.to_datetime(out[month_col], errors="coerce", utc=True)
    y = m.dt.year
    start = (m.dt.month >= 7)
    fy_start = y.where(~start, y)        # placeholder
    fy_end = (y + 1).where(start, y)
    fy = fy_start.where(start, y - 1).astype("Int64").astype("string") + "_" + fy_end.astype("Int64").astype("string")
    out["fiscal_year"] = fy.where(~m.isna(), None)
    return out

def pct(x: float) -> str:
    try: return f"{x*100:.1f}%"
    except Exception: return "0.0%"

# Build FY list (newest first)
def fy_sort_key(v: str) -> int:
    try: return int(str(v).split("_")[0])
    except Exception: return -1

fy_list = []
if not fy_totals.empty and "fiscal_year" in fy_totals.columns:
    fy_list = sorted(fy_totals["fiscal_year"].dropna().astype(str).unique().tolist(), key=fy_sort_key, reverse=True)
elif not monthly_by_status.empty:
    fy_list = sorted(add_fy_from_month(monthly_by_status, "month")["fiscal_year"].dropna().unique().tolist(), key=fy_sort_key, reverse=True)

if not fy_list:
    st.error("Could not derive fiscal years. Check fy_totals/monthly_by_status.")
    st.stop()

# ====== Controls ======
cc1, cc2 = st.columns([0.5, 0.5])
with cc1:
    fy_sel = st.selectbox("Fiscal Year", fy_list, index=0)
with cc2:
    # statuses present in data
    opts = ["compliant", "non_compliant"]
    if "compliance_status" in monthly_by_status.columns:
        present = (
            monthly_by_status["compliance_status"]
            .dropna().astype(str).str.strip().str.lower().unique().tolist()
        )
        opts = [s for s in ["compliant", "non_compliant"] if s in present] or opts
    sel_status = st.multiselect("Statuses", opts, default=opts)

# ====== KPI computation (FY scope) ======
fy_tot = 0; fy_comp = 0; fy_ncomp = 0
if not fy_totals.empty and "fiscal_year" in fy_totals.columns:
    row = fy_totals.loc[fy_totals["fiscal_year"].astype(str) == str(fy_sel)]
    if not row.empty:
        fy_tot = int(row["total_inspections"].iloc[0])

mbs_fy = pd.DataFrame()
if not monthly_by_status.empty:
    mbs = add_fy_from_month(monthly_by_status, "month")
    mbs_fy = mbs[mbs["fiscal_year"].astype(str) == str(fy_sel)].copy()
    if not mbs_fy.empty:
        fy_comp  = int(mbs_fy.loc[mbs_fy["compliance_status"] == "compliant", "inspections"].sum())
        fy_ncomp = int(mbs_fy.loc[mbs_fy["compliance_status"] == "non_compliant", "inspections"].sum())

cr = (fy_comp / fy_tot) if fy_tot else 0.0

# ====== KPI cards ======
k1, k2, k3, k4 = st.columns(4)
with k1:
    st.markdown(f'<div class="kpi"><div class="kpi-title">Total Inspections</div><div class="kpi-value">{fy_tot:,}</div><div class="kpi-note">{fy_sel}</div></div>', unsafe_allow_html=True)
with k2:
    st.markdown(f'<div class="kpi"><div class="kpi-title">Compliant</div><div class="kpi-value">{fy_comp:,}</div><div class="kpi-note">{fy_sel}</div></div>', unsafe_allow_html=True)
with k3:
    st.markdown(f'<div class="kpi"><div class="kpi-title">Non-Compliant</div><div class="kpi-value">{fy_ncomp:,}</div><div class="kpi-note">{fy_sel}</div></div>', unsafe_allow_html=True)
with k4:
    st.markdown(f'<div class="kpi"><div class="kpi-title">Compliance Rate</div><div class="kpi-value">{pct(cr)}</div><div class="kpi-note">Compliant / Total</div></div>', unsafe_allow_html=True)

st.markdown(f'<div class="muted pill">run_day: {day}</div>', unsafe_allow_html=True)
st.divider()

# ====== OVERVIEW (Yearly compliance trend + 3 insights) ======
st.markdown('<div class="section-title">Overview</div>', unsafe_allow_html=True)

# build yearly aggregates from monthly_by_status
yearly = pd.DataFrame()
if not monthly_by_status.empty:
    yearly = add_fy_from_month(monthly_by_status, "month")
    yearly = yearly.dropna(subset=["fiscal_year"])
    # aggregate
    agg = yearly.groupby(["fiscal_year", "compliance_status"], dropna=False)["inspections"].sum().reset_index()
    totals = agg.groupby("fiscal_year")["inspections"].sum().rename("total").reset_index()
    comp = agg[agg["compliance_status"] == "compliant"][["fiscal_year", "inspections"]].rename(columns={"inspections":"compliant"})
    yr = totals.merge(comp, on="fiscal_year", how="left").fillna({"compliant": 0})
    yr["compliance_rate"] = (yr["compliant"] / yr["total"]).where(yr["total"] > 0, 0)
    # sort by FY ascending for the trend
    yr = yr.sort_values(by="fiscal_year", key=lambda s: s.map(lambda v: int(str(v).split("_")[0])))

    # line of compliance rate
    rate_line = (
        alt.Chart(yr)
        .mark_line(point=alt.OverlayMarkDef(size=60, filled=True))
        .encode(
            x=alt.X("fiscal_year:N", title="Fiscal Year", sort=None, axis=alt.Axis(labelAngle=0)),
            y=alt.Y("compliance_rate:Q", title="Compliance Rate", scale=alt.Scale(domain=[0,1]), axis=alt.Axis(format="%")),
            tooltip=[
                alt.Tooltip("fiscal_year:N", title="FY"),
                alt.Tooltip("total:Q", title="Total", format=","),
                alt.Tooltip("compliant:Q", title="Compliant", format=","),
                alt.Tooltip("compliance_rate:Q", title="Rate", format=".1%"),
            ],
            color=alt.value(ACCENT),
        )
        .properties(height=240)
    )
    st.altair_chart(rate_line, use_container_width=True)

    # three insights
    best = yr.loc[yr["compliance_rate"].idxmax()] if len(yr) else None
    worst = yr.loc[yr["compliance_rate"].idxmin()] if len(yr) else None
    busiest = yr.loc[yr["total"].idxmax()] if len(yr) else None

    i1, i2, i3 = st.columns(3)
    with i1:
        if best is not None:
            st.markdown(f'<div class="kpi"><div class="kpi-title">Best FY (Compliance)</div><div class="kpi-value">{pct(float(best["compliance_rate"]))}</div><div class="kpi-note">{best["fiscal_year"]}</div></div>', unsafe_allow_html=True)
    with i2:
        if worst is not None:
            st.markdown(f'<div class="kpi"><div class="kpi-title">Lowest FY (Compliance)</div><div class="kpi-value">{pct(float(worst["compliance_rate"]))}</div><div class="kpi-note">{worst["fiscal_year"]}</div></div>', unsafe_allow_html=True)
    with i3:
        if busiest is not None:
            st.markdown(f'<div class="kpi"><div class="kpi-title">Busiest FY (Total)</div><div class="kpi-value">{int(busiest["total"]):,}</div><div class="kpi-note">{busiest["fiscal_year"]}</div></div>', unsafe_allow_html=True)
else:
    st.info("No monthly data available to compute overview trend.")

st.divider()

# ====== Trends (FY-scoped, statuses) ======
st.markdown('<div class="section-title">Trends (by month)</div>', unsafe_allow_html=True)

if not mbs_fy.empty:
    trend = mbs_fy.copy()
    trend = trend[trend["compliance_status"].isin(sel_status)]
    trend["Month"] = pd.to_datetime(trend["month"], errors="coerce", utc=True)

    line = (
        alt.Chart(trend)
        .mark_line(point=alt.OverlayMarkDef(size=50, filled=True))
        .encode(
            x=alt.X("Month:T", title="Month"),
            y=alt.Y("inspections:Q", title="Inspections"),
            color=alt.Color("compliance_status:N",
                            scale=alt.Scale(domain=["compliant","non_compliant"], range=[ACCENT, DANGER]),
                            title="Status"),
            tooltip=[
                alt.Tooltip("yearmonth(Month):T", title="Month"),
                alt.Tooltip("compliance_status:N", title="Status"),
                alt.Tooltip("inspections:Q", title="Inspections", format=","),
            ],
        )
        .properties(height=280)
    )
    st.altair_chart(line, use_container_width=True)
else:
    st.info("No monthly status data for this fiscal year.")

st.divider()

# ====== Top Sites ======
st.markdown('<div class="section-title">Top Sites (overall)</div>', unsafe_allow_html=True)
if not by_site.empty and {"site","inspections"}.issubset(by_site.columns):
    sites = (by_site.copy()
        .assign(site=by_site["site"].astype(str).replace({"": "Unknown"}))
        .dropna(subset=["site"])
        .sort_values("inspections", ascending=False).head(12))
    bar = (
        alt.Chart(sites)
        .mark_bar()
        .encode(
            x=alt.X("inspections:Q", title="Inspections"),
            y=alt.Y("site:N", sort="-x", title=None),
            tooltip=[alt.Tooltip("site:N", title="Site"), alt.Tooltip("inspections:Q", title="Inspections", format=",")],
            color=alt.value("#2563eb"),
        )
        .properties(height=360)
    )
    st.altair_chart(bar, use_container_width=True)
    with st.expander("View full sites table"):
        st.dataframe(by_site.sort_values("inspections", ascending=False), use_container_width=True, hide_index=True)
else:
    st.info("No site data available.")

st.divider()

# ====== Top PIs ======
st.markdown('<div class="section-title">Top PIs (overall)</div>', unsafe_allow_html=True)
if not by_pi.empty and {"pi","inspections"}.issubset(by_pi.columns):
    pis = (by_pi.copy()
        .assign(pi=by_pi["pi"].astype(str).replace({"": "Unknown"}))
        .dropna(subset=["pi"])
        .sort_values("inspections", ascending=False).head(12))
    barpi = (
        alt.Chart(pis)
        .mark_bar()
        .encode(
            x=alt.X("inspections:Q", title="Inspections"),
            y=alt.Y("pi:N", sort="-x", title=None),
            tooltip=[alt.Tooltip("pi:N", title="PI"), alt.Tooltip("inspections:Q", title="Inspections", format=",")],
            color=alt.value("#0ea5e9"),
        )
        .properties(height=360)
    )
    st.altair_chart(barpi, use_container_width=True)
    with st.expander("View full PI table"):
        st.dataframe(by_pi.sort_values("inspections", ascending=False), use_container_width=True, hide_index=True)
else:
    st.info("No PI data available.")

# ====== FY totals (debug) ======
st.divider()
st.markdown('<div class="section-title">FY totals (troubleshooting)</div>', unsafe_allow_html=True)
if not fy_totals.empty:
    disp = fy_totals.rename(columns={"fiscal_year":"FY", "total_inspections":"Inspections"})
    disp = disp.sort_values(by="FY", key=lambda s: s.map(fy_sort_key), ascending=False)
    st.dataframe(disp, use_container_width=True, hide_index=True)

st.caption(f"Source: s3://{GOLD_ROOT} • run_day: {day}")
