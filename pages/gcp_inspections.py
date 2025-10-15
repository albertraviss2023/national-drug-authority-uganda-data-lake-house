# pages/gcp_inspections.py — GCP Inspections Dashboard (NDA Official Theme) - CLEAN, GREEN-FADE, NO FY FILTER

import os
from pathlib import Path
from typing import List, Optional
from datetime import datetime, timedelta

import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

# -------------------- Page Config --------------------
st.set_page_config(
    page_title="NDA - GCP Inspections Dashboard",
    page_icon="🧪",
    layout="wide",
    initial_sidebar_state="expanded",
)

# -------------------- NDA Official Theme Colors --------------------
NDA_GREEN = "#006341"
NDA_LIGHT_GREEN = "#e0f0e5"
NDA_DARK_GREEN = "#004c30"
NDA_ACCENT = "#8dc63f"
TEXT_DARK = "#0f172a"
TEXT_LIGHT = "#64748b"
BG_COLOR = "#f5f9f7"
CARD_BG = "#FFFFFF"
BORDER_COLOR = "#E5E7EB"

# -------------------- NDA Official CSS Theme --------------------
st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

* {{ font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; }}

.main .block-container {{
    padding-top: .5rem;
    padding-bottom: 0rem;
    background-color: {BG_COLOR};
}}

/* Header */
.dashboard-header {{
    background: {NDA_GREEN};
    color: white;
    padding: .85rem 1.25rem;
    display: flex; align-items: center; justify-content: space-between;
    border-radius: 10px; margin-bottom: 1rem;
}}
.dashboard-header h1 {{ font-size: 1.1rem; margin: 0; font-weight: 600; }}
.dashboard-header p {{ margin: 0; opacity: .9; font-size: .85rem; }}

/* Sidebar */
[data-testid="stSidebar"] {{ background: {CARD_BG}; border-right: 1px solid {BORDER_COLOR}; }}
[data-testid="stSidebar"] .sidebar-content {{ padding: 1rem .75rem; }}
.sidebar-section h3 {{ color: {NDA_GREEN}; font-size: .95rem; margin: 1rem 0 .5rem; font-weight: 600; }}
.sidebar-section a {{ display:block; padding:.45rem .5rem; color:#334155; text-decoration:none; border-radius:8px; }}
.sidebar-section a:hover, .sidebar-section a.active {{ background:{NDA_LIGHT_GREEN}; color:{NDA_DARK_GREEN}; }}

/* Panel Header */
.panel-header {{
    background: {NDA_GREEN};
    color: white; padding: .8rem 1rem; display:flex; align-items:center; justify-content:space-between;
    border-radius: 10px 10px 0 0;
}}
.panel-header h3 {{ margin:0; font-weight:600; font-size:1rem; }}

/* Generic Panel/Card */
.panel {{ background:{CARD_BG}; border:1px solid {BORDER_COLOR}; border-radius:10px; box-shadow:0 2px 12px rgba(0,0,0,.04); overflow:hidden; margin-bottom:1rem; }}
.panel-body {{ padding: 1rem; }}

/* KPI Cards — green fade */
.kpi-grid {{ display:grid; grid-template-columns:repeat(4,1fr); gap: .75rem; }}
.kpi-card {{
    background: linear-gradient(180deg, rgba(141,198,63,0.14) 0%, rgba(0,99,65,0.06) 100%);
    border: 1px solid {NDA_LIGHT_GREEN};
    border-left: 6px solid {NDA_GREEN};
    border-radius: 12px; padding: 1rem; height:100%;
    box-shadow: 0 2px 10px rgba(0,0,0,.05);
}}
.kpi-value {{ font-size: 1.6rem; font-weight: 700; color: {TEXT_DARK}; line-height:1; margin: 0; }}
.kpi-label {{ font-size:.8rem; color:{TEXT_LIGHT}; text-transform:uppercase; letter-spacing:.04em; margin-top:.35rem; }}
.kpi-trend {{ font-size:.75rem; font-weight:600; margin-top:.4rem; }}
.trend-up {{ color:{NDA_ACCENT}; }}
.trend-down {{ color:#ef4444; }}

/* Filters*/
.filters-grid {{ display:grid; grid-template-columns:2fr 1fr 1fr; gap:.75rem; }}
.filter-box {{ background:{CARD_BG}; border:1px solid {BORDER_COLOR}; border-radius:10px; padding:.9rem; }}
.filter-title {{ font-size:.8rem; font-weight:600; color:{NDA_GREEN}; margin-bottom:.4rem; letter-spacing:.04em; text-transform:uppercase; }}

/* Section header */
.section-header {{ color:{NDA_DARK_GREEN}; font-size:1.05rem; font-weight:700; margin:1rem 0 .5rem; }}

/* Streamlit tweaks */
div[data-testid="stHorizontalBlock"] {{ gap:.75rem; }}
.stButton button {{ border:1px solid {NDA_GREEN}; color:{NDA_GREEN}; background:white; border-radius:8px; padding:.4rem .8rem; font-weight:600; }}
.stButton button:hover {{ background:{NDA_LIGHT_GREEN}; }}
</style>
""", unsafe_allow_html=True)

# -------------------- ENV & GOLD ROOT --------------------
CATALOG_WAREHOUSE = os.environ.get("CATALOG_WAREHOUSE", "s3://warehouse")
CATALOG_S3_ENDPOINT = os.environ.get("CATALOG_S3_ENDPOINT", "http://minio:9000")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

assert CATALOG_WAREHOUSE.startswith("s3://")
BUCKET = CATALOG_WAREHOUSE.replace("s3://", "").split("/")[0]
GOLD_ROOT = f"{BUCKET}/gold/dps/gcp"

# -------------------- Helpers --------------------
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
    for sub in ["monthly_by_status", "monthly_totals", "overview", "by_pi", "by_site"]:
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


def add_fy_from_month(df: pd.DataFrame, month_col: str) -> pd.DataFrame:
    """Optional helper: derives fiscal_year for display; not used for filtering."""
    if month_col not in df.columns:
        return df
    out = df.copy()
    m = pd.to_datetime(out[month_col], errors="coerce", utc=True)
    y = m.dt.year
    start = (m.dt.month >= 7)
    fy_start = y.where(~start, y)
    fy_end = (y + 1).where(start, y)
    fy = fy_start.where(start, y - 1).astype("Int64").astype("string") + "_" + fy_end.astype("Int64").astype("string")
    out["fiscal_year"] = fy.where(~m.isna(), None)
    return out

# -------------------- UI helpers --------------------
def kpi_card(value, label, trend=None, trend_value=None, icon=""):
    trend_html = ""
    if trend and trend_value:
        cls = "trend-up" if trend == "up" else "trend-down"
        sym = "↗" if trend == "up" else "↘"
        trend_html = f'<div class="kpi-trend {cls}">{sym} {trend_value}</div>'
    return f"""
    <div class="kpi-card">
      <div style="display:flex;align-items:center;gap:.5rem;opacity:.8">{icon}</div>
      <div class="kpi-value">{value}</div>
      <div class="kpi-label">{label}</div>
      {trend_html}
    </div>
    """


def chart_panel(title):
    return f"""
    <div class="panel">
      <div class="panel-header"><h3>{title}</h3></div>
      <div class="panel-body">
    """

# -------------------- Main --------------------
def main():
    # Header (decluttered)
    st.markdown(
        """
        <div class="dashboard-header">
          <div>
            <h1>National Drug Authority — GCP Inspections</h1>
            <p>Executive analytics (gold)</p>
          </div>
          <div style="font-size:.85rem;opacity:.9;">v2.1</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Sidebar (trimmed)
    with st.sidebar:
        st.markdown(
            """
            <div class="sidebar-content">
              <div style="text-align:center;color:#0b7d3e;font-weight:700;margin:.5rem 0">🧪 GCP Analytics</div>
              <div class="sidebar-section">
                <h3>Navigation</h3>
                <a class="active" href="#">Dashboard</a>
                <a href="#">Reports</a>
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    # Load data
    with st.spinner("Loading gold datasets…"):
        fs = get_fs()
        day = latest_run_day(fs)
        if not day:
            st.error("No gold KPI partitions found under gold/dps/gcp.")
            st.stop()
        monthly_by_status = read_gold(fs, "monthly_by_status", day)
        monthly_totals = read_gold(fs, "monthly_totals", day)
        by_pi = read_gold(fs, "by_pi", day)
        by_site = read_gold(fs, "by_site", day)

    # Filters — FY removed
    st.markdown('<div class="panel"><div class="panel-header"><h3>🔍 Filters</h3></div><div class="panel-body">', unsafe_allow_html=True)
    c1, c2, c3 = st.columns([2, 1, 1])
    with c1:
        st.markdown('<div class="filter-title">Date Range</div>', unsafe_allow_html=True)
        left, right = st.columns(2)
        with left:
            default_start = datetime.now() - timedelta(days=365)
            date_from = st.date_input("From", value=default_start, key="date_from")
        with right:
            date_to = st.date_input("To", value=datetime.now(), key="date_to")
    with c2:
        st.markdown('<div class="filter-title">Status</div>', unsafe_allow_html=True)
        status_options = ["compliant", "non_compliant"]
        if "compliance_status" in monthly_by_status.columns:
            present = monthly_by_status["compliance_status"].dropna().astype(str).str.lower().unique().tolist()
            status_options = [s for s in status_options if s in present] or status_options
        status_selection = st.multiselect("Status", options=status_options, default=status_options, label_visibility="collapsed")
    with c3:
        st.markdown('<div class="filter-title">View</div>', unsafe_allow_html=True)
        view = st.selectbox("View", options=["Overview", "Detailed", "Trends"], label_visibility="collapsed")
    st.markdown('</div></div>', unsafe_allow_html=True)

    # Apply filters
    mbs = monthly_by_status.copy()
    if "month" in mbs.columns:
        mbs["Month"] = pd.to_datetime(mbs["month"], errors="coerce", utc=True)
        mbs = add_fy_from_month(mbs, "Month")  # optional for display only
        if date_from and date_to:
            try:
                start_ts = pd.to_datetime(date_from).tz_localize('UTC')
                end_ts = pd.to_datetime(date_to).tz_localize('UTC')
                mbs = mbs[(mbs["Month"] >= start_ts) & (mbs["Month"] <= end_ts)]
            except Exception as e:
                st.warning(f"Date filtering issue: {e}. Using all dates.")
    if status_selection and "compliance_status" in mbs.columns:
        mbs = mbs[mbs["compliance_status"].isin(status_selection)]

    st.markdown('<div class="section-header">GCP Inspections Analytics</div>', unsafe_allow_html=True)

    # KPIs
    total_inspections = int(mbs["inspections"].sum()) if (not mbs.empty and "inspections" in mbs.columns) else 0
    compliant = int(mbs.loc[mbs.get("compliance_status")=="compliant", "inspections"].sum()) if not mbs.empty else 0
    non_compliant = int(mbs.loc[mbs.get("compliance_status")=="non_compliant", "inspections"].sum()) if not mbs.empty else 0
    compliance_rate = (compliant / total_inspections) if total_inspections > 0 else 0

    k1, k2, k3, k4 = st.columns(4)
    with k1:
        st.markdown(kpi_card(f"{total_inspections:,}", "Total Inspections", "up" if total_inspections>0 else "down", f"{total_inspections} total", ""), unsafe_allow_html=True)
    with k2:
        st.markdown(kpi_card(f"{compliance_rate:.1%}", "Compliance Rate", "up" if compliance_rate>=0.8 else "down", f"{compliance_rate:.1%} compliant"), unsafe_allow_html=True)
    with k3:
        st.markdown(kpi_card(f"{non_compliant:,}", "Non‑Compliant", "down" if non_compliant==0 else "up", f"{non_compliant} findings"), unsafe_allow_html=True)
    with k4:
        st.markdown(kpi_card(f"{compliant:,}", "Compliant", "up" if compliant>=non_compliant else "down", f"{compliant} sites"), unsafe_allow_html=True)

    # Charts — decluttered titles and consistent panels
    cA, cB, cC = st.columns([2,1,1])

    with cA:
        st.markdown(chart_panel("📈 Compliance Trend"), unsafe_allow_html=True)
        if not mbs.empty and {"Month","compliance_status","inspections"}.issubset(mbs.columns):
            trend = mbs.groupby(["Month","compliance_status"])['inspections'].sum().reset_index()
            trend["Month"] = trend["Month"].dt.strftime('%Y-%m')
            fig = px.line(trend, x="Month", y="inspections", color="compliance_status", color_discrete_sequence=[NDA_GREEN, NDA_ACCENT])
            fig.update_layout(plot_bgcolor=CARD_BG, paper_bgcolor=CARD_BG, margin=dict(t=10,l=10,r=10,b=10), height=320, legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1))
            fig.update_traces(line=dict(width=3), marker=dict(size=6))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No trend data for current filters")
        st.markdown('</div></div>', unsafe_allow_html=True)

    with cB:
        st.markdown(chart_panel("📋 Compliance Split"), unsafe_allow_html=True)
        if not mbs.empty and {"compliance_status","inspections"}.issubset(mbs.columns):
            comp = mbs.groupby("compliance_status")["inspections"].sum().reset_index()
            fig = px.pie(comp, values="inspections", names="compliance_status", hole=.4, color_discrete_sequence=[NDA_GREEN, NDA_ACCENT])
            fig.update_layout(plot_bgcolor=CARD_BG, paper_bgcolor=CARD_BG, margin=dict(t=10,l=10,r=10,b=10), height=320)
            fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No compliance data")
        st.markdown('</div></div>', unsafe_allow_html=True)

    with cC:
        st.markdown(chart_panel("👥 Top PIs"), unsafe_allow_html=True)
        if not by_pi.empty and {"pi","inspections"}.issubset(by_pi.columns):
            pi = by_pi.groupby("pi")["inspections"].sum().reset_index().nlargest(8, "inspections")
            fig = px.bar(pi, y="pi", x="inspections", orientation='h', color_discrete_sequence=[NDA_GREEN])
            fig.update_layout(plot_bgcolor=CARD_BG, paper_bgcolor=CARD_BG, margin=dict(t=10,l=10,r=10,b=10), height=320, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No PI data")
        st.markdown('</div></div>', unsafe_allow_html=True)

    d1, d2 = st.columns(2)
    with d1:
        st.markdown(chart_panel("🏢 Inspections by Site"), unsafe_allow_html=True)
        if not by_site.empty and {"site","inspections"}.issubset(by_site.columns):
            site = by_site.groupby("site")["inspections"].sum().reset_index().nlargest(10, "inspections")
            fig = px.bar(site, x="site", y="inspections", color_discrete_sequence=[NDA_GREEN])
            fig.update_layout(plot_bgcolor=CARD_BG, paper_bgcolor=CARD_BG, margin=dict(t=10,l=10,r=10,b=10), height=320, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No site data")
        st.markdown('</div></div>', unsafe_allow_html=True)

    with d2:
        st.markdown(chart_panel("📅 Monthly Totals"), unsafe_allow_html=True)
        if not mbs.empty and {"Month","inspections"}.issubset(mbs.columns):
            monthly = mbs.groupby("Month")["inspections"].sum().reset_index()
            monthly["Month"] = monthly["Month"].dt.strftime('%Y-%m')
            fig = px.bar(monthly, x="Month", y="inspections", color_discrete_sequence=[NDA_GREEN])
            fig.update_layout(plot_bgcolor=CARD_BG, paper_bgcolor=CARD_BG, margin=dict(t=10,l=10,r=10,b=10), height=320, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No monthly data")
        st.markdown('</div></div>', unsafe_allow_html=True)

    # Details table (decluttered)
    st.markdown('<div class="section-header">📋 Inspection Details</div>', unsafe_allow_html=True)
    st.markdown('<div class="panel"><div class="panel-header"><h3>Inspection Details</h3></div><div class="panel-body">', unsafe_allow_html=True)
    if not mbs.empty:
        df = mbs.copy()
        if "Month" in df.columns:
            df["Date"] = df["Month"].dt.strftime('%Y-%m-%d')
        cols = []
        for name in ["Date", "compliance_status", "inspections", "fiscal_year"]:
            if name in df.columns or (name == "Date"):
                cols.append(name)
        show = df[[c for c in cols if c in df.columns]].rename(columns={"compliance_status":"Status", "inspections":"Inspections", "fiscal_year":"Fiscal Year"}).sort_values("Date" if "Date" in df.columns else "Inspections", ascending=False)
        st.dataframe(show, use_container_width=True, height=380, hide_index=True)
    else:
        st.info("No inspection data for current filters")
    st.markdown('</div></div>', unsafe_allow_html=True)

    # Footer
    st.markdown("---")
    st.markdown(f"<div style='text-align:center;color:#475569;font-size:.9rem'>National Drug Authority • GCP Inspections • Data updated: {day if day else 'Unknown'}</div>", unsafe_allow_html=True)


if __name__ == "__main__":
    main()
