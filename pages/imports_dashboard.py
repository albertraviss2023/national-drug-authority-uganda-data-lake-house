# pages/imports_dashboard.py — NDA Imports Dashboard
# Unified with GCP theme: green‑fade KPI cards, panel/card headers, greentone charts, decluttered layout

import os
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import pyarrow.parquet as pq
import pyarrow.fs as pafs

# =================== CONFIGURATION ===================
st.set_page_config(
    page_title="NDA • Imports Dashboard",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded",
)

# -------------------- NDA Theme --------------------
NDA_GREEN = "#006341"
NDA_LIGHT_GREEN = "#e0f0e5"
NDA_DARK_GREEN = "#004c30"
NDA_ACCENT = "#8dc63f"
TEXT_DARK = "#0f172a"
TEXT_LIGHT = "#64748b"
BG_COLOR = "#f5f9f7"
CARD_BG = "#ffffff"
BORDER_COLOR = "#E5E7EB"

st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');

* {{ font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; }}

.main .block-container {{
  padding-top: .6rem; padding-bottom: 0; background:{BG_COLOR};
}}

/* Header */
.dashboard-header {{ background:{NDA_GREEN}; color:#fff; padding:.9rem 1.1rem; border-radius:12px; margin-bottom: 1rem; display:flex; align-items:center; justify-content:space-between; }}
.dashboard-header h1 {{ font-size:1.1rem; margin:0; font-weight:700; }}
.dashboard-header p {{ margin:0; opacity:.9; font-size:.85rem; }}

/* Sidebar */
[data-testid="stSidebar"] {{ background:{CARD_BG}; border-right:1px solid {BORDER_COLOR}; }}
[data-testid="stSidebar"] .sidebar-content {{ padding: 1rem .75rem; }}
.sidebar-title {{ color:{NDA_GREEN}; font-weight:700; text-align:center; margin:.25rem 0 .5rem; }}

/* Panels */
.panel {{ background:{CARD_BG}; border:1px solid {BORDER_COLOR}; border-radius:12px; box-shadow:0 2px 12px rgba(0,0,0,.04); overflow:hidden; margin-bottom:1rem; }}
.panel-header {{ background:{NDA_GREEN}; color:#fff; padding:.8rem 1rem; display:flex; justify-content:space-between; align-items:center; border-radius:12px 12px 0 0; }}
.panel-header h3 {{ margin:0; font-size:1rem; font-weight:700; }}
.panel-body {{ padding:1rem; }}

/* KPI cards: green fade */
.kpi-grid {{ display:grid; grid-template-columns:repeat(4,1fr); gap:.75rem; }}
.kpi-card {{ background: linear-gradient(180deg, rgba(141,198,63,0.14) 0%, rgba(0,99,65,0.06) 100%); border:1px solid {NDA_LIGHT_GREEN}; border-left:6px solid {NDA_GREEN}; border-radius:12px; padding:1rem; box-shadow:0 2px 10px rgba(0,0,0,.05); }}
.kpi-value {{ font-size:1.6rem; font-weight:700; color:{TEXT_DARK}; margin:0; line-height:1; }}
.kpi-label {{ font-size:.8rem; color:{TEXT_LIGHT}; text-transform:uppercase; letter-spacing:.04em; margin-top:.35rem; }}
.kpi-trend {{ font-size:.75rem; font-weight:600; margin-top:.35rem; }}
.trend-up {{ color:{NDA_ACCENT}; }}
.trend-down {{ color:#ef4444; }}

/* Section header */
.section-header {{ color:{NDA_DARK_GREEN}; font-size:1.05rem; font-weight:700; margin:1rem 0 .5rem; }}

/* Streamlit spacing tweak */
div[data-testid="stHorizontalBlock"] {{ gap:.75rem; }}

</style>
""", unsafe_allow_html=True)

# -------------------- Data Loading --------------------
@st.cache_data(ttl=3600)
def load_metrics_data(lane: str) -> pd.DataFrame:
    try:
        CATALOG_WAREHOUSE = os.environ.get("CATALOG_WAREHOUSE", "s3://warehouse")
        BUCKET = CATALOG_WAREHOUSE.replace("s3://", "").split("/")[0]
        GOLD_ROOT = f"gold/inspectorate/imports/{lane}/metrics_parquet"

        fs = pafs.S3FileSystem(
            access_key=os.environ.get("AWS_ACCESS_KEY_ID"),
            secret_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
            endpoint_override=os.environ.get("CATALOG_S3_ENDPOINT", "").replace("http://","" ).replace("https://",""),
            region=os.environ.get("AWS_REGION", "us-east-1"),
            scheme=os.environ.get("CATALOG_S3_ENDPOINT", "http://minio:9000").split("://")[0],
        )

        prefix = f"{BUCKET}/{GOLD_ROOT}/"
        file_infos = fs.get_file_info(pafs.FileSelector(prefix, recursive=True))
        parquet_files = [fi.path for fi in file_infos if fi.is_file and fi.base_name.endswith('.parquet') and 'metrics.parquet' in fi.path]
        if not parquet_files:
            return pd.DataFrame()

        frames: List[pd.DataFrame] = []
        for path in parquet_files:
            with fs.open_input_file(path) as f:
                frames.append(pq.read_table(f).to_pandas())
        df = pd.concat(frames, ignore_index=True)
        if 'month' in df.columns:
            df['month_dt'] = pd.to_datetime(df['month'].astype(str) + '-01', errors='coerce')
            df = df.dropna(subset=['month_dt']).sort_values('month_dt')
        return df
    except Exception as e:
        st.warning(f"Error loading {lane} data: {e}")
        return pd.DataFrame()

# -------------------- UI helpers --------------------
def kpi_card(value, label, trend=None, trend_value=None):
    trend_html = ""
    if trend and trend_value:
        cls = "trend-up" if trend == "up" else "trend-down"
        sym = "↗" if trend == "up" else "↘"
        trend_html = f'<div class="kpi-trend {cls}">{sym} {trend_value}</div>'
    return f"""
    <div class="kpi-card">
      <div class="kpi-value">{value}</div>
      <div class="kpi-label">{label}</div>
      {trend_html}
    </div>
    """

def chart_panel(title):
    return f"""
    <div class=panel>
      <div class=panel-header><h3>{title}</h3></div>
      <div class=panel-body>
    """

# -------------------- Chart helpers --------------------
def ts_line(df: pd.DataFrame, y_col: str, color=NDA_GREEN):
    fig = px.line(df, x='month_dt', y=y_col, color_discrete_sequence=[color])
    fig.update_layout(plot_bgcolor=CARD_BG, paper_bgcolor=CARD_BG, font=dict(color=TEXT_DARK), margin=dict(t=10,l=10,r=10,b=10), height=320, hovermode='x unified')
    fig.update_xaxes(title="", gridcolor=NDA_LIGHT_GREEN)
    fig.update_yaxes(title="", gridcolor=NDA_LIGHT_GREEN)
    fig.update_traces(line=dict(width=3), marker=dict(size=6))
    return fig

def bar_from_dict(df: pd.DataFrame, dict_col: str, accent=NDA_GREEN, top_n=10, orientation='h'):
    """Aggregate a dict/JSON column into Category/Count and render a bar chart.
    Robust to empty/None/strings and returns an empty figure if nothing to plot.
    """
    def _empty_fig():
        fig = go.Figure()
        fig.update_layout(plot_bgcolor=CARD_BG, paper_bgcolor=CARD_BG, margin=dict(t=10,l=10,r=10,b=10), height=320)
        return fig

    if df is None or df.empty or dict_col not in df.columns:
        return _empty_fig()

    agg: Dict[str, float] = {}
    for obj in df[dict_col]:
        # Accept dicts, JSON strings, or skip
        if isinstance(obj, str):
            try:
                parsed = json.loads(obj)
                obj = parsed if isinstance(parsed, dict) else None
            except Exception:
                obj = None
        if isinstance(obj, dict):
            for k, v in obj.items():
                try:
                    val = pd.to_numeric(v, errors='coerce')
                    if pd.notna(val):
                        agg[k] = agg.get(k, 0.0) + float(val)
                except Exception:
                    pass

    data = pd.DataFrame(list(agg.items()), columns=['Category', 'Count'])
    if data.empty or 'Count' not in data.columns:
        return _empty_fig()

    data = data.sort_values('Count', ascending=False).head(top_n)

    if orientation == 'h':
        fig = px.bar(data, x='Count', y='Category', orientation='h', color_discrete_sequence=[accent])
    else:
        fig = px.bar(data, x='Category', y='Count', color_discrete_sequence=[accent])

    fig.update_layout(
        plot_bgcolor=CARD_BG,
        paper_bgcolor=CARD_BG,
        font=dict(color=TEXT_DARK),
        margin=dict(t=10,l=10,r=10,b=10),
        height=320,
        showlegend=False,
    )
    fig.update_xaxes(gridcolor=NDA_LIGHT_GREEN, title="")
    fig.update_yaxes(gridcolor=NDA_LIGHT_GREEN, title="", categoryorder='total ascending')
    return fig

# =================== SECTION RENDERERS ===================
def render_licensing_dashboard():
    st.markdown("""
    <div class=dashboard-header>
      <div>
        <h1>📋 Licensing Analytics</h1>
        <p>Application volumes, turnaround times, approvals</p>
      </div>
      <div style="opacity:.9;font-size:.85rem">v0.1</div>
    </div>
    """, unsafe_allow_html=True)

    df = load_metrics_data('licensing')
    if df.empty:
        st.info("No licensing data yet.")
        return

    # Filters (date only)
    st.markdown(chart_panel("🔍 Filters"), unsafe_allow_html=True)
    c1, c2 = st.columns([2,1])
    with c1:
        start = df['month_dt'].min(); end = df['month_dt'].max()
        left, right = st.columns(2)
        with left:
            d_from = st.date_input("From", value=start)
        with right:
            d_to = st.date_input("To", value=end)
    st.markdown('</div></div>', unsafe_allow_html=True)

    mask = (df['month_dt'] >= pd.to_datetime(d_from)) & (df['month_dt'] <= pd.to_datetime(d_to))
    d = df.loc[mask].copy()

    # KPIs
    total_apps = int(d.get('total_applications', pd.Series(dtype=int)).sum())
    appr = float(d.get('approval_rate', pd.Series(dtype=float)).mean() or 0) * 100
    tot = float(d.get('avg_tot_days', pd.Series(dtype=float)).mean() or 0)
    qrate = float(d.get('query_rate', pd.Series(dtype=float)).mean() or 0) * 100

    k1,k2,k3,k4 = st.columns(4)
    with k1: st.markdown(kpi_card(f"{total_apps:,}", "Total Applications", "up" if total_apps>0 else "down", f"{total_apps} total"), unsafe_allow_html=True)
    with k2: st.markdown(kpi_card(f"{appr:.1f}%", "Approval Rate", "up" if appr>=50 else "down", f"{appr:.1f}%"), unsafe_allow_html=True)
    with k3: st.markdown(kpi_card(f"{tot:.1f}", "Avg TOT (days)", "down" if tot<30 else "up", f"{tot:.1f} days"), unsafe_allow_html=True)
    with k4: st.markdown(kpi_card(f"{qrate:.1f}%", "Query Rate", "down" if qrate<20 else "up", f"{qrate:.1f}%"), unsafe_allow_html=True)

    # Charts
    cA, cB = st.columns(2)
    with cA:
        st.markdown(chart_panel("📈 Application Volume"), unsafe_allow_html=True)
        st.plotly_chart(ts_line(d, 'total_applications'), use_container_width=True)
        st.markdown('</div></div>', unsafe_allow_html=True)
    with cB:
        st.markdown(chart_panel("⏱️ Turnaround Time"), unsafe_allow_html=True)
        st.plotly_chart(ts_line(d, 'avg_tot_days', NDA_ACCENT), use_container_width=True)
        st.markdown('</div></div>', unsafe_allow_html=True)

    st.markdown(chart_panel("📑 Applications by License Type"), unsafe_allow_html=True)
    st.plotly_chart(bar_from_dict(d, 'license_types'), use_container_width=True)
    st.markdown('</div></div>', unsafe_allow_html=True)

    cC, cD = st.columns(2)
    with cC:
        if 'approval_rate' in d.columns:
            temp = d.copy(); temp['approval_rate_pct'] = temp['approval_rate'] * 100
            st.markdown(chart_panel("✅ Approval Rate Trend"), unsafe_allow_html=True)
            st.plotly_chart(ts_line(temp, 'approval_rate_pct', "#10B981"), use_container_width=True)
            st.markdown('</div></div>', unsafe_allow_html=True)
    with cD:
        if 'query_rate' in d.columns:
            temp = d.copy(); temp['query_rate_pct'] = temp['query_rate'] * 100
            st.markdown(chart_panel("❓ Query Rate Trend"), unsafe_allow_html=True)
            st.plotly_chart(ts_line(temp, 'query_rate_pct', "#EF4444"), use_container_width=True)
            st.markdown('</div></div>', unsafe_allow_html=True)


def render_td_dashboard():
    st.markdown("""
    <div class=dashboard-header>
      <div>
        <h1>📄 Technical Declarations</h1>
        <p>Volumes, declared value, shipment analysis</p>
      </div>
    </div>
    """, unsafe_allow_html=True)

    df = load_metrics_data('td')
    if df.empty:
        st.info("No TD data yet.")
        return

    # KPIs
    total_decls = int(df.get('total_declarations', pd.Series(dtype=int)).sum())
    total_value_m = float(df.get('total_declared_value', pd.Series(dtype=float)).sum() or 0) / 1_000_000
    avg_proc = float(df.get('avg_processing_days', pd.Series(dtype=float)).mean() or 0)
    ier = float(df.get('import_export_ratio', pd.Series(dtype=float)).mean() or 0)

    k1,k2,k3,k4 = st.columns(4)
    with k1: st.markdown(kpi_card(f"{total_decls:,}", "Total Declarations", "up" if total_decls>0 else "down", f"{total_decls:,}"), unsafe_allow_html=True)
    with k2: st.markdown(kpi_card(f"${total_value_m:.1f}M", "Declared Value", "up" if total_value_m>0 else "down", f"${total_value_m:.1f}M"), unsafe_allow_html=True)
    with k3: st.markdown(kpi_card(f"{avg_proc:.1f}", "Avg Processing Days", "down" if avg_proc<10 else "up", f"{avg_proc:.1f} days"), unsafe_allow_html=True)
    with k4: st.markdown(kpi_card(f"{ier:.1f}:1", "Import/Export Ratio", "up" if ier>1 else "down", f"{ier:.1f}:1"), unsafe_allow_html=True)

    cA, cB = st.columns(2)
    with cA:
        st.markdown(chart_panel("📈 Declaration Volume"), unsafe_allow_html=True)
        st.plotly_chart(ts_line(df, 'total_declarations'), use_container_width=True)
        st.markdown('</div></div>', unsafe_allow_html=True)
    with cB:
        st.markdown(chart_panel("💰 Declared Value"), unsafe_allow_html=True)
        temp = df.copy(); temp['total_value_millions'] = temp.get('total_declared_value', 0) / 1_000_000
        st.plotly_chart(ts_line(temp, 'total_value_millions', NDA_ACCENT), use_container_width=True)
        st.markdown('</div></div>', unsafe_allow_html=True)

    cC, cD = st.columns(2)
    with cC:
        st.markdown(chart_panel("🚢 Shipment Modes"), unsafe_allow_html=True)
        st.plotly_chart(bar_from_dict(df, 'shipment_modes'), use_container_width=True)
        st.markdown('</div></div>', unsafe_allow_html=True)
    with cD:
        st.markdown(chart_panel("📦 Shipment Categories"), unsafe_allow_html=True)
        st.plotly_chart(bar_from_dict(df, 'shipment_categories', NDA_ACCENT), use_container_width=True)
        st.markdown('</div></div>', unsafe_allow_html=True)

    st.markdown(chart_panel("🏆 Top Products"), unsafe_allow_html=True)
    st.plotly_chart(bar_from_dict(df, 'top_products'), use_container_width=True)
    st.markdown('</div></div>', unsafe_allow_html=True)


def render_vc_dashboard():
    st.markdown("""
    <div class=dashboard-header>
      <div>
        <h1>✅ Verification Certificates</h1>
        <p>Certificate volumes, processing times, business metrics</p>
      </div>
    </div>
    """, unsafe_allow_html=True)

    df = load_metrics_data('vc')
    if df.empty:
        st.info("No VC data yet.")
        return

    total_certs = int(df.get('total_certificates', pd.Series(dtype=int)).sum())
    total_verif_m = float(df.get('total_verification_value', pd.Series(dtype=float)).sum() or 0) / 1_000_000
    appr = float(df.get('approval_rate', pd.Series(dtype=float)).mean() or 0) * 100
    p2d = float(df.get('avg_payment_to_decision_days', pd.Series(dtype=float)).mean() or 0)

    k1,k2,k3,k4 = st.columns(4)
    with k1: st.markdown(kpi_card(f"{total_certs:,}", "Total Certificates", "up" if total_certs>0 else "down", f"{total_certs:,}"), unsafe_allow_html=True)
    with k2: st.markdown(kpi_card(f"${total_verif_m:.1f}M", "Verification Value", "up" if total_verif_m>0 else "down", f"${total_verif_m:.1f}M"), unsafe_allow_html=True)
    with k3: st.markdown(kpi_card(f"{appr:.1f}%", "Approval Rate", "up" if appr>50 else "down", f"{appr:.1f}%"), unsafe_allow_html=True)
    with k4: st.markdown(kpi_card(f"{p2d:.1f}", "Payment→Decision (days)", "down" if p2d<10 else "up", f"{p2d:.1f} days"), unsafe_allow_html=True)

    cA, cB = st.columns(2)
    with cA:
        st.markdown(chart_panel("📈 Certificate Volume"), unsafe_allow_html=True)
        st.plotly_chart(ts_line(df, 'total_certificates'), use_container_width=True)
        st.markdown('</div></div>', unsafe_allow_html=True)
    with cB:
        st.markdown(chart_panel("⏱️ Processing Times"), unsafe_allow_html=True)
        fig = go.Figure()
        if 'avg_receipt_to_billing_days' in df.columns:
            fig.add_trace(go.Scatter(x=df['month_dt'], y=df['avg_receipt_to_billing_days'], name='Receipt→Billing', line=dict(color=NDA_GREEN)))
        if 'avg_payment_to_decision_days' in df.columns:
            fig.add_trace(go.Scatter(x=df['month_dt'], y=df['avg_payment_to_decision_days'], name='Payment→Decision', line=dict(color=NDA_ACCENT)))
        fig.update_layout(plot_bgcolor=CARD_BG, paper_bgcolor=CARD_BG, font=dict(color=TEXT_DARK), margin=dict(t=10,l=10,r=10,b=10), height=320, hovermode='x unified', legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1))
        fig.update_xaxes(gridcolor=NDA_LIGHT_GREEN, title="")
        fig.update_yaxes(gridcolor=NDA_LIGHT_GREEN, title="Days")
        st.plotly_chart(fig, use_container_width=True)
        st.markdown('</div></div>', unsafe_allow_html=True)

    cC, cD = st.columns(2)
    with cC:
        st.markdown(chart_panel("📑 Certificate Types"), unsafe_allow_html=True)
        st.plotly_chart(bar_from_dict(df, 'vc_types'), use_container_width=True)
        st.markdown('</div></div>', unsafe_allow_html=True)
    with cD:
        st.markdown(chart_panel("🎯 Import Reasons"), unsafe_allow_html=True)
        st.plotly_chart(bar_from_dict(df, 'import_reasons', NDA_ACCENT), use_container_width=True)
        st.markdown('</div></div>', unsafe_allow_html=True)

    st.markdown(chart_panel("💰 Avg Value per Certificate (K)"), unsafe_allow_html=True)
    if 'avg_value_per_certificate' in df.columns:
        tmp = df.copy(); tmp['avg_value_thousands'] = tmp['avg_value_per_certificate']/1000
        st.plotly_chart(ts_line(tmp, 'avg_value_thousands', "#10B981"), use_container_width=True)
    else:
        st.info("No value-per-certificate metric available")
    st.markdown('</div></div>', unsafe_allow_html=True)

# =================== MAIN ===================
def main():
    # Sidebar (minimal)
    with st.sidebar:
        st.markdown(f"<div class='sidebar-content'><div class='sidebar-title'>📦 Imports Dashboard</div><div style='text-align:center;color:{TEXT_LIGHT};font-size:.85rem'>NDA Analytics Platform</div></div>", unsafe_allow_html=True)
        if st.button("🔄 Refresh Data", use_container_width=True):
            st.cache_data.clear(); st.rerun()

    tabs = st.tabs(["📋 Licensing", "📄 Technical Declarations", "✅ Verification Certificates"])
    with tabs[0]:
        render_licensing_dashboard()
    with tabs[1]:
        render_td_dashboard()
    with tabs[2]:
        render_vc_dashboard()


if __name__ == "__main__":
    main()
