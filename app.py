# app.py — NDA Analytics Hub (Modern Overhaul)
import os
from pathlib import Path
from typing import List, Optional

import streamlit as st
import altair as alt

# -------------------- Page / Theme --------------------
st.set_page_config(
    page_title="NDA Analytics Hub", 
    page_icon="🏛️", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# Brand colors (NDA) - refined for modern look
PRIMARY = "#0B7D3E"        # NDA green
PRIMARY_DARK = "#0A5D2E"
PRIMARY_SOFT = "#DCF2E6"
ACCENT_AMBER = "#F59E0B"
ACCENT_GRAY = "#94A3B8"
TEXT_DARK = "#0F172A"      # Darker for better contrast
MUTED = "#64748B"
BORDER = "#E2E8F0"
BG = "#F8FAFC"             # Lighter BG for sleek feel
SHADOW_LIGHT = "0 1px 3px rgba(0, 0, 0, 0.1)"
SHADOW_MED = "0 4px 12px rgba(0, 0, 0, 0.08)"
SHADOW_HEAVY = "0 8px 24px rgba(0, 0, 0, 0.12)"

# -------------------- Global CSS (Modern Overhaul) --------------------
st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

* {{
    font-family: 'Inter', sans-serif;
}}

html, body, [class*="main"] {{ 
    background-color: {BG}; 
}}

.block-container {{ 
    padding-top: 1rem; 
    padding-bottom: 0.5rem; 
    max-width: 100%;
}}

/* Header Styling */
.main-header {{
    background: linear-gradient(135deg, {PRIMARY} 0%, {PRIMARY_DARK} 100%);
    color: white;
    border-radius: 24px;
    padding: 3rem 2.5rem;
    margin-bottom: 2rem;
    text-align: center;
    box-shadow: {SHADOW_HEAVY};
}}

.main-title {{
    font-size: 3.5rem;
    font-weight: 800;
    margin: 0 0 1rem 0;
    letter-spacing: -0.025em;
    background: linear-gradient(135deg, #FFFFFF 0%, #F0FDF4 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
}}

.main-subtitle {{
    font-size: 1.3rem;
    opacity: 0.9;
    margin: 0;
    font-weight: 400;
    max-width: 600px;
    margin: 0 auto;
}}

/* Unit Cards - Modern Grid */
.unit-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(350px, 1fr));
    gap: 1.5rem;
    margin: 2rem 0;
}}

.unit-card-modern {{
    background: white;
    border: 1px solid {BORDER};
    border-radius: 20px;
    padding: 2rem;
    box-shadow: {SHADOW_LIGHT};
    transition: all 0.3s ease;
    position: relative;
    overflow: hidden;
}}

.unit-card-modern::before {{
    content: '';
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    height: 4px;
    background: linear-gradient(90deg, {PRIMARY} 0%, {ACCENT_AMBER} 100%);
}}

.unit-card-modern:hover {{
    transform: translateY(-4px);
    box-shadow: {SHADOW_HEAVY};
}}

.unit-icon-modern {{
    font-size: 3rem;
    margin-bottom: 1rem;
    background: linear-gradient(135deg, {PRIMARY} 0%, {ACCENT_AMBER} 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
}}

.unit-title-modern {{
    font-size: 1.5rem;
    font-weight: 700;
    margin: 0 0 0.75rem 0;
    color: {TEXT_DARK};
}}

.unit-desc-modern {{
    color: {MUTED};
    font-size: 1rem;
    line-height: 1.6;
    margin-bottom: 1.5rem;
}}

.unit-badge {{
    background: {PRIMARY_SOFT};
    color: {PRIMARY_DARK};
    padding: 0.25rem 0.75rem;
    border-radius: 20px;
    font-size: 0.8rem;
    font-weight: 600;
    display: inline-block;
    margin-bottom: 1rem;
}}

.unit-btn-modern {{
    background: {PRIMARY};
    color: white;
    border: none;
    border-radius: 12px;
    padding: 0.75rem 1.5rem;
    font-weight: 600;
    cursor: pointer;
    transition: all 0.2s;
    width: 100%;
    text-align: center;
    text-decoration: none;
    display: block;
}}

.unit-btn-modern:hover {{
    background: {PRIMARY_DARK};
    transform: translateY(-1px);
}}

/* Sidebar Styling */
[data-testid="stSidebar"] {{
    background: white;
    border-right: 1px solid {BORDER};
}}

.sidebar-header {{
    text-align: center;
    padding: 1.5rem 0;
    border-bottom: 1px solid {BORDER};
    margin-bottom: 1rem;
}}

.sidebar-title {{
    font-size: 1.5rem;
    font-weight: 700;
    color: {PRIMARY};
    margin: 0;
}}

/* Quick Stats */
.stats-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 1rem;
    margin: 2rem 0;
}}

.stat-card {{
    background: white;
    border-radius: 16px;
    padding: 1.5rem;
    border: 1px solid {BORDER};
    box-shadow: {SHADOW_LIGHT};
    text-align: center;
}}

.stat-value {{
    font-size: 2rem;
    font-weight: 700;
    color: {PRIMARY};
    margin: 0;
}}

.stat-label {{
    font-size: 0.9rem;
    color: {MUTED};
    font-weight: 500;
    margin: 0.5rem 0 0 0;
}}

/* Footer */
.footer {{
    text-align: center;
    padding: 3rem;
    background: linear-gradient(135deg, {PRIMARY_SOFT} 0%, #FFFFFF 100%);
    border-radius: 20px;
    margin-top: 3rem;
}}
</style>
""", unsafe_allow_html=True)

# -------------------- Header / Logo --------------------
def find_logo():
    here = Path(__file__).parent
    candidates = ["logo.png", "nda_logo.png", "logo.jpg", "nda_logo.jpg", "logo.jpeg", "nda_logo.jpeg"]
    for name in candidates:
        p = here / name
        if p.exists():
            return name
    assets = here / "assets"
    if assets.exists():
        for name in candidates:
            p = assets / name
            if p.exists():
                return f"assets/{name}"
    return None

logo = find_logo()

# -------------------- Sidebar Navigation (Modern) --------------------
st.sidebar.markdown("""
<div class="sidebar-header">
    <div class="sidebar-title">🏛️ NDA Analytics Hub</div>
</div>
""", unsafe_allow_html=True)

st.sidebar.markdown("### 🧭 Navigation")

# Home always active
if st.sidebar.button("🏠 **Dashboard Home**", use_container_width=True, type="primary"):
    st.switch_page("app.py")

st.sidebar.markdown("---")
st.sidebar.markdown("### 🧪 GCP Unit")
col1, col2 = st.sidebar.columns(2)
with col1:
    if st.button("📊 Dashboard", use_container_width=True):
        st.switch_page("pages/gcp_inspections.py")
with col2:
    if st.button("📝 Data Entry", use_container_width=True):
        st.switch_page("pages/gcp_data_entry.py")

st.sidebar.markdown("### 📦 Imports Unit")
col1, col2 = st.sidebar.columns(2)
with col1:
    if st.button("📈 Analytics", use_container_width=True):
        st.switch_page("pages/imports_dashboard.py")
with col2:
    if st.button("⚙️ Management", use_container_width=True, disabled=True):
        st.switch_page("pages/imports_data_entry.py")

st.sidebar.markdown("---")
st.sidebar.markdown("### 🔮 Coming Soon")
st.sidebar.markdown("""
- 🏭 Manufacturing Unit
- 🚚 Logistics Unit  
- 📊 Executive Dashboard
""")

if logo:
    st.sidebar.markdown("---")
    st.sidebar.markdown(f'<div style="text-align: center;"><img src="{logo}" width="150" style="border-radius: 12px;"></div>', unsafe_allow_html=True)

st.sidebar.markdown("""
<div style="text-align: center; margin-top: 2rem; color: #64748B; font-size: 0.8rem;">
    <p>Powered by NDA Analytics</p>
    <p>v2.0 • October 2025</p>
</div>
""", unsafe_allow_html=True)

# -------------------- Main Hub Content (Modern) --------------------
st.markdown("""
<div class="main-header">
    <h1 class="main-title">NDA Analytics Hub</h1>
    <p class="main-subtitle">Unified platform for regulatory intelligence and operational excellence across all NDA units</p>
</div>
""", unsafe_allow_html=True)

# Quick Stats
st.markdown("### 📈 Platform Overview")
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.markdown("""
    <div class="stat-card">
        <div class="stat-value">2</div>
        <div class="stat-label">Active Units</div>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown("""
    <div class="stat-card">
        <div class="stat-value">5+</div>
        <div class="stat-label">Dashboards</div>
    </div>
    """, unsafe_allow_html=True)

with col3:
    st.markdown("""
    <div class="stat-card">
        <div class="stat-value">100%</div>
        <div class="stat-label">Uptime</div>
    </div>
    """, unsafe_allow_html=True)

with col4:
    st.markdown("""
    <div class="stat-card">
        <div class="stat-value">24/7</div>
        <div class="stat-label">Monitoring</div>
    </div>
    """, unsafe_allow_html=True)

# Unit Grid
st.markdown("### 🚀 Available Units")

units = [
    {
        "icon": "🧪",
        "title": "GCP Inspections",
        "description": "Comprehensive Good Clinical Practice monitoring with real-time compliance tracking, trend analysis, and CAPA management.",
        "badge": "ACTIVE",
        "page": "pages/gcp_inspections.py",
        "status": "active"
    },
    {
        "icon": "📦", 
        "title": "Imports Analytics",
        "description": "End-to-end import declaration tracking with licensing, technical declarations, and verification certificate analytics.",
        "badge": "NEW",
        "page": "pages/imports_dashboard.py", 
        "status": "active"
    },
    {
        "icon": "🏭",
        "title": "Manufacturing QA",
        "description": "Quality assurance and batch release tracking for pharmaceutical manufacturing operations (Q4 2025).",
        "badge": "COMING SOON",
        "page": "#",
        "status": "soon"
    },
    {
        "icon": "🚚",
        "title": "Logistics Tracking", 
        "description": "Supply chain visibility and cold chain monitoring for pharmaceutical distribution (Q1 2026).",
        "badge": "PLANNED",
        "page": "#",
        "status": "planned"
    }
]

st.markdown('<div class="unit-grid">', unsafe_allow_html=True)

for unit in units:
    badge_color = PRIMARY if unit["status"] == "active" else ACCENT_AMBER if unit["status"] == "soon" else ACCENT_GRAY
    button_text = "Access Dashboard" if unit["status"] == "active" else "Coming Soon"
    
    st.markdown(f"""
    <div class="unit-card-modern">
        <div class="unit-badge" style="background: {PRIMARY_SOFT if unit['status'] == 'active' else '#FEF3C7' if unit['status'] == 'soon' else '#F1F5F9'}; color: {PRIMARY_DARK if unit['status'] == 'active' else '#92400E' if unit['status'] == 'soon' else '#475569'};">
            {unit['badge']}
        </div>
        <div class="unit-icon-modern">{unit['icon']}</div>
        <h3 class="unit-title-modern">{unit['title']}</h3>
        <p class="unit-desc-modern">{unit['description']}</p>
        <a href="{unit['page'] if unit['status'] == 'active' else '#'}" class="unit-btn-modern" style="background: {PRIMARY if unit['status'] == 'active' else ACCENT_GRAY}; cursor: {'pointer' if unit['status'] == 'active' else 'not-allowed'};">
            {button_text}
        </a>
    </div>
    """, unsafe_allow_html=True)

st.markdown('</div>', unsafe_allow_html=True)

# Footer CTA
st.markdown("""
<div class="footer">
    <h2 style="color: {PRIMARY_DARK}; margin-bottom: 1rem;">Ready to Transform Your Operations?</h2>
    <p style="color: {MUTED}; font-size: 1.1rem; max-width: 600px; margin: 0 auto 2rem auto;">
        Our analytics platform provides real-time insights, predictive analytics, and comprehensive reporting 
        to drive regulatory compliance and operational excellence across all NDA units.
    </p>
    <div style="display: flex; gap: 1rem; justify-content: center;">
        <a href="pages/gcp_inspections.py" class="unit-btn-modern" style="display: inline-block; width: auto; padding: 1rem 2rem;">
            🧪 Launch GCP Dashboard
        </a>
        <a href="pages/imports_dashboard.py" class="unit-btn-modern" style="display: inline-block; width: auto; padding: 1rem 2rem; background: {ACCENT_AMBER};">
            📦 Explore Imports Analytics
        </a>
    </div>
</div>
""".format(PRIMARY_DARK=PRIMARY_DARK, MUTED=MUTED, ACCENT_AMBER=ACCENT_AMBER), unsafe_allow_html=True)