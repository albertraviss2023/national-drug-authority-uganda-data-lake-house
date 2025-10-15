# pages/gcp_data_entry.py — GCP-Specific Data Entry Form

import os
import io
import json
from pathlib import Path
from datetime import datetime

import pandas as pd
import streamlit as st
from s3fs import S3FileSystem  # Reuse from utils if extracted

# Import shared theme (assume utils/theme.py exists; for now, inline)
def apply_gcp_theme():
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    /* Paste full CSS from app.py here for modularity */
    /* ... (PRIMARY, etc. definitions and styles) ... */
    .upload-card { background: #fff; border: 1px solid #E2E8F0; border-radius: 16px; padding: 1.5rem; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
    .success-msg { background: linear-gradient(135deg, #0B7D3E 0%, #0A5D2E 100%); color: white; padding: 1rem; border-radius: 12px; text-align: center; }
    .error-msg { background: #FEE2E2; color: #DC2626; padding: 1rem; border-radius: 12px; }
    </style>
    """, unsafe_allow_html=True)

apply_gcp_theme()

# Page Config (Sub-page friendly)
st.set_page_config(page_title="GCP Data Entry", layout="wide")

# Header
st.title("🧪 GCP Data Entry")
st.markdown("Upload and validate GCP inspection Excel files. Tailored schema for studies, compliance, CAPA, etc.")

# Schema Definition (GCP-Specific; load from JSON for scalability)
@st.cache_data
def load_gcp_schema():
    schema = {
        "required_columns": ["title", "cta_number", "pi", "sites", "date_of_report", "compliance_status"],
        "data_types": {
            "title": "str",
            "cta_number": "str",
            "pi": "str",
            "sites": "str",
            "date_of_report": "datetime",
            "compliance_status": ["compliant", "minor", "major", "critical"],
            "capa_verified": [True, False, None],
            "date_of_capa_verification": "datetime"
        },
        "optional_columns": ["inspectors", "justification_for_inspection"]
    }
    return schema

gcp_schema = load_gcp_schema()

# S3 Upload Helper (GCP Bronze Path)
@st.cache_resource
def get_s3_fs():
    # Reuse from gold loader
    key = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
    secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")
    endpoint = os.environ.get("CATALOG_S3_ENDPOINT", "http://minio:9000")
    fs = S3FileSystem(key=key, secret=secret, client_kwargs={"endpoint_url": endpoint})
    return fs

def upload_to_gcp_bronze(df, filename):
    bucket = "warehouse"  # From env
    upload_date = datetime.now().strftime("%Y%m%d")
    path = f"bronze/dps/gcp/raw/{upload_date}/{filename.replace('.xlsx', '.parquet')}"
    with get_s3_fs().open(f"{bucket}/{path}", "wb") as f:
        df.to_parquet(f)
    return path

# Form UI
st.markdown('<div class="upload-card">', unsafe_allow_html=True)
uploaded_file = st.file_uploader("Choose GCP Excel File", type="xlsx", help="Upload .xlsx with GCP inspection data.")

if uploaded_file:
    # Preview
    df = pd.read_excel(uploaded_file)
    st.subheader("📋 Data Preview")
    st.dataframe(df.head(), use_container_width=True)

    # Validation
    if st.button("Validate Schema & Types"):
        errors = []
        # Check required columns
        missing_cols = [col for col in gcp_schema["required_columns"] if col not in df.columns]
        if missing_cols:
            errors.append(f"Missing columns: {', '.join(missing_cols)}")

        # Check data types & enums
        for col, dtype in gcp_schema["data_types"].items():
            if col in df.columns:
                if dtype == "datetime":
                    try:
                        pd.to_datetime(df[col])
                    except:
                        errors.append(f"Invalid dates in {col}")
                elif isinstance(dtype, list):
                    invalid = df[col].dropna().apply(lambda x: str(x).lower() not in [e.lower() for e in dtype])
                    if invalid.any():
                        errors.append(f"Invalid values in {col}: {df.loc[invalid, col].unique()}")

        if errors:
            st.error("Validation Failed:")
            for err in errors:
                st.markdown(f'<div class="error-msg">{err}</div>', unsafe_allow_html=True)
        else:
            st.success("✅ Validation Passed! Ready to upload.")

            # Metadata
            col1, col2 = st.columns(2)
            with col1:
                fiscal_year = st.selectbox("Fiscal Year", options=["2024_2025", "2025_2026"])  # From utils later
            with col2:
                notes = st.text_area("Upload Notes")

            if st.button("Upload to Bronze Layer"):
                # Add metadata columns
                df["upload_date"] = datetime.now()
                df["fiscal_year"] = fiscal_year
                df["notes"] = notes
                df["upload_user"] = st.session_state.get("user_id", "anonymous")  # Future RBAC

                filename = uploaded_file.name
                path = upload_to_gcp_bronze(df, filename)
                st.markdown(f'<div class="success-msg">Uploaded to s3://warehouse/{path}</div>', unsafe_allow_html=True)
                st.balloons()

st.markdown('</div>', unsafe_allow_html=True)

# Footer
st.markdown("---")
st.caption("GCP Data Entry • Tailored for inspection uploads. Future: RBAC restricts to GCP users.")