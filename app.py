# Swagelok UNSPSC Intelligence Platform
# Clean rewrite from scratch – fast, validated, no duplicates
# -------------------------------------------------------------

import re
import time
import requests
import pandas as pd
import streamlit as st
from bs4 import BeautifulSoup
from urllib.parse import urlparse

# ============================
# Page configuration
# ============================
st.set_page_config(
    page_title="Swagelok UNSPSC Intelligence Platform",
    page_icon="🔎",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ============================
# Styling (clean + professional)
# ============================
st.markdown(
    """
    <style>
        body { background-color: #0e1117; }
        .hero {
            background: linear-gradient(90deg, #0f4c81, #1f8ef1);
            padding: 2.5rem;
            border-radius: 18px;
            text-align: center;
            color: white;
            margin-bottom: 2rem;
        }
        .hero h1 { font-size: 2.4rem; margin-bottom: 0.5rem; }
        .hero p { font-size: 1.1rem; opacity: 0.95; }
        .metric {
            background: #111827;
            padding: 1rem;
            border-radius: 12px;
            text-align: center;
            color: #e5e7eb;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

# ============================
# Header
# ============================
st.markdown(
    """
    <div class="hero">
        <h1>🔍 Swagelok UNSPSC Intelligence Platform</h1>
        <p>Fast • Page-Verified • Latest UNSPSC Only • Zero Guessing 😎</p>
    </div>
    """,
    unsafe_allow_html=True,
)

# ============================
# Helpers
# ============================

HEADERS = {
    "User-Agent": "Mozilla/5.0 (UNSPSC-Intel/1.0; +https://streamlit.app)"
}

UNSPSC_PATTERN = re.compile(r"UNSPSC\s*\(([^)]+)\)", re.I)


def normalize_part(part: str) -> str:
    """Ensure full part number is kept exactly as shown on site."""
    if not part:
        return ""
    return part.strip()


def extract_part_number(soup: BeautifulSoup) -> str:
    """Extract full Part # (no truncation like CWS-C only)."""
    label = soup.find(text=re.compile(r"Part #", re.I))
    if not label:
        return ""
    parent = label.parent
    value = parent.find("span") or parent.find("strong") or parent
    return normalize_part(value.get_text())


def extract_unspsc_latest(soup: BeautifulSoup):
    """
    From Specifications table:
    - Collect all UNSPSC versions
    - Return ONLY the latest (highest version)
    """
    unspsc_rows = []

    for row in soup.select("table tr"):
        cells = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
        if len(cells) != 2:
            continue
        attr, value = cells
        match = UNSPSC_PATTERN.search(attr)
        if match:
            try:
                version = float(match.group(1))
                code = re.sub(r"\D", "", value)
                if code:
                    unspsc_rows.append((version, code))
            except ValueError:
                pass

    if not unspsc_rows:
        return "", ""

    # pick highest version ONLY
    latest_version, latest_code = max(unspsc_rows, key=lambda x: x[0])
    return f"UNSPSC ({latest_version})", latest_code


def scrape_product(url: str) -> dict:
    """Scrape ONE product page safely."""
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "lxml")

    part = extract_part_number(soup)
    unspsc_feature, unspsc_code = extract_unspsc_latest(soup)

    return {
        "Part": part,
        "Company": "Swagelok",
        "URL": url,
        "UNSPSC Feature (Latest)": unspsc_feature,
        "UNSPSC Code": unspsc_code,
    }


# ============================
# Upload section
# ============================

st.subheader("📤 Upload Excel file")
st.caption("One column must contain Swagelok product URLs (.xlsx / .xls)")

uploaded_file = st.file_uploader("", type=["xlsx", "xls"])

if uploaded_file:
    df_input = pd.read_excel(uploaded_file)

    url_col = None
    for col in df_input.columns:
        if df_input[col].astype(str).str.contains("swagelok.com", case=False).any():
            url_col = col
            break

    if not url_col:
        st.error("❌ No Swagelok URLs found in file")
        st.stop()

    urls = (
        df_input[url_col]
        .dropna()
        .astype(str)
        .str.strip()
        .unique()
        .tolist()
    )

    st.info(f"🔗 {len(urls)} unique URLs detected")

    start = time.time()
    results = []
    errors = []

    progress = st.progress(0)

    for i, url in enumerate(urls, 1):
        try:
            data = scrape_product(url)
            if data["Part"] and data["UNSPSC Code"]:
                results.append(data)
        except Exception as e:
            errors.append({"URL": url, "Error": str(e)})

        progress.progress(i / len(urls))

    elapsed = round(time.time() - start, 2)

    df = pd.DataFrame(results)

    # ============================
    # Validation rules (STRICT)
    # ============================
    df = df.drop_duplicates(subset=["Part", "UNSPSC Code", "URL"])
    df = df[df["Part"].str.len() > 3]
    df = df[df["UNSPSC Code"].str.len() >= 6]

    # ============================
    # Metrics
    # ============================
    c1, c2, c3 = st.columns(3)
    c1.metric("Valid Parts", len(df))
    c2.metric("Errors", len(errors))
    c3.metric("Time (sec)", elapsed)

    # ============================
    # Output table
    # ============================
    st.subheader("📊 Final Clean Output (Validated)")
    st.dataframe(df, use_container_width=True)

    # ============================
    # Download
    # ============================
    out = df.to_excel(index=False, engine="openpyxl")
    st.download_button(
        "⬇️ Download Excel",
        data=out,
        file_name="swagelok_unspsc_clean.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    if errors:
        with st.expander("⚠️ Errors (not included in output)"):
            st.dataframe(pd.DataFrame(errors))

else:
    st.caption("Built for serious data people 🧠")
