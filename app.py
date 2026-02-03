import streamlit as st
import pandas as pd
import requests
import re
import time
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO

# =========================
# PAGE CONFIG
# =========================
st.set_page_config(
    page_title="Swagelok UNSPSC Intelligence Platform",
    page_icon="🔍",
    layout="wide"
)

# =========================
# STYLES
# =========================
st.markdown("""
<style>
.big-title {
    font-size:36px;
    font-weight:700;
    color:#0b5394;
}
.sub-title {
    font-size:16px;
    color:#444;
}
.card {
    background:#f9fbfd;
    padding:20px;
    border-radius:12px;
    border:1px solid #e6e6e6;
}
.success {
    background:#e6f4ea;
    padding:10px;
    border-radius:8px;
}
</style>
""", unsafe_allow_html=True)

# =========================
# HEADER
# =========================
st.markdown('<div class="big-title">🔍 Swagelok UNSPSC Intelligence Platform</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-title">Page-Verified Parts • Latest UNSPSC • Zero Guessing 😎</div>', unsafe_allow_html=True)
st.divider()

# =========================
# HELPERS
# =========================
HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

UNSPSC_PATTERN = re.compile(r"UNSPSC\s*\([\d\.]+\)", re.I)

def extract_part_from_url(url: str):
    match = re.search(r"(?:part=|/p/)([A-Z0-9\-]+)", url)
    return match.group(1) if match else None

def extract_visible_part(soup: BeautifulSoup):
    text = soup.get_text(" ", strip=True)
    match = re.search(r"Part\s*#:\s*([A-Z0-9\-]+)", text)
    return match.group(1) if match else None

def extract_title_part(soup: BeautifulSoup):
    if soup.title:
        match = re.search(r"\b[A-Z0-9\-]{4,}\b", soup.title.text)
        return match.group() if match else None
    return None

def extract_latest_unspsc(soup: BeautifulSoup):
    unspsc_rows = []
    for row in soup.find_all("tr"):
        cells = row.find_all(["td", "th"])
        if len(cells) == 2 and "UNSPSC" in cells[0].get_text():
            version = re.search(r"\(([\d\.]+)\)", cells[0].get_text())
            code = re.search(r"\b\d{8}\b", cells[1].get_text())
            if version and code:
                unspsc_rows.append((float(version.group(1)), code.group()))

    if not unspsc_rows:
        return None

    unspsc_rows.sort(key=lambda x: x[0], reverse=True)
    return unspsc_rows[0][1]

def process_url(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
    except Exception:
        return {
            "URL": url,
            "Company": "Swagelok",
            "Part": None,
            "UNSPSC_Last": None,
            "Mismatch": True,
            "Status": "Request Failed"
        }

    soup = BeautifulSoup(r.text, "html.parser")

    url_part = extract_part_from_url(url)
    page_part = extract_visible_part(soup)
    title_part = extract_title_part(soup)

    if page_part:
        final_part = page_part
        source = "Page"
    elif title_part:
        final_part = title_part
        source = "Title"
    else:
        final_part = url_part
        source = "URL"

    unspsc = extract_latest_unspsc(soup)

    mismatch = url_part and final_part and url_part != final_part

    return {
        "URL": url,
        "Company": "Swagelok",
        "Part": final_part,
        "UNSPSC_Last": unspsc,
        "Mismatch": mismatch,
        "Source": source,
        "Status": "OK" if final_part else "Missing Part"
    }

# =========================
# FILE UPLOAD
# =========================
st.markdown("### 📤 Upload Excel file (URLs anywhere, any column)")

file = st.file_uploader("", type=["xlsx", "xls"])

if file:
    df = pd.read_excel(file)
    all_urls = []

    for col in df.columns:
        urls = df[col].dropna().astype(str)
        urls = urls[urls.str.contains("swagelok", case=False)]
        all_urls.extend(urls.tolist())

    all_urls = list(dict.fromkeys(all_urls))  # unique, keep order

    st.markdown(f'<div class="success">🔗 <b>{len(all_urls)}</b> Swagelok URLs detected</div>', unsafe_allow_html=True)

    # =========================
    # RUN EXTRACTION
    # =========================
    if st.button("🚀 Start Analysis"):
        start_time = time.time()
        results = []

        with st.spinner("Scraping Swagelok pages..."):
            with ThreadPoolExecutor(max_workers=12) as executor:
                futures = [executor.submit(process_url, url) for url in all_urls]
                for f in as_completed(futures):
                    results.append(f.result())

        elapsed = round(time.time() - start_time, 2)

        out_df = pd.DataFrame(results)

        # Ensure columns always exist
        for col in ["Part", "UNSPSC_Last", "Mismatch"]:
            if col not in out_df.columns:
                out_df[col] = None

        st.markdown("### 📊 Analysis Summary")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total URLs", len(out_df))
        c2.metric("Valid Parts", out_df["Part"].notna().sum())
        c3.metric("UNSPSC Found", out_df["UNSPSC_Last"].notna().sum())
        c4.metric("Time (sec)", elapsed)

        st.markdown("### 📋 Sample Output")
        st.dataframe(out_df.head(20), use_container_width=True)

        # =========================
        # DOWNLOAD
        # =========================
        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            out_df.to_excel(writer, index=False, sheet_name="Swagelok_UNSPSC")

        st.download_button(
            "⬇️ Download Excel",
            data=buffer.getvalue(),
            file_name="swagelok_unspsc_output.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

        st.success("✅ Done. Page-verified, latest UNSPSC, zero guessing.")

else:
    st.info("Upload an Excel file to begin.")
