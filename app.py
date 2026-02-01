import re
import time
import pandas as pd
import requests
import streamlit as st
from bs4 import BeautifulSoup
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed

MAX_WORKERS = 12
COMPANY_NAME = "Swagelok"

# ==================================
# Swagelok UNSPSC Extractor (STRICT)
# ==================================
class SwagelokExtractor:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        })

    def extract(self, url: str):
        base_result = {
            "Part": self._part_from_url(url),
            "Company": COMPANY_NAME,
            "URL": url,
            "UNSPSC Feature (Latest)": "Not Found",
            "UNSPSC Code": "Not Found"
        }

        try:
            r = self.session.get(url, timeout=20)
            if r.status_code != 200 or not r.text:
                return base_result

            soup = BeautifulSoup(r.text, "html.parser")
            feature, code = self._extract_latest_unspsc(soup, r.text)

            if feature and code:
                base_result["UNSPSC Feature (Latest)"] = feature
                base_result["UNSPSC Code"] = code

            return base_result

        except Exception:
            return base_result

    def _part_from_url(self, url: str) -> str:
        if not isinstance(url, str):
            return "Not Found"
        m = re.search(r"part=([A-Z0-9\-]+)", url, re.IGNORECASE)
        return m.group(1) if m else "Not Found"

    def _extract_latest_unspsc(self, soup, html_text):
        found = []

        # ---- HTML table extraction (primary) ----
        for row in soup.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 2:
                continue

            label = cells[0].get_text(strip=True)
            value = cells[1].get_text(strip=True)

            version_match = re.search(r"UNSPSC\s*\(([\d.]+)\)", label)
            code_match = re.fullmatch(r"\d{6,8}", value)

            if version_match and code_match:
                version = self._parse_version(version_match.group(1))
                found.append((version, label, value))

        # ---- Regex fallback (secondary) ----
        if not found:
            for v, c in re.findall(r"UNSPSC\s*\(([\d.]+)\)[^\d]*(\d{6,8})", html_text):
                version = self._parse_version(v)
                found.append((version, f"UNSPSC ({v})", c))

        if not found:
            return None, None

        # ---- Select latest version ----
        found.sort(key=lambda x: x[0], reverse=True)
        return found[0][1], found[0][2]

    def _parse_version(self, v: str):
        try:
            return tuple(int(p) for p in v.split("."))
        except Exception:
            return (0,)

# ==================================
# STREAMLIT UI
# ==================================
st.set_page_config(
    page_title="Swagelok UNSPSC Intelligence Platform",
    page_icon="🔍",
    layout="wide"
)

st.markdown("""
<style>
.main-header {
    background: linear-gradient(135deg, #667eea, #764ba2);
    padding: 2rem;
    border-radius: 15px;
    color: white;
    text-align: center;
    margin-bottom: 2rem;
}
.stat-card {
    background: white;
    padding: 1.2rem;
    border-radius: 10px;
    box-shadow: 0 4px 12px rgba(0,0,0,0.08);
}
.success-box {
    background: linear-gradient(135deg, #11998e, #38ef7d);
    padding: 1.5rem;
    border-radius: 12px;
    color: white;
    text-align: center;
}
</style>
""", unsafe_allow_html=True)

st.markdown("""
<div class="main-header">
    <h1>🔍 Swagelok UNSPSC Intelligence Platform</h1>
    <p>Exact Input • No Modification • Enterprise Accuracy</p>
</div>
""", unsafe_allow_html=True)

st.markdown("""
### About
This platform extracts **UNSPSC data from Swagelok product pages**  
⚠️ **Important rule:**  
- Input data is processed **exactly as uploaded**
- No cleaning, no filtering, no deduplication
- Each input row produces one output row
""")

# ==================================
# FILE UPLOAD
# ==================================
uploaded_file = st.file_uploader(
    "Upload Excel file (must contain ONE URL column)",
    type=["xlsx", "xls"]
)

if uploaded_file:
    df = pd.read_excel(uploaded_file)

    # ---- Auto-detect URL column ----
    url_column = None
    for col in df.columns:
        if df[col].astype(str).str.contains("http", case=False, na=False).any():
            url_column = col
            break

    if not url_column:
        st.error("❌ No URL column detected in the uploaded file.")
        st.stop()

    urls = df[url_column].tolist()

    st.markdown(f"### 🔗 Detected URL column: `{url_column}`")

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Rows", len(df))
    col2.metric("Company", COMPANY_NAME)
    col3.metric("Processing Mode", "Exact Match")

    if st.button("🚀 Start Extraction", use_container_width=True):
        extractor = SwagelokExtractor()
        results = []

        progress = st.progress(0)
        status = st.empty()

        start = time.time()

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(extractor.extract, url): i
                for i, url in enumerate(urls)
            }

            for idx, future in enumerate(as_completed(futures), 1):
                results.append(future.result())
                progress.progress(idx / len(urls))
                status.markdown(f"Processing row {idx} / {len(urls)}")

        total_time = int(time.time() - start)

        output_df = pd.DataFrame(results)

        st.markdown(f"""
        <div class="success-box">
            <h2>✅ Completed</h2>
            <p>{len(output_df)} rows processed in {total_time} seconds</p>
        </div>
        """, unsafe_allow_html=True)

        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            output_df.to_excel(writer, index=False)

        st.download_button(
            "📥 Download Result",
            buffer.getvalue(),
            file_name=f"swagelok_unspsc_result_{int(time.time())}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )

        st.markdown("### 📋 Preview")
        st.dataframe(output_df, use_container_width=True)
