"""
🔍 Swagelok UNSPSC Intelligence Platform
FINAL – PART EXTRACTION FIXED & VERIFIED

Author: Abdelmoneim Moustafa
"""

import time
import re
import pandas as pd
import requests
import streamlit as st
from bs4 import BeautifulSoup
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed

# ==================== CONFIG ====================
MAX_WORKERS = 6
COMPANY_NAME = "Swagelok"
TIMEOUT = 20
BATCH_SIZE = 100

# ==================== PAGE CONFIG ====================
st.set_page_config(
    page_title="Swagelok UNSPSC Platform",
    page_icon="🔍",
    layout="wide"
)

# ==================== STYLES ====================
st.markdown("""
<style>
.main-header {
    background: linear-gradient(135deg, #0f2027, #203a43, #2c5364);
    padding: 2.5rem;
    border-radius: 15px;
    color: white;
    text-align: center;
    margin-bottom: 2rem;
}
.success-box {
    background: linear-gradient(135deg, #11998e, #38ef7d);
    padding: 1.5rem;
    border-radius: 12px;
    color: white;
    text-align: center;
}
.checkpoint-box {
    background: #fff3e0;
    border-left: 4px solid #ff9800;
    padding: 1rem;
    border-radius: 8px;
}
</style>
""", unsafe_allow_html=True)

# ==================== PART EXTRACTION (FIXED) ====================
def extract_part_from_page(html: str) -> str:
    """
    Extract ONLY from site text:
    'Part #: XXXXX'
    """
    soup = BeautifulSoup(html, "html.parser")

    for text in soup.stripped_strings:
        if text.startswith("Part #"):
            part = text.split("Part #")[-1].replace(":", "").strip()
            if part:
                return part

    raise ValueError("Part not found on product page")

def extract_part_from_url(url: str) -> str:
    if "/p/" not in url:
        return ""
    return url.split("/p/")[-1].split("?")[0].strip()

# ==================== EXTRACTOR ====================
class SwagelokExtractor:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0"
        })

    def extract(self, url: str) -> dict:
        result = {
            "Company": COMPANY_NAME,
            "URL": url,
            "Part": "",
            "Part_URL": "",
            "Part_Check": False,
            "UNSPSC Feature (Latest)": "Not Found",
            "UNSPSC Code": "Not Found"
        }

        if not url or not isinstance(url, str):
            return result

        r = self.session.get(url, timeout=TIMEOUT)
        r.raise_for_status()

        # ✅ PART (SOURCE OF TRUTH = PAGE)
        part_page = extract_part_from_page(r.text)
        part_url = extract_part_from_url(url)

        result["Part"] = part_page
        result["Part_URL"] = part_url
        result["Part_Check"] = part_page == part_url

        # UNSPSC (unchanged – per your request)
        for m in re.findall(r'UNSPSC[^0-9]*(\d{6,8})', r.text, re.I):
            result["UNSPSC Feature (Latest)"] = f"UNSPSC ({m})"
            result["UNSPSC Code"] = m
            break

        return result

# ==================== UI ====================
st.markdown("""
<div class="main-header">
    <h1>🔍 Swagelok UNSPSC Intelligence Platform</h1>
    <h3>Part Extraction = Site Truth • Verified Against URL</h3>
</div>
""", unsafe_allow_html=True)

uploaded_file = st.file_uploader("📤 Upload Excel File", type=["xlsx", "xls"])

if uploaded_file:
    df = pd.read_excel(uploaded_file)

    url_col = next(
        (c for c in df.columns if df[c].astype(str).str.contains("http", na=False).any()),
        None
    )

    if not url_col:
        st.error("❌ No URL column detected")
        st.stop()

    urls = [
        str(u).strip() if pd.notna(u) and str(u).strip() else None
        for u in df[url_col].tolist()
    ]

    st.success(f"✅ Loaded {len(urls)} rows")

    if st.button("🚀 Start Extraction", use_container_width=True):
        extractor = SwagelokExtractor()
        all_results = []

        progress = st.progress(0.0)
        status = st.empty()
        start = time.time()

        batches = (len(urls) + BATCH_SIZE - 1) // BATCH_SIZE

        for b in range(batches):
            s = b * BATCH_SIZE
            e = min((b + 1) * BATCH_SIZE, len(urls))
            batch_urls = urls[s:e]

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as exe:
                futures = [exe.submit(extractor.extract, u) for u in batch_urls]

                for f in as_completed(futures):
                    all_results.append(f.result())
                    done = len(all_results)
                    progress.progress(done / len(urls))

                    elapsed = time.time() - start
                    speed = done / elapsed if elapsed else 0
                    status.write(f"⚡ {done}/{len(urls)} | {speed:.1f}/sec")

        output = pd.DataFrame(all_results)

        st.markdown(f"""
        <div class="success-box">
            <h2>✅ Completed</h2>
            <p>Rows: {len(output)} | Time: {int(time.time()-start)}s</p>
            <p>Part Match TRUE: {(output["Part_Check"]==True).sum()}</p>
        </div>
        """, unsafe_allow_html=True)

        buf = BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as w:
            output.to_excel(w, index=False)

        st.download_button(
            "📥 Download Final File",
            buf.getvalue(),
            file_name="swagelok_final_verified.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )

        st.dataframe(output, use_container_width=True, height=400)
