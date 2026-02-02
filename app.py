import re
import time
import pandas as pd
import requests
import streamlit as st
from bs4 import BeautifulSoup
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================= CONFIG =================
MAX_WORKERS = 10
COMPANY_NAME = "Swagelok"

# ================= SESSION STATE =================
if "results" not in st.session_state:
    st.session_state.results = None
if "running" not in st.session_state:
    st.session_state.running = False

# ================= EXTRACTOR =================
class SwagelokExtractor:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Mozilla/5.0"})

    def extract(self, url):
        result = {
            "Part": "Not Found",
            "Company": COMPANY_NAME,
            "URL": url,
            "UNSPSC Feature (Latest)": "Not Found",
            "UNSPSC Code": "Not Found"
        }

        if not isinstance(url, str) or not url.startswith("http"):
            return result

        try:
            r = self.session.get(url, timeout=25)
            if r.status_code != 200 or not r.text:
                return result

            soup = BeautifulSoup(r.text, "html.parser")

            part = self._extract_part_from_page(soup, url)
            if part:
                result["Part"] = part

            feature, code = self._extract_latest_unspsc(soup, r.text)
            if feature and code:
                result["UNSPSC Feature (Latest)"] = feature
                result["UNSPSC Code"] = code

            return result
        except Exception:
            return result

    def _extract_part_from_page(self, soup, url):
        for t in soup.find_all(string=re.compile(r"Part\s*#:?", re.I)):
            m = re.search(r"Part\s*#:\s*([A-Z0-9\-]+)", t.parent.get_text())
            if m:
                return m.group(1)

        for row in soup.find_all("tr"):
            tds = row.find_all("td")
            if len(tds) >= 2 and "part" in tds[0].get_text().lower():
                return tds[1].get_text(strip=True)

        m = re.search(r"part=([A-Z0-9\-]+)", url, re.I)
        return m.group(1) if m else None

    def _extract_latest_unspsc(self, soup, html):
        found = []
        for row in soup.find_all("tr"):
            tds = row.find_all("td")
            if len(tds) >= 2:
                v = re.search(r"UNSPSC\s*\(([\d.]+)\)", tds[0].get_text())
                c = re.fullmatch(r"\d{6,8}", tds[1].get_text(strip=True))
                if v and c:
                    found.append((tuple(map(int, v.group(1).split("."))), v.group(0), c.group()))

        if not found:
            return None, None

        found.sort(reverse=True)
        return found[0][1], found[0][2]

# ================= UI =================
st.set_page_config("Swagelok UNSPSC Intelligence Platform", "🔍", layout="wide")

st.markdown("""
<div style="background:linear-gradient(135deg,#0f4c81,#1fa2ff);
padding:2.2rem;border-radius:18px;color:white;text-align:center;margin-bottom:2rem">
<h1>🔍 Swagelok UNSPSC Intelligence Platform</h1>
<p>Page-Verified Parts • Latest UNSPSC • Audit-Ready</p>
</div>
""", unsafe_allow_html=True)

uploaded_file = st.file_uploader(
    "📤 Upload Excel file (one column must contain Swagelok URLs)",
    type=["xlsx", "xls"]
)

if uploaded_file:
    df = pd.read_excel(uploaded_file)

    url_col = next(
        (c for c in df.columns if df[c].astype(str).str.contains("http", na=False).any()),
        None
    )

    if not url_col:
        st.error("❌ No URL column detected.")
        st.stop()

    urls = df[url_col].tolist()
    total = len(urls)

    st.info(f"🔗 URL column detected: **{url_col}**")
    st.metric("📊 Total Rows", total)

    # ---------- RUN BUTTON ----------
    if st.button(
        "🚀 Start Intelligent Extraction",
        use_container_width=True,
        disabled=st.session_state.running
    ):
        st.session_state.running = True
        extractor = SwagelokExtractor()
        results = []

        progress = st.progress(0.0)
        status = st.empty()

        with ThreadPoolExecutor(MAX_WORKERS) as exe:
            futures = [exe.submit(extractor.extract, u) for u in urls]
            for i, f in enumerate(as_completed(futures), 1):
                results.append(f.result())
                progress.progress(i / total)
                status.markdown(
                    f"🔄 Processing <b>{i}</b> / <b>{total}</b>",
                    unsafe_allow_html=True
                )

        st.session_state.results = pd.DataFrame(results)
        st.session_state.running = False
        st.success("✅ Extraction completed successfully")

# ================= RESULTS (PERSISTENT) =================
if st.session_state.results is not None:
    out_df = st.session_state.results

    buffer = BytesIO()
    out_df.to_excel(buffer, index=False)

    st.download_button(
        "📥 Download Excel Results",
        buffer.getvalue(),
        "swagelok_unspsc_output.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True
    )

    st.dataframe(out_df, use_container_width=True)
