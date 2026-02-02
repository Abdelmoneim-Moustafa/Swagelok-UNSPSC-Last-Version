import re
import time
import pandas as pd
import requests
import streamlit as st
from bs4 import BeautifulSoup
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================= CONFIG =================
MAX_WORKERS = 15
COMPANY_NAME = "Swagelok"
TIMEOUT = 20

# ================= SESSION STATE =================
if "results" not in st.session_state:
    st.session_state.results = None
if "running" not in st.session_state:
    st.session_state.running = False

# ================= EXTRACTOR =================
class SwagelokExtractor:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0",
            "Accept-Language": "en-US,en;q=0.9"
        })

    def extract(self, url):
        row = {
            "Part": "Not Found",
            "Company": COMPANY_NAME,
            "URL": url,
            "UNSPSC Feature (Latest)": "Not Found",
            "UNSPSC Code": "Not Found",
            "Confidence Score": 0
        }

        if not isinstance(url, str) or not url.startswith("http"):
            return row

        try:
            r = self.session.get(url, timeout=TIMEOUT)
            if r.status_code != 200 or not r.text:
                return row

            soup = BeautifulSoup(r.text, "lxml")

            part = self._extract_part(soup, url)
            if part:
                row["Part"] = part
                row["Confidence Score"] += 50

            feature, code = self._extract_unspsc(soup, r.text)
            if feature and code:
                row["UNSPSC Feature (Latest)"] = feature
                row["UNSPSC Code"] = code
                row["Confidence Score"] += 50

            return row

        except Exception:
            return row

    # ---------- PART ----------
    def _extract_part(self, soup, url):
        for t in soup.find_all(string=re.compile(r"Part\s*#:?", re.I)):
            m = re.search(r"Part\s*#:\s*([A-Z0-9\-]+)", t.parent.get_text(" ", strip=True))
            if m:
                return m.group(1)

        for row in soup.select("tr"):
            tds = row.find_all("td")
            if len(tds) >= 2 and "part" in tds[0].get_text(strip=True).lower():
                val = tds[1].get_text(strip=True)
                if "-" in val:
                    return val

        m = re.search(r"part=([A-Z0-9\-]+)", url, re.I)
        return m.group(1) if m else None

    # ---------- UNSPSC ----------
    def _extract_unspsc(self, soup, html):
        found = []

        for row in soup.select("tr"):
            tds = row.find_all("td")
            if len(tds) >= 2:
                vm = re.search(r"UNSPSC\s*\(([\d.]+)\)", tds[0].get_text())
                cm = re.fullmatch(r"\d{6,8}", tds[1].get_text(strip=True))
                if vm and cm:
                    found.append((tuple(map(int, vm.group(1).split("."))), vm.group(0), cm.group()))

        if not found:
            return None, None

        found.sort(reverse=True)
        return found[0][1], found[0][2]

# ================= UI =================
st.set_page_config("Swagelok UNSPSC Intelligence Platform", "🔍", layout="wide")

st.markdown("""
<style>
.header {background:linear-gradient(135deg,#0f4c81,#1fa2ff);
padding:2.2rem;border-radius:18px;color:white;text-align:center;margin-bottom:2rem}
.card {background:#f8fafc;padding:1.4rem;border-radius:14px;
border-left:6px solid #1fa2ff;margin-bottom:1.4rem}
.good {color:#1e8449;font-weight:700}
.bad {color:#c0392b;font-weight:700}
</style>
""", unsafe_allow_html=True)

st.markdown("""
<div class="header">
<h1>🔍 Swagelok UNSPSC Intelligence Platform</h1>
<p>Fast • Page-Verified • Audit-Ready</p>
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

    valid_urls = sum(isinstance(u, str) and u.startswith("http") for u in urls)
    coverage = round((valid_urls / total) * 100, 2)

    # ---------- FILE ANALYSIS ----------
    st.markdown("### 📂 File Analysis")
    st.markdown(f"""
    <div class="card">
    <b>Total Rows:</b> {total}<br>
    <b>Valid URLs:</b> {valid_urls}<br>
    <b>Coverage:</b>
    <span class="{ 'good' if coverage >= 90 else 'bad' }">{coverage}%</span>
    </div>
    """, unsafe_allow_html=True)

    # ---------- RUN BUTTON ----------
    if st.button("🚀 Start Fast Extraction", use_container_width=True, disabled=st.session_state.running):
        st.session_state.running = True
        extractor = SwagelokExtractor()
        results = []

        progress = st.progress(0.0)

        with ThreadPoolExecutor(MAX_WORKERS) as exe:
            futures = [exe.submit(extractor.extract, u) for u in urls]
            for i, f in enumerate(as_completed(futures), 1):
                results.append(f.result())
                progress.progress(i / total)

        out_df = pd.DataFrame(results)
        st.session_state.results = out_df
        st.session_state.running = False

# ================= RESULTS =================
if st.session_state.results is not None:
    out_df = st.session_state.results

    part_rate = round((out_df["Part"] != "Not Found").mean() * 100, 2)
    unspsc_rate = round((out_df["UNSPSC Code"] != "Not Found").mean() * 100, 2)
    avg_conf = round(out_df["Confidence Score"].mean(), 2)

    # ---------- OUTPUT ANALYSIS ----------
    st.markdown("### 📊 Output Analysis")
    st.markdown(f"""
    <div class="card">
    <b>Part Detection:</b>
    <span class="{ 'good' if part_rate >= 90 else 'bad' }">{part_rate}%</span><br>
    <b>UNSPSC Coverage:</b>
    <span class="{ 'good' if unspsc_rate >= 85 else 'bad' }">{unspsc_rate}%</span><br>
    <b>Average Confidence Score:</b> {avg_conf}
    </div>
    """, unsafe_allow_html=True)

    # ---------- QA SUMMARY SHEET ----------
    qa_summary = pd.DataFrame({
        "Metric": [
            "Total Rows",
            "Parts Found %",
            "UNSPSC Found %",
            "Average Confidence Score"
        ],
        "Value": [
            total,
            part_rate,
            unspsc_rate,
            avg_conf
        ]
    })

    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        out_df.to_excel(writer, index=False, sheet_name="Results")
        qa_summary.to_excel(writer, index=False, sheet_name="QA Summary")

    st.download_button(
        "📥 Download Excel (Results + QA Summary)",
        buffer.getvalue(),
        "swagelok_unspsc_output.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True
    )

    st.dataframe(out_df, use_container_width=True)
