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

    # ---------- PART ----------
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

    # ---------- UNSPSC ----------
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
<style>
.header {
    background: linear-gradient(135deg,#0f4c81,#1fa2ff);
    padding:2.5rem;
    border-radius:18px;
    color:white;
    text-align:center;
    margin-bottom:2rem;
}
.card {
    background:#f8fafc;
    padding:1.6rem;
    border-radius:14px;
    border-left:6px solid #1fa2ff;
    margin-bottom:1.5rem;
}
.good {color:#1e8449;font-weight:700;}
.bad {color:#c0392b;font-weight:700;}
table {width:100%;border-collapse:collapse;}
th,td {padding:10px;border-bottom:1px solid #ddd;}
th {background:#eef2f7;}
</style>
""", unsafe_allow_html=True)

st.markdown("""
<div class="header">
    <h1>🔍 Swagelok UNSPSC Intelligence Platform</h1>
    <p>Page-Verified Parts • Latest UNSPSC • Audit-Ready Results</p>
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

    total = len(df)
    valid = df[url_col].astype(str).str.startswith("http").sum()
    invalid = total - valid
    coverage = round((valid / total) * 100, 2)

    # ---------- FILE ANALYSIS ----------
    st.markdown("### 📂 File Analysis")
    st.markdown(f"""
    <div class="card">
        <b>Total Rows:</b> {total}<br>
        <b>URL Column:</b> {url_col}<br>
        <b>Valid URLs:</b> {valid}<br>
        <b>Invalid / Empty URLs:</b> {invalid}<br>
        <b>Coverage:</b> <span class="{ 'good' if coverage >= 90 else 'bad' }">{coverage}%</span>
    </div>
    """, unsafe_allow_html=True)

    urls = df[url_col].tolist()

    if st.button("🚀 Start Extraction", use_container_width=True):
        extractor = SwagelokExtractor()
        results = []

        with ThreadPoolExecutor(MAX_WORKERS) as exe:
            futures = [exe.submit(extractor.extract, u) for u in urls]
            for f in as_completed(futures):
                results.append(f.result())

        out_df = pd.DataFrame(results)

        part_ok = (out_df["Part"] != "Not Found").sum()
        unspsc_ok = (out_df["UNSPSC Code"] != "Not Found").sum()

        part_rate = round((part_ok / total) * 100, 2)
        unspsc_rate = round((unspsc_ok / total) * 100, 2)

        # ---------- OUTPUT ANALYSIS ----------
        st.markdown("### 📊 Output Analysis")
        st.markdown(f"""
        <div class="card">
            <b>Part Detection:</b>
            <span class="{ 'good' if part_rate >= 90 else 'bad' }">{part_rate}%</span><br>
            <b>UNSPSC Detection:</b>
            <span class="{ 'good' if unspsc_rate >= 85 else 'bad' }">{unspsc_rate}%</span><br>
            <b>Rows Needing Review:</b> {total - unspsc_ok}
        </div>
        """, unsafe_allow_html=True)

        issues = out_df[
            (out_df["Part"] == "Not Found") |
            (out_df["UNSPSC Code"] == "Not Found")
        ]

        if not issues.empty:
            st.markdown("### ⚠ Rows Requiring Review")
            st.markdown(issues.to_html(index=False), unsafe_allow_html=True)

        buffer = BytesIO()
        out_df.to_excel(buffer, index=False)

        st.download_button(
            "📥 Download Excel Output",
            buffer.getvalue(),
            "swagelok_unspsc_output.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )

        st.markdown("### ✅ Final Output")
        st.dataframe(out_df, use_container_width=True)
