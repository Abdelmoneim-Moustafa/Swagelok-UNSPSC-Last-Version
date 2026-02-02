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
            if r.status_code != 200:
                return result

            soup = BeautifulSoup(r.text, "html.parser")

            part = self._extract_part(soup, url)
            if part:
                result["Part"] = part

            feature, code = self._extract_unspsc(soup, r.text)
            if feature:
                result["UNSPSC Feature (Latest)"] = feature
                result["UNSPSC Code"] = code

            return result
        except Exception:
            return result

    def _extract_part(self, soup, url):
        for t in soup.find_all(string=re.compile(r"Part\s*#:?", re.I)):
            m = re.search(r"Part\s*#:\s*([A-Z0-9\-]+)", t.parent.get_text())
            if m:
                return m.group(1)

        for row in soup.find_all("tr"):
            tds = row.find_all("td")
            if len(tds) >= 2 and "part" in tds[0].get_text().lower():
                return tds[1].get_text(strip=True)

        h1 = soup.find("h1")
        if h1 and "-" in h1.get_text():
            return h1.get_text(strip=True)

        m = re.search(r"part=([A-Z0-9\-]+)", url, re.I)
        return m.group(1) if m else None

    def _extract_unspsc(self, soup, html):
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
st.set_page_config("Swagelok UNSPSC Dashboard", "📊", layout="wide")
st.title("📊 Swagelok UNSPSC Intelligence Dashboard")
st.caption("Extraction • Validation • Data Quality Analysis")

# ================= FILE UPLOAD =================
uploaded_file = st.file_uploader("Upload Excel file", type=["xlsx", "xls"])

if uploaded_file:
    df = pd.read_excel(uploaded_file)

    # -------- FILE ANALYSIS --------
    st.subheader("📂 File Analysis")
    url_col = next((c for c in df.columns if df[c].astype(str).str.contains("http", na=False).any()), None)

    total_rows = len(df)
    valid_urls = df[url_col].astype(str).str.startswith("http").sum()
    invalid_urls = total_rows - valid_urls

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Rows", total_rows)
    col2.metric("Valid URLs", valid_urls)
    col3.metric("Invalid / Empty URLs", invalid_urls)

    pie_df = pd.DataFrame({
        "Type": ["Valid URLs", "Invalid URLs"],
        "Count": [valid_urls, invalid_urls]
    })
    st.bar_chart(pie_df.set_index("Type"))

    urls = df[url_col].tolist()

    if st.button("🚀 Start Extraction", use_container_width=True):
        extractor = SwagelokExtractor()
        results = []

        with ThreadPoolExecutor(MAX_WORKERS) as exe:
            futures = [exe.submit(extractor.extract, u) for u in urls]
            for f in as_completed(futures):
                results.append(f.result())

        out_df = pd.DataFrame(results)

        # -------- OUTPUT ANALYSIS --------
        st.subheader("📊 Output Analysis")

        part_found = (out_df["Part"] != "Not Found").sum()
        unspsc_found = (out_df["UNSPSC Code"] != "Not Found").sum()

        col1, col2, col3 = st.columns(3)
        col1.metric("Parts Found", part_found)
        col2.metric("UNSPSC Found", unspsc_found)
        col3.metric("Success Rate %", round((unspsc_found / total_rows) * 100, 2))

        chart_df = pd.DataFrame({
            "Metric": ["Part Found", "Part Missing"],
            "Count": [part_found, total_rows - part_found]
        })
        st.bar_chart(chart_df.set_index("Metric"))

        # -------- DATA QUALITY TABLE --------
        st.subheader("⚠ Rows Needing Review")
        issues = out_df[
            (out_df["Part"] == "Not Found") |
            (out_df["UNSPSC Code"] == "Not Found")
        ]
        st.dataframe(issues, use_container_width=True)

        # -------- DOWNLOAD --------
        buffer = BytesIO()
        out_df.to_excel(buffer, index=False)

        st.download_button(
            "📥 Download Excel Output",
            buffer.getvalue(),
            "swagelok_unspsc_output.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )

        st.subheader("✅ Full Output")
        st.dataframe(out_df, use_container_width=True)
