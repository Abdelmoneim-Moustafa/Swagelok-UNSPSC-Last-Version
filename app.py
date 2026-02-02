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
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0"
        })

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

            # ---- PART (STRICT) ----
            part = self._extract_part_from_page(soup, url)
            if part:
                result["Part"] = part

            # ---- UNSPSC ----
            feature, code = self._extract_latest_unspsc(soup, r.text)
            if feature and code:
                result["UNSPSC Feature (Latest)"] = feature
                result["UNSPSC Code"] = code

            return result

        except Exception:
            return result

    # ---------- PART ----------
    def _extract_part_from_page(self, soup, url):
        # 1️⃣ Exact "Part #:" label
        for t in soup.find_all(string=re.compile(r"Part\s*#:?", re.IGNORECASE)):
            text = t.parent.get_text(" ", strip=True)
            m = re.search(r"Part\s*#:\s*([A-Z0-9\-]+)", text, re.IGNORECASE)
            if m:
                return m.group(1)

        # 2️⃣ Table metadata
        for row in soup.find_all("tr"):
            tds = row.find_all("td")
            if len(tds) >= 2:
                label = tds[0].get_text(strip=True).lower()
                value = tds[1].get_text(strip=True)
                if "part" in label and self._looks_like_part(value):
                    return value

        # 3️⃣ H1 fallback
        h1 = soup.find("h1")
        if h1 and self._looks_like_part(h1.get_text(strip=True)):
            return h1.get_text(strip=True)

        # 4️⃣ URL fallback
        return self._part_from_url(url)

    def _part_from_url(self, url):
        m = re.search(r"part=([A-Z0-9\-]+)", url, re.IGNORECASE)
        return m.group(1) if m else None

    def _looks_like_part(self, text):
        return (
            isinstance(text, str)
            and "-" in text
            and any(c.isdigit() for c in text)
            and 4 <= len(text) <= 40
        )

    # ---------- UNSPSC ----------
    def _extract_latest_unspsc(self, soup, html):
        found = []

        for row in soup.find_all("tr"):
            tds = row.find_all("td")
            if len(tds) >= 2:
                label = tds[0].get_text(strip=True)
                value = tds[1].get_text(strip=True)

                vm = re.search(r"UNSPSC\s*\(([\d.]+)\)", label)
                cm = re.fullmatch(r"\d{6,8}", value)

                if vm and cm:
                    found.append((self._parse_version(vm.group(1)), label, value))

        if not found:
            for v, c in re.findall(r"UNSPSC\s*\(([\d.]+)\)[^\d]*(\d{6,8})", html):
                found.append((self._parse_version(v), f"UNSPSC ({v})", c))

        if not found:
            return None, None

        found.sort(key=lambda x: x[0], reverse=True)
        return found[0][1], found[0][2]

    def _parse_version(self, v):
        try:
            return tuple(int(x) for x in v.split("."))
        except Exception:
            return (0,)

# ================= STREAMLIT UI =================
st.set_page_config("Swagelok UNSPSC Platform", "🔍", layout="wide")

st.title("🔍 Swagelok UNSPSC Intelligence Platform")
st.caption("Exact Input • No Cleaning • Page-Verified Part #")

uploaded_file = st.file_uploader("Upload Excel file (one column must contain URLs)", type=["xlsx", "xls"])

if uploaded_file:
    df = pd.read_excel(uploaded_file)

    # Auto-detect URL column
    url_col = None
    for col in df.columns:
        if df[col].astype(str).str.contains("http", na=False).any():
            url_col = col
            break

    if not url_col:
        st.error("No URL column detected.")
        st.stop()

    urls = df[url_col].tolist()

    st.info(f"Detected URL column: {url_col}")
    st.metric("Total Rows", len(urls))

    if st.button("🚀 Start Extraction", use_container_width=True):
        extractor = SwagelokExtractor()
        results = []

        progress = st.progress(0.0)
        status = st.empty()
        start = time.time()

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as exe:
            futures = [exe.submit(extractor.extract, u) for u in urls]

            for i, f in enumerate(as_completed(futures), 1):
                results.append(f.result())
                progress.progress(i / len(urls))
                status.write(f"Processing {i}/{len(urls)}")

        out_df = pd.DataFrame(results)

        buffer = BytesIO()
        out_df.to_excel(buffer, index=False)

        st.success("Extraction completed")
        st.download_button(
            "📥 Download Excel",
            buffer.getvalue(),
            "swagelok_unspsc_output.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )

        st.dataframe(out_df, use_container_width=True)
