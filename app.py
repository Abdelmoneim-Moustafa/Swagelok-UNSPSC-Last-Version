import re
import time
import pandas as pd
import requests
import streamlit as st
from bs4 import BeautifulSoup
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed

# =========================================================
# CONFIG
# =========================================================
MAX_WORKERS = 16
REQUEST_TIMEOUT = 20
COMPANY_NAME = "Swagelok"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

# =========================================================
# SCRAPER CORE
# =========================================================
class SwagelokScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def scrape(self, url: str):
        """Scrape ONE product page"""
        result = {
            "Part": None,
            "Company": COMPANY_NAME,
            "URL": url,
            "UNSPSC Feature (Latest)": None,
            "UNSPSC Code": None,
        }

        if not isinstance(url, str) or not url.startswith("http"):
            return None

        try:
            r = self.session.get(url, timeout=REQUEST_TIMEOUT)
            if r.status_code != 200 or not r.text:
                return None

            soup = BeautifulSoup(r.text, "html.parser")

            part = self._extract_part(soup)
            unspsc_feature, unspsc_code = self._extract_latest_unspsc(soup)

            if not part or not unspsc_code:
                return None

            result["Part"] = part
            result["UNSPSC Feature (Latest)"] = unspsc_feature
            result["UNSPSC Code"] = unspsc_code

            return result

        except Exception:
            return None

    # -----------------------------------------------------
    # PART NUMBER (STRICT)
    # -----------------------------------------------------
    def _extract_part(self, soup):
        """
        ONLY accept full part from:
        Part #: CWS-C.040-.405-P
        """

        for txt in soup.find_all(string=re.compile(r"Part\s*#:", re.I)):
            block = txt.parent.get_text(" ", strip=True)
            match = re.search(r"Part\s*#:\s*([A-Z0-9.\-]+)", block)
            if match:
                return match.group(1).strip()

        return None

    # -----------------------------------------------------
    # UNSPSC (LATEST VERSION ONLY)
    # -----------------------------------------------------
    def _extract_latest_unspsc(self, soup):
        found = []

        for row in soup.select("tr"):
            cells = row.find_all("td")
            if len(cells) != 2:
                continue

            label = cells[0].get_text(strip=True)
            value = cells[1].get_text(strip=True)

            if not value.isdigit():
                continue

            version_match = re.search(r"UNSPSC\s*\(([\d.]+)\)", label)
            if version_match:
                version = self._parse_version(version_match.group(1))
                found.append((version, label, value))

        if not found:
            return None, None

        found.sort(key=lambda x: x[0], reverse=True)
        return found[0][1], found[0][2]

    @staticmethod
    def _parse_version(v):
        try:
            return tuple(int(x) for x in v.split("."))
        except Exception:
            return (0,)

# =========================================================
# STREAMLIT UI
# =========================================================
st.set_page_config(
    page_title="Swagelok UNSPSC Intelligence Platform",
    page_icon="🔎",
    layout="wide"
)

# ------------------ STYLE ------------------
st.markdown("""
<style>
.header {
    background: linear-gradient(135deg, #0f4c81, #1fa2ff);
    padding: 2.6rem;
    border-radius: 20px;
    color: white;
    text-align: center;
    margin-bottom: 2rem;
    box-shadow: 0 18px 40px rgba(0,0,0,.35);
}
.header h1 { font-size: 2.6rem; font-weight: 800; }
.header p { opacity: .95; }

.info {
    background: white;
    color: #1f2937;
    padding: 1.6rem 2rem;
    border-radius: 16px;
    border-left: 6px solid #1fa2ff;
    box-shadow: 0 12px 25px rgba(0,0,0,.2);
    margin-bottom: 2rem;
}
</style>
""", unsafe_allow_html=True)

# ------------------ HEADER ------------------
st.markdown("""
<div class="header">
    <h1>🔎 Swagelok UNSPSC Intelligence Platform</h1>
    <p>Page-Verified Parts • Latest UNSPSC • Zero Guessing 😎</p>
</div>
""", unsafe_allow_html=True)

# ------------------ INFO ------------------
st.markdown("""
<div class="info">
<b>Data rules (strict):</b><br><br>
• Part is extracted only from <b>“Part #:”</b> (full code, no truncation)<br>
• UNSPSC is taken only from <b>Specifications table</b><br>
• Highest UNSPSC version is selected automatically<br>
• One unique Part per row (duplicates removed)<br>
• Rows with missing or invalid data are excluded<br>
</div>
""", unsafe_allow_html=True)

# =========================================================
# FILE UPLOAD
# =========================================================
uploaded_file = st.file_uploader(
    "📤 Upload Excel file (URLs anywhere, any column)",
    type=["xlsx", "xls"]
)

if uploaded_file:
    df = pd.read_excel(uploaded_file)

    # ------------------ URL DETECTION ------------------
    urls = []
    for col in df.columns:
        urls.extend(df[col].dropna().astype(str).tolist())

    urls = list({u for u in urls if u.startswith("http")})

    st.success(f"🔗 {len(urls)} URLs detected")

    if st.button("🚀 Start Extraction", use_container_width=True):
        scraper = SwagelokScraper()
        results = []

        progress = st.progress(0.0)
        status = st.empty()
        start = time.time()

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(scraper.scrape, u) for u in urls]

            for i, future in enumerate(as_completed(futures), 1):
                res = future.result()
                if res:
                    results.append(res)

                progress.progress(i / len(urls))
                status.write(f"Processing {i}/{len(urls)}")

        elapsed = round(time.time() - start, 2)

        # ------------------ CLEAN OUTPUT ------------------
        out_df = pd.DataFrame(results)

        if not out_df.empty:
            out_df = out_df.drop_duplicates(subset=["Part"])
            out_df = out_df.dropna()

        st.success(
            f"✅ Finished in {elapsed} seconds | "
            f"{len(out_df)} valid unique parts"
        )

        # ------------------ DOWNLOAD ------------------
        buffer = BytesIO()
        out_df.to_excel(buffer, index=False)

        st.download_button(
            "📥 Download Excel",
            buffer.getvalue(),
            "swagelok_unspsc_output.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )

        st.dataframe(out_df, use_container_width=True)
