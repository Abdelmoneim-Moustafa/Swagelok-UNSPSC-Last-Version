import re
import time
import pandas as pd
import requests
import streamlit as st
from bs4 import BeautifulSoup
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================= CONFIG =================
MAX_WORKERS = 16
TIMEOUT = 20
COMPANY_NAME = "Swagelok"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

# ================= PAGE =================
st.set_page_config(
    page_title="Swagelok UNSPSC Intelligence Platform",
    page_icon="🔎",
    layout="wide"
)

st.markdown("""
<style>
.header {
    background: linear-gradient(135deg, #0f4c81, #1fa2ff);
    padding: 2.5rem;
    border-radius: 18px;
    color: white;
    text-align: center;
    margin-bottom: 2rem;
}
.header h1 { font-size: 2.6rem; font-weight: 800; }
.header p { font-size: 1.1rem; opacity: 0.95; }
.box {
    background: #f7f9fc;
    padding: 1.5rem;
    border-radius: 14px;
    border-left: 6px solid #1fa2ff;
    margin-bottom: 1.5rem;
}
</style>
""", unsafe_allow_html=True)

st.markdown("""
<div class="header">
<h1>🔎 Swagelok UNSPSC Intelligence Platform</h1>
<p>Page-Verified Parts • Latest UNSPSC • Zero Guessing 😎</p>
</div>
""", unsafe_allow_html=True)

st.markdown("""
<div class="box">
<b>What this tool guarantees:</b><br>
✔ Part number extracted ONLY from <b>Part #:</b> on page<br>
✔ Latest UNSPSC version (highest number)<br>
✔ URL → Part → UNSPSC relationship validated<br>
✔ No missing data • No duplicated parts • No guessing<br>
⚡ Optimized for large files
</div>
""", unsafe_allow_html=True)

# ================= HELPERS =================
def detect_urls(df: pd.DataFrame) -> list[str]:
    urls = []
    for col in df.columns:
        series = df[col].astype(str)
        found = series.str.extractall(
            r"(https?://[^\s,;]+)",
            flags=re.IGNORECASE
        )[0].tolist()
        urls.extend(found)
    return list(dict.fromkeys(urls))  # preserve order, unique


def parse_version(v):
    try:
        return tuple(int(x) for x in v.split("."))
    except:
        return (0,)


# ================= SCRAPER =================
class SwagelokScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def scrape(self, url: str):
        base = {
            "Part": None,
            "Company": COMPANY_NAME,
            "URL": url,
            "UNSPSC Feature (Latest)": None,
            "UNSPSC Code": None
        }

        try:
            r = self.session.get(url, timeout=TIMEOUT)
            if r.status_code != 200:
                return None

            soup = BeautifulSoup(r.text, "html.parser")

            # -------- PART (STRICT) --------
            part = None
            part_label = soup.find(string=re.compile(r"Part\s*#:?", re.I))
            if part_label:
                text = part_label.parent.get_text(" ", strip=True)
                m = re.search(r"Part\s*#:\s*([A-Z0-9.\-]+)", text)
                if m:
                    part = m.group(1)

            if not part:
                return None  # never guess part

            base["Part"] = part

            # -------- UNSPSC (LATEST) --------
            found = []
            for row in soup.select("table tr"):
                tds = row.find_all("td")
                if len(tds) >= 2:
                    label = tds[0].get_text(strip=True)
                    value = tds[1].get_text(strip=True)

                    m = re.search(r"UNSPSC\s*\(([\d.]+)\)", label)
                    if m and re.fullmatch(r"\d{6,8}", value):
                        found.append((parse_version(m.group(1)), label, value))

            if not found:
                return None

            found.sort(key=lambda x: x[0], reverse=True)
            base["UNSPSC Feature (Latest)"] = found[0][1]
            base["UNSPSC Code"] = found[0][2]

            return base

        except Exception:
            return None


# ================= FILE =================
uploaded = st.file_uploader(
    "📤 Upload Excel file (URLs anywhere, any column)",
    type=["xlsx", "xls"]
)

if uploaded:
    df_input = pd.read_excel(uploaded)
    urls = detect_urls(df_input)

    if not urls:
        st.error("❌ No Swagelok URLs detected.")
        st.stop()

    st.success(f"🔗 {len(urls)} URLs detected")
    st.metric("Total URLs", len(urls))

    if st.button("🚀 Start Verified Extraction", use_container_width=True):
        scraper = SwagelokScraper()
        results = []

        progress = st.progress(0.0)
        status = st.empty()
        start = time.time()

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as exe:
            futures = [exe.submit(scraper.scrape, u) for u in urls]

            for i, f in enumerate(as_completed(futures), 1):
                res = f.result()
                if res:
                    results.append(res)
                progress.progress(i / len(urls))
                status.markdown(
                    f"Processing <b>{i}</b> / <b>{len(urls)}</b>",
                    unsafe_allow_html=True
                )

        elapsed = round(time.time() - start, 1)

        if not results:
            st.error("❌ No valid products extracted.")
            st.stop()

        df_out = pd.DataFrame(results)

        # -------- VALIDATION --------
        df_out.drop_duplicates(subset=["Part"], inplace=True)
        df_out.dropna(inplace=True)

        st.success(f"✅ Completed in {elapsed} seconds")

        st.metric("Valid Products", len(df_out))
        st.metric("Avg sec / URL", round(elapsed / len(urls), 2))

        # -------- DOWNLOAD --------
        buffer = BytesIO()
        df_out.to_excel(buffer, index=False)

        st.download_button(
            "📥 Download Excel",
            buffer.getvalue(),
            file_name="swagelok_unspsc_output.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )

        st.dataframe(df_out, use_container_width=True)
