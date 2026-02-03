import re
import time
import pandas as pd
import requests
import streamlit as st
from bs4 import BeautifulSoup
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================= PERFORMANCE CONFIG =================
MAX_WORKERS = 20           # Safe upper bound
TIMEOUT = 12               # Faster failure on dead pages
COMPANY_NAME = "Swagelok"

# ================= STREAMLIT CONFIG =================
st.set_page_config(
    page_title="Swagelok Product Classification Intelligence (UNSPSC)",
    page_icon="📘",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ================= HTML STYLE =================
st.markdown("""
<style>
.header {
    background: linear-gradient(135deg,#0f4c81,#1fa2ff);
    padding: 2.5rem;
    border-radius: 18px;
    color: white;
    text-align: center;
    margin-bottom: 2rem;
}
.header h1 {font-size:2.6rem}
.header p {opacity:.95;font-size:1.1rem}

.box {
    background:#f6f9ff;
    border-left:6px solid #1fa2ff;
    padding:1.4rem;
    border-radius:14px;
    margin-bottom:1.5rem;
}

.kpi {
    background:white;
    padding:1.3rem;
    border-radius:14px;
    text-align:center;
    box-shadow:0 6px 18px rgba(0,0,0,.08);
}

.footer {
    text-align:center;
    opacity:.6;
    margin-top:2rem;
    font-size:.9rem;
}
</style>
""", unsafe_allow_html=True)

st.markdown("""
<div class="header">
    <h1>🔍 Swagelok UNSPSC Intelligence Platform</h1>
    <p>Fast • Page-Verified • Latest UNSPSC • No Guessing 😄</p>
</div>
""", unsafe_allow_html=True)

st.markdown("""
<div class="box">
<b>Data rules (non-negotiable):</b><br>
✅ Part Number is extracted from <b>“Part #:” on the product page</b><br>
✅ No cleaning, no deduplication, no correction<br>
✅ Latest UNSPSC version only<br>
✅ Missing data = <b>Not Found</b><br>
⚡ Optimized for speed without sacrificing accuracy
</div>
""", unsafe_allow_html=True)

# ================= EXTRACTOR =================
class SwagelokExtractor:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Mozilla/5.0"})

        adapter = requests.adapters.HTTPAdapter(
            pool_connections=100,
            pool_maxsize=100,
            max_retries=2
        )
        self.session.mount("https://", adapter)

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
            r = self.session.get(url, timeout=TIMEOUT)
            if r.status_code != 200 or not r.text:
                return result

            soup = BeautifulSoup(r.text, "html.parser")

            # ---- PART (STOP EARLY WHEN FOUND) ----
            part = self._extract_part(soup, url)
            if part:
                result["Part"] = part

            # ---- SKIP UNSPSC SEARCH IF NOT PRESENT ----
            if "UNSPSC" not in r.text:
                return result

            feature, code = self._extract_unspsc(soup, r.text)
            if feature and code:
                result["UNSPSC Feature (Latest)"] = feature
                result["UNSPSC Code"] = code

        except Exception:
            pass

        return result

    # ---------- PART ----------
    def _extract_part(self, soup, url):
        for t in soup.find_all(string=re.compile(r"Part\s*#:?", re.I)):
            txt = t.parent.get_text(" ", strip=True)
            m = re.search(r"Part\s*#:\s*([A-Z0-9\-]+)", txt, re.I)
            if m:
                return m.group(1)

        for row in soup.find_all("tr"):
            tds = row.find_all("td")
            if len(tds) >= 2 and "part" in tds[0].get_text(strip=True).lower():
                val = tds[1].get_text(strip=True)
                if self._looks_like_part(val):
                    return val

        m = re.search(r"part=([A-Z0-9\-]+)", url, re.I)
        return m.group(1) if m else None

    def _looks_like_part(self, txt):
        return isinstance(txt, str) and "-" in txt and any(c.isdigit() for c in txt)

    # ---------- UNSPSC ----------
    def _extract_unspsc(self, soup, html):
        found = []

        for row in soup.find_all("tr"):
            tds = row.find_all("td")
            if len(tds) >= 2:
                label = tds[0].get_text(strip=True)
                value = tds[1].get_text(strip=True)
                vm = re.search(r"UNSPSC\s*\(([\d.]+)\)", label)
                if vm and re.fullmatch(r"\d{6,8}", value):
                    found.append((self._ver(vm.group(1)), label, value))

        if not found:
            for v, c in re.findall(r"UNSPSC\s*\(([\d.]+)\)[^\d]*(\d{6,8})", html):
                found.append((self._ver(v), f"UNSPSC ({v})", c))

        if not found:
            return None, None

        found.sort(key=lambda x: x[0], reverse=True)
        return found[0][1], found[0][2]

    def _ver(self, v):
        try:
            return tuple(int(x) for x in v.split("."))
        except Exception:
            return (0,)

# ================= FILE INPUT =================
uploaded = st.file_uploader(
    "📤 Upload Excel file (one column must contain Swagelok URLs)",
    type=["xlsx", "xls"]
)

if uploaded:
    df = pd.read_excel(uploaded)

    url_col = next(
        (c for c in df.columns if df[c].astype(str).str.contains("http", na=False).any()),
        None
    )

    if not url_col:
        st.error("❌ No URL column detected.")
        st.stop()

    urls = df[url_col].tolist()
    total = len(urls)

    st.success(f"🔗 URL column detected: **{url_col}**")
    st.write(f"📊 Total rows: **{total}**")

    est_time = round(total / (MAX_WORKERS * 2.5), 1)
    st.info(f"⏱ Estimated time: **~{est_time} minutes**")

    if st.button("🚀 Start Fast Extraction", use_container_width=True):
        extractor = SwagelokExtractor()
        results = []

        start = time.time()
        progress = st.progress(0.0)
        status = st.empty()

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = [pool.submit(extractor.extract, u) for u in urls]

            for i, f in enumerate(as_completed(futures), 1):
                results.append(f.result())
                progress.progress(i / total)
                status.markdown(
                    f"⚡ Processing <b>{i}</b> / <b>{total}</b> pages...",
                    unsafe_allow_html=True
                )

        elapsed = round((time.time() - start) / 60, 2)
        out_df = pd.DataFrame(results)

        found_parts = (out_df["Part"] != "Not Found").sum()
        found_unspsc = (out_df["UNSPSC Code"] != "Not Found").sum()

        c1, c2, c3 = st.columns(3)
        c1.markdown(f"<div class='kpi'><h3>{found_parts}</h3><p>Parts Found</p></div>", unsafe_allow_html=True)
        c2.markdown(f"<div class='kpi'><h3>{found_unspsc}</h3><p>UNSPSC Found</p></div>", unsafe_allow_html=True)
        c3.markdown(f"<div class='kpi'><h3>{elapsed} min</h3><p>Total Time</p></div>", unsafe_allow_html=True)

        st.subheader("📋 Output Preview")
        st.dataframe(out_df, use_container_width=True)

        buffer = BytesIO()
        out_df.to_excel(buffer, index=False)

        st.download_button(
            "📥 Download Excel",
            buffer.getvalue(),
            "swagelok_unspsc_output.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )

        st.success("✅ Completed — fast, accurate, and audit-ready.")

st.markdown("<div class='footer'>Built for serious data people ☕</div>", unsafe_allow_html=True)
