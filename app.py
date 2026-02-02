import re
import time
import pandas as pd
import requests
import streamlit as st
from bs4 import BeautifulSoup
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================= CONFIG =================
MAX_WORKERS = 12
TIMEOUT = 20
COMPANY_NAME = "Swagelok"

# ================= SESSION STATE =================
for key, default in {
    "run": False,
    "results": None,
    "stats": None
}.items():
    if key not in st.session_state:
        st.session_state[key] = default

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
            "UNSPSC Code": "Not Found"
        }

        if not isinstance(url, str) or not url.startswith("http"):
            return row, False

        try:
            r = self.session.get(url, timeout=TIMEOUT)
            if r.status_code != 200 or not r.text:
                return row, False

            soup = BeautifulSoup(r.text, "lxml")

            part = self._extract_part(soup, url)
            if part:
                row["Part"] = part

            feature, code = self._extract_unspsc(soup, r.text)
            if feature and code:
                row["UNSPSC Feature (Latest)"] = feature
                row["UNSPSC Code"] = code

            return row, True
        except Exception:
            return row, False

    def _extract_part(self, soup, url):
        for t in soup.find_all(string=re.compile(r"Part\s*#:?", re.I)):
            m = re.search(r"Part\s*#:\s*([A-Z0-9\-]+)", t.parent.get_text(" ", strip=True))
            if m:
                return m.group(1)

        for tr in soup.select("tr"):
            tds = tr.find_all("td")
            if len(tds) >= 2 and "part" in tds[0].get_text(strip=True).lower():
                val = tds[1].get_text(strip=True)
                if "-" in val:
                    return val

        m = re.search(r"part=([A-Z0-9\-]+)", url, re.I)
        return m.group(1) if m else None

    def _extract_unspsc(self, soup, html):
        found = []
        for tr in soup.select("tr"):
            tds = tr.find_all("td")
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
<div style="background:linear-gradient(135deg,#0f4c81,#1fa2ff);
padding:2rem;border-radius:18px;color:white;text-align:center;margin-bottom:2rem">
<h1>🔍 Swagelok UNSPSC Intelligence Platform</h1>
<p>Fast • Measured • Reliable Execution</p>
</div>
""", unsafe_allow_html=True)

uploaded_file = st.file_uploader(
    "📤 Upload Excel file (one column must contain Swagelok URLs)",
    type=["xlsx", "xls"]
)

# ================= PREP =================
urls = []
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
    st.info(f"Detected URL column: **{url_col}**")
    st.metric("Total Rows", len(urls))

# ================= RUN BUTTON =================
if urls:
    if st.button("🚀 Run Extraction", use_container_width=True):
        st.session_state.run = True

# ================= EXECUTION =================
if st.session_state.run:
    st.session_state.run = False  # reset immediately (CRITICAL)

    extractor = SwagelokExtractor()
    results = []
    success_count = 0

    start = time.time()
    progress = st.progress(0.0)

    with ThreadPoolExecutor(MAX_WORKERS) as exe:
        futures = [exe.submit(extractor.extract, u) for u in urls]
        for i, f in enumerate(as_completed(futures), 1):
            row, ok = f.result()
            results.append(row)
            success_count += int(ok)
            progress.progress(i / len(urls))

    elapsed = round(time.time() - start, 2)

    out_df = pd.DataFrame(results)

    st.session_state.results = out_df
    st.session_state.stats = {
        "Total Rows": len(urls),
        "Successful Pages": success_count,
        "Elapsed Time (sec)": elapsed,
        "Avg Time / URL (sec)": round(elapsed / len(urls), 3)
    }

# ================= RESULTS =================
if st.session_state.results is not None:
    st.success("✅ Extraction completed successfully")

    stats = st.session_state.stats
    st.markdown("### ⏱ Execution Summary")
    for k, v in stats.items():
        st.write(f"**{k}:** {v}")

    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        st.session_state.results.to_excel(writer, index=False, sheet_name="Results")
        pd.DataFrame.from_dict(stats, orient="index", columns=["Value"])\
            .to_excel(writer, sheet_name="Execution Stats")

    st.download_button(
        "📥 Download Excel",
        buffer.getvalue(),
        "swagelok_unspsc_output.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True
    )

    st.dataframe(st.session_state.results, use_container_width=True)
