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
for k in ["results", "running", "stats"]:
    if k not in st.session_state:
        st.session_state[k] = None if k != "running" else False

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
            "Confidence Score": 0,
            "_success": False
        }

        if not isinstance(url, str) or not url.startswith("http"):
            return row

        try:
            r = self.session.get(url, timeout=TIMEOUT)
            if r.status_code != 200 or not r.text:
                return row

            row["_success"] = True
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
st.set_page_config("Swagelok UNSPSC Intelligence Platform", "⏱️", layout="wide")

st.markdown("""
<style>
.header {background:linear-gradient(135deg,#0f4c81,#1fa2ff);
padding:2.2rem;border-radius:18px;color:white;text-align:center;margin-bottom:2rem}
.card {background:#f8fafc;padding:1.4rem;border-radius:14px;
border-left:6px solid #1fa2ff;margin-bottom:1.4rem}
.good {color:#1e8449;font-weight:700}
.bad {color:#c0392b;font-weight:700}
.small {font-size:0.9rem;color:#555}
</style>
""", unsafe_allow_html=True)

st.markdown("""
<div class="header">
<h1>⏱️ Swagelok UNSPSC Intelligence Platform</h1>
<p>Fast • Measured • Transparent Execution</p>
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

    st.markdown("### 📂 File Overview")
    st.markdown(f"""
    <div class="card">
    <b>Total rows:</b> {total}<br>
    <b>Valid URLs:</b> {valid_urls}<br>
    <span class="small">
    Only valid URLs are requested. Invalid rows are skipped automatically.
    </span>
    </div>
    """, unsafe_allow_html=True)

    if st.button("🚀 Start Extraction", use_container_width=True, disabled=st.session_state.running):
        st.session_state.running = True

        extractor = SwagelokExtractor()
        results = []

        start_time = time.time()

        with ThreadPoolExecutor(MAX_WORKERS) as exe:
            futures = [exe.submit(extractor.extract, u) for u in urls]
            for f in as_completed(futures):
                results.append(f.result())

        end_time = time.time()

        df_out = pd.DataFrame(results)

        # -------- STATS --------
        elapsed = round(end_time - start_time, 2)
        success = df_out["_success"].sum()
        part_ok = (df_out["Part"] != "Not Found").sum()
        unspsc_ok = (df_out["UNSPSC Code"] != "Not Found").sum()
        avg_time = round(elapsed / total, 3)
        throughput = round(total / elapsed, 2) if elapsed > 0 else 0

        st.session_state.results = df_out.drop(columns=["_success"])
        st.session_state.stats = {
            "elapsed": elapsed,
            "avg_time": avg_time,
            "throughput": throughput,
            "success": success,
            "part_ok": part_ok,
            "unspsc_ok": unspsc_ok,
            "total": total
        }
        st.session_state.running = False

# ================= RESULTS + TIME EXPLANATION =================
if st.session_state.results is not None:
    stats = st.session_state.stats

    st.markdown("### ⏱️ Execution Analysis")
    st.markdown(f"""
    <div class="card">
    <b>Total execution time:</b> {stats["elapsed"]} seconds<br>
    <b>Average time per URL:</b> {stats["avg_time"]} seconds<br>
    <b>Processing speed:</b> {stats["throughput"]} URLs / second<br><br>

    <b>Pages fetched successfully:</b> {stats["success"]} / {stats["total"]}<br>
    <b>Parts found:</b> {stats["part_ok"]}<br>
    <b>UNSPSC found:</b> {stats["unspsc_ok"]}<br>

    <p class="small">
    ⓘ Time depends mainly on Swagelok server response, not your file size.
    Parallel workers ({MAX_WORKERS}) are used to optimize speed safely.
    </p>
    </div>
    """, unsafe_allow_html=True)

    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        st.session_state.results.to_excel(writer, index=False, sheet_name="Results")
        pd.DataFrame.from_dict(stats, orient="index", columns=["Value"])\
            .to_excel(writer, sheet_name="Execution Stats")

    st.download_button(
        "📥 Download Excel (Results + Execution Stats)",
        buffer.getvalue(),
        "swagelok_unspsc_output.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True
    )

    st.dataframe(st.session_state.results, use_container_width=True)
