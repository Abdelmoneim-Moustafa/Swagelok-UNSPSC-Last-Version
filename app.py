# app.py
"""
Updated Streamlit UI for Swagelok UNSPSC Scraper
- Keeps original scraping & checkpoint behavior
- Removes per-row UI clutter and replaces with single progress spinner + bar
- Centers the Start button and adds a user-friendly description
- Dark/light friendly styling
- Small performance improvement: throttle UI updates to every few rows
"""

import re
import time
from io import BytesIO

import pandas as pd
import requests
from bs4 import BeautifulSoup
import streamlit as st

# ---------- CONFIG ----------
TIMEOUT = 20  # seconds for HTTP requests
COMPANY_NAME = "Swagelok"
CHECKPOINT_INTERVAL = 100  # save checkpoint every 100 URLs
UI_UPDATE_EVERY = 5  # update progress UI only every N rows to reduce overhead

# ---------- PAGE + STYLES ----------
st.set_page_config(page_title="Swagelok UNSPSC Scraper", page_icon="🔍", layout="wide")

# CSS that respects streamlit light/dark mode variables where possible
st.markdown(
    """
    <style>
    .main-header{
      background:linear-gradient(135deg,#667eea,#764ba2);
      padding:1.8rem;border-radius:12px;color:white;text-align:center;margin-bottom:1.2rem;
      box-shadow:0 8px 20px rgba(102,126,234,0.12)}
    .info-box{
      background:var(--card-background,#f5f7fb);padding:1rem;border-radius:10px;margin:0.8rem 0;
      border-left:4px solid rgba(102,126,234,0.9)}
    .center-button {display:flex; justify-content:center; margin-top:0.6rem; margin-bottom:0.6rem;}
    .summary-box{padding:1rem;border-radius:8px;margin-top:1rem;}
    .small-muted{font-size:0.9rem;color:var(--subtle-text,#6b7280)}
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown('<div class="main-header"><h1>🔍 Swagelok UNSPSC Scraper</h1>'
            '<div style="opacity:0.95">Extract Part and latest UNSPSC (feature & code) from Swagelok product pages</div></div>',
            unsafe_allow_html=True)

st.markdown(
    '<div class="info-box">'
    '<strong>What this tool does:</strong> Upload an Excel file containing product page URLs (one column). '
    'The app will fetch each page, extract the correct <strong>Part</strong> and the <strong>latest UNSPSC</strong> '
    '(feature and numeric code) and return a simple Excel containing only: '
    '<em>URL, Part, UNSPSC Feature (Latest), UNSPSC Code, Company</em>. Checkpoints are saved so you can resume if interrupted.'
    '</div>',
    unsafe_allow_html=True,
)

# ---------- MODE / UPLOAD ----------
st.markdown("### Input")
mode = st.radio("Mode", ("New upload", "Resume from checkpoint"), horizontal=True)
file_label = "Upload Excel file (URLs only)" if mode == "New upload" else "Upload checkpoint Excel (to resume)"
uploaded_file = st.file_uploader(file_label, type=["xlsx", "xls"])

if not uploaded_file:
    st.info("Please upload an Excel file to begin. If resuming a previous run, upload the checkpoint file you downloaded earlier.")
    st.stop()

# ---------- READ & NORMALIZE INPUT ----------
try:
    df = pd.read_excel(uploaded_file)
except Exception as e:
    st.error(f"Failed to read uploaded file: {e}")
    st.stop()

# find URL column (prefer 'URL' if present)
url_col = None
if "URL" in df.columns:
    url_col = "URL"
else:
    for c in df.columns:
        try:
            if df[c].astype(str).str.contains("http", na=False, case=False).any():
                url_col = c
                break
        except Exception:
            continue

if url_col is None:
    st.error("No URL column found. Please upload a file with a URL column.")
    st.stop()

# rename to standard name
df = df.rename(columns={url_col: "URL"})

# ensure output columns exist
for c in ["Part", "UNSPSC Feature (Latest)", "UNSPSC Code", "Status", "Error"]:
    if c not in df.columns:
        df[c] = ""

# company column
if "Company" not in df.columns:
    df["Company"] = COMPANY_NAME

# ensure URL column is string and stripped
df["URL"] = df["URL"].astype(str).str.strip()

# compute start index for resume mode: first row missing Part & UNSPSC Code
def next_index_to_process(df_local: pd.DataFrame) -> int:
    for i, row in df_local.iterrows():
        if not str(row.get("Part", "")).strip() and not str(row.get("UNSPSC Code", "")).strip():
            return int(i)
    return len(df_local)

start_idx = next_index_to_process(df) if mode == "Resume from checkpoint" else 0

# show summary metrics
total = len(df)
valid_count = sum(1 for u in df["URL"] if u and u.lower().startswith("http"))

c1, c2, c3 = st.columns(3)
c1.metric("📄 Rows in file", total)
c2.metric("🔗 Valid URLs", valid_count)
c3.metric("▶ Start from row", start_idx + 1 if start_idx < total else "Done")

st.markdown("---")

# ---------- Centered Start Button ----------
col_l, col_c, col_r = st.columns([1, 2, 1])
with col_c:
    start_button = st.button("Start Extraction", type="primary")

# placeholders
progress_ph = st.empty()
summary_ph = st.empty()
checkpoint_ph = st.empty()
preview_ph = st.empty()
error_log = []

# helper for checkpoint bytes
def df_to_excel_bytes(df_out: pd.DataFrame) -> bytes:
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df_out.to_excel(writer, index=False, sheet_name="Results")
    return buf.getvalue()

# regex helpers
_part_rx = re.compile(r'Part\s*#\s*[:\-]?\s*([A-Z0-9.\-_/]+)', re.IGNORECASE)
_unspsc_rx = re.compile(r'UNSPSC\s*\(([\d\.]+)\)[^\d]*?(\d{6,8})', re.IGNORECASE)

# parsing helpers (kept simple and robust)
def extract_part_from_html(html_text: str, soup: BeautifulSoup, url: str) -> str:
    # try regex on full html
    m = _part_rx.search(html_text)
    if m:
        return m.group(1).strip()
    # try searching visible text for "Part #:"
    text = soup.get_text(" ", strip=True)
    m = _part_rx.search(text)
    if m:
        return m.group(1).strip()
    # fallback: try url param 'part'
    try:
        from urllib.parse import urlparse, parse_qs
        qs = parse_qs(urlparse(url).query)
        if "part" in qs and qs["part"]:
            return qs["part"][0]
    except Exception:
        pass
    return ""

def extract_latest_unspsc(soup: BeautifulSoup, html_text: str) -> (str, str):
    found = []
    # look in table rows first
    for tr in soup.find_all("tr"):
        tds = tr.find_all(["td", "th"])
        if len(tds) >= 2:
            key = tds[0].get_text(" ", strip=True)
            val = tds[1].get_text(" ", strip=True)
            if "UNSPSC" in key.upper():
                m = re.search(r'UNSPSC\s*\(([\d\.]+)\)', key, re.IGNORECASE)
                code_m = re.search(r'(\d{6,8})', val)
                if m and code_m:
                    ver = tuple(int(p) for p in m.group(1).split("."))
                    found.append((ver, f"UNSPSC ({m.group(1)})", code_m.group(1)))
    # fallback: search whole HTML
    for m in _unspsc_rx.finditer(html_text):
        ver_str, code = m.group(1), m.group(2)
        ver = tuple(int(p) for p in ver_str.split("."))
        found.append((ver, f"UNSPSC ({ver_str})", code))
    if not found:
        return "", ""
    found.sort(key=lambda x: x[0], reverse=True)
    return found[0][1], found[0][2]

# main processing (keep synchronous requests.Session for reliability; small UI improvement: throttle updates)
if start_button:
    if start_idx >= total:
        st.success("Nothing to process — all rows appear completed.")
    else:
        session = requests.Session()
        session.headers.update({"User-Agent": "Mozilla/5.0"})
        start_time = time.time()
        processed = 0
        progress_bar = progress_ph.progress(0)
        # We'll collect errors to show after finishing (to avoid per-row UI lines)
        error_log = []

        # iterate rows
        for idx in range(start_idx, total):
            url = df.at[idx, "URL"]
            # default placeholders
            part_val = df.at[idx, "Part"] if pd.notna(df.at[idx, "Part"]) else ""
            feat_val = df.at[idx, "UNSPSC Feature (Latest)"] if pd.notna(df.at[idx, "UNSPSC Feature (Latest)"]) else ""
            code_val = df.at[idx, "UNSPSC Code"] if pd.notna(df.at[idx, "UNSPSC Code"]) else ""

            if not url or not url.lower().startswith("http"):
                df.at[idx, "Status"] = "Invalid URL"
                df.at[idx, "Error"] = "Empty or invalid URL"
            else:
                try:
                    resp = session.get(url, timeout=TIMEOUT)
                    if resp.status_code != 200:
                        df.at[idx, "Status"] = f"HTTP {resp.status_code}"
                        df.at[idx, "Error"] = f"Status {resp.status_code}"
                    else:
                        html = resp.text
                        soup = BeautifulSoup(html, "html.parser")

                        # extract part
                        part = extract_part_from_html(html, soup, url)
                        if part:
                            df.at[idx, "Part"] = part
                        # extract unspsc
                        feat, code = extract_latest_unspsc(soup, html)
                        if feat:
                            df.at[idx, "UNSPSC Feature (Latest)"] = feat
                        if code:
                            df.at[idx, "UNSPSC Code"] = code
                        df.at[idx, "Status"] = "Success"
                        df.at[idx, "Error"] = ""
                except Exception as e:
                    df.at[idx, "Status"] = "Error"
                    df.at[idx, "Error"] = str(e)[:200]
                    error_log.append(f"Row {idx+1}: {str(e)[:200]}")

            processed += 1
            # throttle UI updates to reduce overhead
            if processed % UI_UPDATE_EVERY == 0 or idx == total - 1:
                completed = idx - start_idx + 1
                pct = int(completed * 100 / (total - start_idx))
                progress_bar.progress(pct)
                elapsed = int(time.time() - start_time)
                progress_ph.text(f"Processing: {completed} / {total - start_idx} rows (elapsed {elapsed}s)")

            # checkpoint when needed
            if ((idx + 1) % CHECKPOINT_INTERVAL) == 0 or idx == total - 1:
                # create checkpoint bytes and update the single checkpoint button placeholder (no clutter)
                cp_bytes = df_to_excel_bytes(df)
                checkpoint_ph.download_button(
                    label=f"💾 Download Checkpoint (up to row {idx+1})",
                    data=cp_bytes,
                    file_name=f"checkpoint_{idx+1}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key=f"checkpoint_{idx+1}"
                )

        # done processing: clear progress area for clean final UI
        progress_ph.empty()

        # Summary
        total_processed = total - start_idx
        success_count = (df.loc[start_idx:, "Status"] == "Success").sum()
        parts_found = (df.loc[start_idx:, "Part"].astype(bool)).sum()
        unspsc_found = (df.loc[start_idx:, "UNSPSC Code"].astype(bool)).sum()
        run_time = int(time.time() - start_time)

        summary_ph.markdown(
            f'<div class="summary-box">'
            f'<h3>✅ Complete</h3>'
            f'<p class="small-muted"><strong>Processed:</strong> {total_processed} rows in {run_time}s</p>'
            f'<p class="small-muted"><strong>Success:</strong> {int(success_count)} | '
            f'<strong>Parts found:</strong> {int(parts_found)} | '
            f'<strong>UNSPSC found:</strong> {int(unspsc_found)}</p>'
            f'</div>',
            unsafe_allow_html=True
        )

        # results preview and final download (only required columns)
        preview_ph.markdown("### 📋 Results (preview)")
        preview_df = df[["URL", "Part", "UNSPSC Feature (Latest)", "UNSPSC Code", "Company"]].copy()
        st.dataframe(preview_df.head(50), use_container_width=True)

        final_bytes = df_to_excel_bytes(preview_df)
        st.download_button(
            "📥 Download Final Results (Excel)",
            data=final_bytes,
            file_name=f"swagelok_unspsc_results_{int(time.time())}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="final_dl"
        )

        # show error log only if there are errors
        if error_log:
            with st.expander(f"⚠️ Error log ({len(error_log)}) — expand to view"):
                for line in error_log:
                    st.text(line)

        st.success("You can now download the checkpoint or final results. If more rows remain, re-upload the checkpoint to resume.")
