import pandas as pd
import requests
from bs4 import BeautifulSoup
import streamlit as st
from io import BytesIO
import time
import re

# ---------- Constants (unchanged scraping logic) ----------
TIMEOUT = 20
COMPANY_NAME = "Swagelok"
CHECKPOINT_INTERVAL = 100

# ---------- Page config ----------
st.set_page_config(page_title="Swagelok UNSPSC Scraper — Clean UI", page_icon="🔍", layout="wide")

# ---------- Theme CSS (light & dark) ----------
LIGHT_CSS = """
:root{
  --bg:#ffffff;
  --card:#ffffff;
  --muted:#6b7280;
  --accent1:#667eea;
  --accent2:#764ba2;
  --success:#16a34a;
  --danger:#ef4444;
  --border:rgba(15,23,42,0.06);
}

/* Remove repeating background lines seen in screenshots */
.stApp, .main, .block-container {
  background: var(--bg) !important;
  background-image: none !important;
}

/* Clean header */
.header { padding: 28px; border-radius: 14px; color: white; text-align: left; }
.header h1{ margin:0; font-size:28px }
.header p{ margin:4px 0 0 0; opacity:0.95 }

.card { background:var(--card); padding:16px; border-radius:12px; box-shadow: 0 6px 18px rgba(17,24,39,0.06); border:1px solid var(--border); }
.metrics .stMetric{ padding: 8px 10px }

/* Progress log styling */
.progress-log{ max-height:320px; overflow:auto; padding:8px; border-radius:8px }
.progress-row{ padding:10px; border-radius:8px; margin-bottom:8px; background: linear-gradient(90deg, rgba(230,240,255,0.7), rgba(245,248,255,0.6)); }

/* Clean table: remove gridlines */
.stTable table, .stDataFrame table{ border-collapse:collapse !important }
.stTable th, .stTable td, .stDataFrame th, .stDataFrame td{ border:none !important; }

/* Buttons & download */
.stButton>button { padding:8px 14px }

"""

DARK_CSS = """
:root{
  --bg:#0b1220;
  --card:#071024;
  --muted:#9ca3af;
  --accent1:#7c3aed;
  --accent2:#06b6d4;
  --success:#34d399;
  --danger:#fb7185;
  --border:rgba(255,255,255,0.06);
}

.stApp, .main, .block-container { background: var(--bg) !important; background-image:none !important }
.header { padding: 28px; border-radius: 14px; color: white; text-align: left; }
.header h1{ margin:0; font-size:28px }
.header p{ margin:4px 0 0 0; opacity:0.9 }
.card { background:var(--card); padding:16px; border-radius:12px; box-shadow: 0 6px 18px rgba(2,6,23,0.6); border:1px solid var(--border); }
.metrics .stMetric{ padding: 8px 10px }
.progress-log{ max-height:320px; overflow:auto; padding:8px; border-radius:8px }
.progress-row{ padding:10px; border-radius:8px; margin-bottom:8px; background: linear-gradient(90deg, rgba(10,20,30,0.45), rgba(6,8,16,0.45)); }
.stTable table, .stDataFrame table{ border-collapse:collapse !important }
.stTable th, .stTable td, .stDataFrame th, .stDataFrame td{ border:none !important; }
"""

# Theme selection UI
with st.sidebar:
    st.markdown("## ⚙️ Settings")
    theme = st.radio("Theme:", ("Light", "Dark"), index=0)
    st.markdown("---")
    st.markdown("### Behavior")
    timeout_input = st.number_input("HTTP timeout (s)", min_value=5, max_value=120, value=TIMEOUT)
    checkpoint_interval = st.number_input("Checkpoint interval (rows)", min_value=10, max_value=1000, value=CHECKPOINT_INTERVAL)
    st.markdown("---")
    st.markdown("Made for end-users: clean, fast and clear.")

# Apply the selected theme CSS
st.markdown(f"<style>{LIGHT_CSS if theme=='Light' else DARK_CSS}</style>", unsafe_allow_html=True)

# ---------- Header ----------
col1, col2 = st.columns([3,1])
with col1:
    st.markdown(f"<div class='header' style='background: linear-gradient(135deg,{"#667eea"},{"#764ba2"});'>"
                f"<h1>🔍 Swagelok UNSPSC Extractor</h1>"
                f"<p>Upload product page URLs (Excel) and extract Part, UNSPSC feature & code.</p></div>", unsafe_allow_html=True)
with col2:
    st.markdown("<div style='text-align:right'><img src='https://raw.githubusercontent.com/Abdelmoneim-Moustafa/Swagelok-UNSPSC-Last-Version/main/logo.png' width='64' onerror='this.style.display=\"none\"'></div>", unsafe_allow_html=True)

st.markdown("<div style='display:flex;gap:12px;margin-top:12px'>", unsafe_allow_html=True)

# ---------- Upload area ----------
st.markdown("<div class='card'>", unsafe_allow_html=True)
mode = st.radio("Choose mode:", ("New upload", "Resume from checkpoint"))
file_label = "Upload Excel file (URLs only)" if mode == "New upload" else "Upload checkpoint Excel"
uploaded_file = st.file_uploader(file_label, type=["xlsx", "xls"])
st.markdown("</div>", unsafe_allow_html=True)

if uploaded_file:
    try:
        df = pd.read_excel(uploaded_file)
    except Exception as e:
        st.error(f"❌ Failed to read file: {e}")
        st.stop()

    # Detect URL column
    url_col = next((c for c in df.columns if df[c].astype(str).str.contains("http", na=False, case=False).any()), None)
    if not url_col:
        st.error("❌ No URL column found. Please provide an Excel with product-page URLs.")
        st.stop()
    st.success(f"✅ URL column detected: **{url_col}**")

    # Standardize and add columns (unchanged core data)
    df = df.rename(columns={url_col: "URL"})
    if "Company" not in df.columns:
        df["Company"] = COMPANY_NAME
    for col in ["Part", "UNSPSC Feature (Latest)", "UNSPSC Code", "Status", "Error"]:
        if col not in df.columns:
            df[col] = ""

    start_idx = 0
    if mode == "Resume from checkpoint":
        last_idx = df["Status"].last_valid_index()
        start_idx = last_idx + 1 if pd.notna(last_idx) else 0

    urls = df["URL"].astype(str).tolist()
    total = len(urls)
    valid_count = sum(1 for u in urls if u and u.lower().startswith("http"))

    # ---------- Top metrics ----------
    with st.container():
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Total URLs", total)
        m2.metric("Valid URLs", valid_count)
        est_time = int(valid_count * 0.25)
        m3.metric("Est. time (s)", est_time)
        m4.metric("Start index", start_idx)

    # ---------- Action buttons & progress area ----------
    start_button = st.button("🚀 Start Extraction")

    # reserved UI containers
    progress_box = st.container()
    log_box = st.container()
    checkpoint_box = st.container()

    if start_button:
        session = requests.Session()
        session.headers.update({"User-Agent": "Mozilla/5.0"})
        start_time = time.time()
        errors = []

        # show a compact progress bar and a scrollable log area
        prog = st.progress(0)
        with log_box:
            st.markdown("### Progress log")
            log_container = st.empty()

        for idx in range(start_idx, total):
            url = urls[idx]
            row_num = idx + 1
            prog.progress((idx+1)/total)

            # initialize result (kept same as your logic)
            row_result = {"Part": "Not Found", "UNSPSC Feature (Latest)": "Not Found", "UNSPSC Code": "Not Found", "Status": "Success", "Error": ""}

            if not url or not url.lower().startswith("http"):
                row_result["Status"] = "Invalid URL"
                row_result["Error"] = "Empty or invalid URL"
            else:
                try:
                    resp = session.get(url, timeout=int(timeout_input))
                    if resp.status_code != 200:
                        row_result["Status"] = f"HTTP {resp.status_code}"
                        row_result["Error"] = f"Status {resp.status_code}"
                    else:
                        soup = BeautifulSoup(resp.text, "html.parser")
                        html = resp.text
                        m = re.search(r'Part\s*#\s*:\s*([A-Z0-9.\-_/]+)', html, re.IGNORECASE)
                        if m:
                            row_result["Part"] = m.group(1).strip()
                        unspsc_entries = []
                        for tr in soup.find_all('tr'):
                            cells = tr.find_all('td')
                            if len(cells) >= 2:
                                attr = cells[0].text.strip()
                                val = cells[1].text.strip()
                                if attr.upper().startswith("UNSPSC") and re.match(r'^\d{6,8}$', val):
                                    unspsc_entries.append((attr, val))
                        if unspsc_entries:
                            # safe sort guard
                            def parse_ver(s):
                                found = re.search(r'\(([\d\.]+)\)', s)
                                if not found: return (0,)
                                return tuple(int(x) for x in found.group(1).split('.'))
                            unspsc_entries.sort(key=lambda x: parse_ver(x[0]), reverse=True)
                            row_result["UNSPSC Feature (Latest)"] = unspsc_entries[0][0]
                            row_result["UNSPSC Code"] = unspsc_entries[0][1]
                except Exception as e:
                    row_result["Status"] = "Error"
                    row_result["Error"] = str(e)[:120]

            # write results
            df.at[idx, "Part"] = row_result["Part"]
            df.at[idx, "UNSPSC Feature (Latest)"] = row_result["UNSPSC Feature (Latest)"]
            df.at[idx, "UNSPSC Code"] = row_result["UNSPSC Code"]
            df.at[idx, "Status"] = row_result["Status"]
            df.at[idx, "Error"] = row_result["Error"]

            # update a concise progress card
            elapsed = time.time() - start_time
            speed = (idx - start_idx + 1) / elapsed if elapsed > 0 else 0
            remaining = int((total - (idx+1)) / (speed or 1))
            with progress_box:
                st.markdown(
                    f"<div class='card'><div style='display:flex;justify-content:space-between;align-items:center'>"
                    f"<div><strong>Row {row_num}/{total}</strong><div style='color:var(--muted);font-size:13px'>Speed: {speed:.2f} rows/s | Remaining: {remaining}s</div></div>"
                    f"<div style='text-align:right'><strong>{row_result['Part']}</strong><div style='color:var(--muted);font-size:13px'>Code: {row_result['UNSPSC Code']} • Status: {row_result['Status']}</div></div>"
                    f"</div></div>", unsafe_allow_html=True)

            # append to the log (scrollable)
            with log_container:
                rows_html = []
                # grab last 15 rows to render (keeps DOM small)
                last = min(idx+1, 15)
                # we show a compact timestamp + part + status
                rows_html.append("<div class='progress-log'>")
                rows_html.append(f"<div class='progress-row'><strong>Row {row_num}</strong> — <em>{row_result['Part']}</em> — {row_result['Status']}</div>")
                if row_result['Error']:
                    rows_html.append(f"<div style='padding:10px;border-radius:8px;background:rgba(255,240,240,0.9);margin-bottom:8px'>{row_result['Error']}</div>")
                rows_html.append("</div>")
                st.markdown('\n'.join(rows_html), unsafe_allow_html=True)

            if row_result["Status"] != "Success":
                errors.append((row_num, row_result["Status"], row_result["Error"]))

            # checkpoint
            if ((row_num) % int(checkpoint_interval)) == 0 or (idx == total - 1):
                buf = BytesIO()
                with pd.ExcelWriter(buf, engine="openpyxl") as writer:
                    df.to_excel(writer, index=False)
                with checkpoint_box:
                    st.download_button(
                        label=f"💾 Checkpoint ({row_num})",
                        data=buf.getvalue(),
                        file_name=f"checkpoint_{row_num}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key=f"cp_{row_num}"
                    )

        # final summary
        total_processed = total - start_idx
        success_count = (df.loc[start_idx:, "Status"] == "Success").sum()
        parts_found = (df.loc[start_idx:, "Part"] != "Not Found").sum()
        unspsc_found = (df.loc[start_idx:, "UNSPSC Code"] != "Not Found").sum()
        run_time = int(time.time() - start_time)
        st.success(f"✅ Complete! Processed {total_processed} rows in {run_time//60}m {run_time%60}s — Success: {success_count}/{total_processed}")

        # final download (cleaned)
        final_df = df.drop(columns=["Status", "Error"]) if all(col in df.columns for col in ("Status","Error")) else df
        final_buf = BytesIO()
        with pd.ExcelWriter(final_buf, engine="openpyxl") as writer:
            final_df.to_excel(writer, index=False, sheet_name="Results")
        st.download_button("📥 Download Final Results", final_buf.getvalue(), file_name=f"swagelok_unspsc_results_{int(time.time())}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    # Notebook-style preview: small, clean table without grid lines
    st.markdown("### 📋 Sample of Results")
    st.table(df.head(8))

    # Quick analysis box
    with st.expander("📈 Quick Analysis"):
        st.write(f"- Rows: {len(df)} — Parts found: {(df['Part']!='Not Found').sum()} — UNSPSC found: {(df['UNSPSC Code']!='Not Found').sum()}")
        st.write("You can change the Theme or Checkpoint interval in the sidebar. The UI minimizes visual noise and provides a compact progress log for better readability.")

else:
    st.info("Upload an Excel file with product page URLs to get started.")

# ---------- Footer ----------
st.markdown("---")
st.markdown("Built for end-users • Clean UI • Light / Dark modes")
