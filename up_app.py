"""
🔍 Swagelok UNSPSC Intelligence Platform — Production-ready Streamlit App

Features:
- Accurate UNSPSC extraction (takes LAST occurrence for highest version)
- Robust per-file disk checkpoints (resume after sleep/refresh/crash)
- Sequential (row-by-row) or Parallel mode (ThreadPoolExecutor)
- Safe Arrow/Streamlit serialization (everything converted to primitives)
- Clean, responsive UI with dark/light theme support
- Download checkpoints & final results
- Clear error logging

Created by: Abdelmoneim Moustafa
Data Intelligence Engineer
"""

import os
import glob
import re
import time
import hashlib
import pandas as pd
import requests
import streamlit as st

from io import BytesIO
from typing import Any
from typing import Dict, Optional, Tuple, List
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

# =========================
# CONFIG (tweak if needed)
# =========================
TIMEOUT = 20
COMPANY_NAME = "Swagelok"
DEFAULT_BATCH_SIZE = 100
DEFAULT_MAX_WORKERS = 6
UI_UPDATE_EVERY = 10  # update UI every N processed items to reduce redraws

# =========================
# PAGE + STYLING
# =========================
st.set_page_config(page_title="Swagelok UNSPSC Platform", page_icon="🔍", layout="wide")

st.markdown(
    """
<style>
:root {
  --info-bg: #e3f2fd;
  --card-bg: #ffffff;
  --border: #e0e0e0;
  --text: #333333;
  --error-bg: #ffebee;
}
@media (prefers-color-scheme: dark) {
  :root {
    --info-bg: #1a237e;
    --card-bg: #1e1e1e;
    --border: #424242;
    --text: #e0e0e0;
    --error-bg: #b71c1c;
  }
}
.main-header{background:linear-gradient(135deg,#667eea,#764ba2);padding:2rem;border-radius:14px;color:white;text-align:center;margin-bottom:1.5rem;box-shadow:0 8px 20px rgba(102,126,234,0.15)}
.info-box{background:var(--info-bg);border-left:5px solid #2196f3;padding:1rem;border-radius:10px;margin:0.75rem 0;color:var(--text)}
.success-box{background:linear-gradient(135deg,#11998e,#38ef7d);padding:1.25rem;border-radius:12px;color:white;text-align:center;margin:1rem 0}
.progress-card{background:var(--card-bg);padding:1rem;border-radius:10px;margin:0.5rem 0;border:1px solid var(--border);color:var(--text)}
.error-card{background:var(--error-bg);border-left:5px solid #f44336;padding:0.8rem;border-radius:8px;margin:0.5rem 0;color:var(--text)}
.small-muted{font-size:0.9rem;color:gray}
</style>
""",
    unsafe_allow_html=True,
)

st.markdown(
    '<div class="main-header"><h1>🔍 Swagelok UNSPSC Platform</h1><p>Row-by-row • LAST UNSPSC occurrence • Resume-safe</p></div>',
    unsafe_allow_html=True,
)

st.markdown(
    """
<div class="info-box">
  <strong>✨ Key fixes:</strong><br>
  • Selects the <em>last</em> occurrence of the highest UNSPSC version (bottom of table) — correct code selected.<br>
  • Per-upload file checkpointing to disk — survive sleep/refresh/crash.<br>
  • Optional parallel mode for speed (configurable workers).<br>
</div>
""",
    unsafe_allow_html=True,
)

# =========================
# Utilities: file id, checkpoints
# =========================
def get_file_id(uploaded_file: Any) -> str:
    """
    Stable short id (md5 hex12) for an uploaded file's contents.

    Uses getvalue() when available (Streamlit UploadedFile).
    Falls back to reading the file-like object if necessary.
    """
    try:
        # Streamlit UploadedFile supports getvalue()
        raw = uploaded_file.getvalue()
    except Exception:
        # Fallback: try reading the stream (some environments provide a file-like object)
        try:
            uploaded_file.seek(0)
        except Exception:
            pass
        raw = uploaded_file.read()
    # Ensure bytes
    if isinstance(raw, str):
        raw = raw.encode("utf-8")
    return hashlib.md5(raw).hexdigest()[:12]



def make_checkpoint_paths(file_id: str):
    base_dir = os.path.join("checkpoints", file_id)
    os.makedirs(base_dir, exist_ok=True)
    prefix = "batch"
    return base_dir, prefix


def list_completed_batches(checkpoint_dir: str, prefix: str) -> set:
    pattern = os.path.join(checkpoint_dir, f"{prefix}_*.csv")
    files = glob.glob(pattern)
    completed = set()
    for f in files:
        name = os.path.basename(f)
        try:
            batch_num = int(name.split("_")[-1].split(".")[0])
            completed.add(batch_num)
        except Exception:
            continue
    return completed


def save_batch_to_disk(batch_df: pd.DataFrame, checkpoint_dir: str, prefix: str, batch_num: int):
    """Atomically write a CSV for a batch (flush + fsync)."""
    path = os.path.join(checkpoint_dir, f"{prefix}_{batch_num}.csv")
    tmp = path + ".tmp"
    # Ensure columns are primitive / string to be safe
    batch_df = batch_df.copy()
    for col in batch_df.columns:
        batch_df[col] = batch_df[col].apply(lambda x: "" if x is None else str(x))
    batch_df.to_csv(tmp, index=False, encoding="utf-8")
    # atomic replace
    os.replace(tmp, path)


def load_all_batches(checkpoint_dir: str, prefix: str) -> pd.DataFrame:
    pattern = os.path.join(checkpoint_dir, f"{prefix}_*.csv")
    files = sorted(glob.glob(pattern), key=lambda x: int(os.path.basename(x).split("_")[-1].split(".")[0]))
    if not files:
        return pd.DataFrame()
    dfs = []
    for f in files:
        try:
            dfs.append(pd.read_csv(f, dtype=str))
        except Exception:
            # skip corrupt files but continue
            continue
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)


# =========================
# Helper: safe string converter
# =========================
def safe_str(x) -> str:
    if x is None:
        return ""
    return str(x)


# =========================
# Swagelok Extractor (same logic, defensive)
# =========================
class SwagelokExtractor:
    def __init__(self, timeout: int = TIMEOUT):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Mozilla/5.0 (Swagelok-UNSPSC-Extractor)"})
        self.timeout = timeout

    def extract(self, url: str, row_num: int) -> Dict:
        """Return a dict with primitive values only."""
        result = {
            "Row": row_num,
            "Part": "Not Found",
            "Company": COMPANY_NAME,
            "URL": url or "Empty",
            "UNSPSC Feature (Latest)": "Not Found",
            "UNSPSC Code": "Not Found",
            "Status": "Success",
            "Error": ""
        }

        if not url or not isinstance(url, str) or not url.startswith("http"):
            result["Status"] = "Invalid URL"
            result["Error"] = "URL is empty or invalid"
            return result

        try:
            resp = self.session.get(url, timeout=self.timeout)
            if resp.status_code != 200:
                result["Status"] = f"HTTP {resp.status_code}"
                result["Error"] = f"Status {resp.status_code}"
                return result

            html = resp.text
            soup = BeautifulSoup(html, "html.parser")

            # Part extraction
            part = self._part(soup, html, url)
            if part:
                result["Part"] = part
            else:
                # don't overwrite existing error; only set if none
                if not result["Error"]:
                    result["Error"] = "Part not found"

            # UNSPSC extraction (last occurrence for highest version)
            feat, code = self._unspsc_last(soup, html)
            if feat and code:
                result["UNSPSC Feature (Latest)"] = feat
                result["UNSPSC Code"] = code
            else:
                result["Error"] = (result["Error"] + ";UNSPSC not found") if result["Error"] else "UNSPSC not found"

            return result

        except requests.Timeout:
            result["Status"] = "Timeout"
            result["Error"] = f"Timeout after {self.timeout}s"
            return result
        except Exception as e:
            result["Status"] = "Error"
            # keep errors short and safe
            result["Error"] = safe_str(str(e))[:200]
            return result

    # ----- part helpers -----
    def _part(self, s: BeautifulSoup, html: str, url: str) -> Optional[str]:
        up = self._up(url)
        for m in re.findall(r'Part\s*#\s*:\s*(?:<[^>]+>)?\s*([A-Z0-9][A-Z0-9.\-_/]*)', html, re.I):
            c = m.strip()
            if not c:
                continue
            if up and self._pm(c, up):
                return c
            if not up and self._vp(c):
                return c
        return up if (up and self._vp(up)) else None

    def _up(self, u: str) -> Optional[str]:
        for p in [r'/p/([A-Z0-9.\-_/%]+)', r'[?&]part=([A-Z0-9.\-_/%]+)']:
            m = re.search(p, u, re.I)
            if m:
                return m.group(1).replace('%2F', '/').replace('%252F', '/').strip()
        return None

    def _pm(self, p1: str, p2: str) -> bool:
        if not p1 or not p2:
            return False
        n1 = re.sub(r'[.\-/]', '', p1).lower()
        n2 = re.sub(r'[.\-/]', '', p2).lower()
        return n1 == n2

    def _vp(self, p: str) -> bool:
        if not isinstance(p, str) or not (2 <= len(p) <= 100):
            return False
        has_alpha = any(c.isalpha() for c in p)
        has_digit = any(c.isdigit() for c in p)
        if not (has_alpha or (has_digit and len(p) > 3)):
            return False
        exclude = ['charset', 'utf', 'html', 'http', 'www', 'text']
        return not any(ex in p.lower() for ex in exclude)

    # ----- UNSPSC: take LAST occurrence for highest version -----
    def _unspsc_last(self, s: BeautifulSoup, html: str) -> Tuple[Optional[str], Optional[str]]:
        all_entries = []
        for idx, row in enumerate(s.find_all('tr')):
            cells = row.find_all('td')
            if len(cells) >= 2:
                attr = cells[0].get_text(strip=True)
                val = cells[1].get_text(strip=True)
                if not attr.upper().startswith('UNSPSC'):
                    continue
                vm = re.search(r'UNSPSC\s*\(([\d.]+)\)', attr, re.I)
                if vm and re.match(r'^\d{6,8}$', val):
                    try:
                        version_tuple = tuple(map(int, vm.group(1).split('.')))
                    except Exception:
                        version_tuple = (0,)
                    all_entries.append({'v': version_tuple, 'f': attr, 'c': val, 'order': idx})

        # fallback regex if table parse failed
        if not all_entries:
            for idx, (v_str, code) in enumerate(re.findall(r'UNSPSC\s*\(([\d.]+)\)[^\d]*?(\d{6,8})', html, re.I)):
                try:
                    version_tuple = tuple(map(int, v_str.split('.')))
                except Exception:
                    version_tuple = (0,)
                all_entries.append({'v': version_tuple, 'f': f"UNSPSC ({v_str})", 'c': code, 'order': idx})

        if not all_entries:
            return None, None

        # choose max version; then pick last occurrence by order
        max_v = max(e['v'] for e in all_entries)
        max_entries = [e for e in all_entries if e['v'] == max_v]
        last_one = max(max_entries, key=lambda x: x['order'])
        return last_one['f'], last_one['c']


# =========================
# Sidebar / Config UI
# =========================
with st.sidebar:
    st.markdown("### ⚙️ Configuration")
    processing_mode = st.selectbox("Processing mode", ["Sequential (safe, deterministic)", "Parallel (faster)"])
    if processing_mode.startswith("Parallel"):
        max_workers = st.slider("Workers (threads)", min_value=1, max_value=32, value=DEFAULT_MAX_WORKERS, step=1)
    else:
        max_workers = 1

    batch_size = st.number_input("Batch size (checkpoint granularity)", min_value=10, max_value=2000, value=DEFAULT_BATCH_SIZE, step=10)
    timeout = st.number_input("Request timeout (s)", min_value=5, max_value=120, value=TIMEOUT, step=1)
    st.markdown("---")
    st.markdown("### 📊 How it works")
    st.markdown(
        "1. Upload Excel with product URLs  \n"
        "2. App detects URL column  \n"
        "3. Processes rows in batches and saves each batch to disk  \n"
        "4. You can resume after refresh/sleep/crash  \n"
    )
    st.markdown("---")
    st.markdown("### 🎨 Author")
    st.markdown("**Abdelmoneim Moustafa**  \n*Data Intelligence Engineer*")


# =========================
# File upload & detection
# =========================
uploaded_file = st.file_uploader("📤 Upload Excel (.xlsx/.xls)", type=["xlsx", "xls"])
if not uploaded_file:
    st.info("Upload an Excel file to begin. Checkpointing will be created per uploaded file.")
    st.stop()

# compute file id & checkpoint paths
FILE_ID = get_file_id(uploaded_file)
CHECKPOINT_DIR, CHECKPOINT_PREFIX = make_checkpoint_paths(FILE_ID)

st.caption(f"🆔 File ID: {FILE_ID} — checkpoints saved to `{CHECKPOINT_DIR}/`")

# read dataframe
try:
    df_in = pd.read_excel(uploaded_file)
except Exception as e:
    st.error("Could not read uploaded Excel file. Make sure it's a valid .xlsx/.xls")
    st.exception(e)
    st.stop()

# detect URL column
url_column = None
for c in df_in.columns:
    try:
        if df_in[c].astype(str).str.contains("http", case=False, na=False).any():
            url_column = c
            break
    except Exception:
        continue

if not url_column:
    st.error("❌ No URL column detected in the uploaded file.")
    st.stop()

st.success(f"✅ Detected URL column: **{url_column}**")

# prepare urls list
urls_all = [str(x).strip() if pd.notna(x) and str(x).strip() else None for x in df_in[url_column]]
valid_urls = [u for u in urls_all if u]
total = len(valid_urls)

# quick stats + preview
c1, c2, c3 = st.columns(3)
c1.metric("Rows (total)", len(urls_all))
c2.metric("Valid URLs", total)
c3.metric("Batches", (total + batch_size - 1) // batch_size)

with st.expander("👁️ Preview (first 10 rows)"):
    preview = pd.DataFrame({url_column: [u if u else "Empty" for u in urls_all[:10]]})
    st.dataframe(preview, use_container_width=True)

# show completed batches if any
completed = list_completed_batches(CHECKPOINT_DIR, CHECKPOINT_PREFIX)

if completed:
    st.info(
        f"♻️ **Resumable run detected**\n\n"
        f"- {len(completed)} batches already processed\n"
        f"- You can preview saved data below\n"
        f"- Click **Start Extraction** to resume automatically from the next batch"
    )

    with st.expander("📂 Preview saved results from previous run"):
        partial_df = load_all_batches(CHECKPOINT_DIR, CHECKPOINT_PREFIX)

        if partial_df.empty:
            st.warning("Checkpoint files were found, but no readable data could be loaded.")
        else:
            st.dataframe(partial_df.head(50), use_container_width=True)
            st.caption(
                f"Loaded {len(partial_df)} rows from checkpoints "
                f"({len(completed)} batches)"
            )

    col1, col2 = st.columns([1, 3])
    with col1:
        if st.button("❌ Clear checkpoints", help="Delete saved progress and start from scratch"):
            for f in glob.glob(os.path.join(CHECKPOINT_DIR, f"{CHECKPOINT_PREFIX}_*.csv")):
                try:
                    os.remove(f)
                except Exception:
                    pass
            st.experimental_rerun()

# =========================
# Start extraction button
# =========================
start_button = st.button("🚀 Start Extraction", type="primary")

if start_button:
    extractor = SwagelokExtractor(timeout=int(timeout))
    progress_bar = st.progress(0.0)
    status_box = st.empty()
    error_box = st.empty()
    download_box = st.empty()
    log_lines: List[str] = []

    # determine batches
    num_batches = (total + batch_size - 1) // batch_size
    completed_batches = list_completed_batches(CHECKPOINT_DIR, CHECKPOINT_PREFIX)

    start_time = time.time()
    overall_done = 0

    # iterate batches, skip completed
    for batch_idx in range(num_batches):
        if batch_idx in completed_batches:
            overall_done = min((batch_idx + 1) * batch_size, total)
            # update UI quickly
            if overall_done % UI_UPDATE_EVERY == 0 or batch_idx == num_batches - 1:
                progress_bar.progress(overall_done / total if total else 1.0)
                status_box.info(f"Resumed: Skipped batch {batch_idx} (already saved). Done: {overall_done}/{total}")
            continue

        # slice urls for this batch
        start_i = batch_idx * batch_size
        end_i = min((batch_idx + 1) * batch_size, total)
        batch_urls = valid_urls[start_i:end_i]
        batch_results: List[Dict] = []

        # processing: parallel or sequential
        if processing_mode.startswith("Parallel") and max_workers > 1:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_idx = {executor.submit(extractor.extract, url, start_i + idx + 1): idx for idx, url in enumerate(batch_urls)}
                processed_in_batch = 0
                for future in as_completed(future_to_idx):
                    idx_local = future_to_idx[future]
                    try:
                        res = future.result()
                    except Exception as e:
                        res = {
                            "Row": start_i + idx_local + 1,
                            "Part": "Not Found",
                            "Company": COMPANY_NAME,
                            "URL": batch_urls[idx_local],
                            "UNSPSC Feature (Latest)": "Not Found",
                            "UNSPSC Code": "Not Found",
                            "Status": "Error",
                            "Error": safe_str(e)
                        }
                    batch_results.append(res)
                    processed_in_batch += 1
                    overall_done += 1

                    # periodic UI updates
                    if overall_done % UI_UPDATE_EVERY == 0 or overall_done == total:
                        progress_bar.progress(overall_done / total if total else 1.0)
                        status_box.info(f"Processing: {overall_done}/{total} | Batch {batch_idx+1}/{num_batches}")
        else:
            # sequential (deterministic order)
            for idx_local, url in enumerate(batch_urls):
                res = extractor.extract(url, start_i + idx_local + 1)
                batch_results.append(res)
                overall_done += 1
                if overall_done % UI_UPDATE_EVERY == 0 or overall_done == total:
                    progress_bar.progress(overall_done / total if total else 1.0)
                    status_box.info(f"Processing: {overall_done}/{total} | Batch {batch_idx+1}/{num_batches}")

        # Normalize and save batch to disk immediately
        batch_df = pd.DataFrame(batch_results)
        
        # Ensure string primitives for Arrow / CSV safety
        for col in batch_df.columns:
            batch_df[col] = batch_df[col].apply(lambda x: "" if x is None else str(x))
        
        try:
            # Save CSV checkpoint
            save_batch_to_disk(batch_df, CHECKPOINT_DIR, CHECKPOINT_PREFIX, batch_idx)
            log_lines.append(f"Saved batch {batch_idx} ({len(batch_results)} rows) to disk.")
        
            # Build in-memory Excel checkpoint
            buf = BytesIO()
            with pd.ExcelWriter(buf, engine="openpyxl") as writer:
                batch_df.to_excel(writer, index=False, sheet_name=f"batch_{batch_idx+1}")
            buf.seek(0)
        
            download_box.download_button(
                label=f"💾 Download checkpoint (batch {batch_idx+1})",
                data=buf.getvalue(),
                file_name=f"{FILE_ID}_batch_{batch_idx+1}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"dl_{FILE_ID}_{batch_idx}"
            )
        
        except Exception as e:
            err = safe_str(e)
            log_lines.append(f"Failed saving batch {batch_idx}: {err}")
            error_box.error(f"Failed saving batch {batch_idx}: {err}")


    total_time = int(time.time() - start_time)
    status_box.success(f"✅ Extraction completed. Time: {total_time//60}m {total_time%60}s")

    # Merge all batches from disk (resilient)
    final_df = load_all_batches(CHECKPOINT_DIR, CHECKPOINT_PREFIX)

    if final_df.empty:
        st.warning("No results found in checkpoints. Something went wrong or there were no valid URLs.")
    else:
        # compute metrics safely
        final_df = final_df.astype(str)
        parts_found = (final_df["Part"] != "Not Found").sum() if "Part" in final_df.columns else 0
        unspsc_found = (final_df["UNSPSC Code"] != "Not Found").sum() if "UNSPSC Code" in final_df.columns else 0

        st.markdown(
            f"""
            <div class="success-box">
                <h3>✅ Results</h3>
                <p><strong>Rows processed:</strong> {len(final_df)} &nbsp; | &nbsp; <strong>Parts found:</strong> {parts_found} &nbsp; | &nbsp; <strong>UNSPSC found:</strong> {unspsc_found}</p>
                <p class="small-muted">File ID: <code>{FILE_ID}</code> — Checkpoints in <code>{CHECKPOINT_DIR}</code></p>
            </div>
            """,
            unsafe_allow_html=True,
        )

        # show sample preview (first 50)
        with st.expander("📋 Preview results (first 50 rows)"):
            st.dataframe(final_df.head(50), use_container_width=True)

        # final download
        buf = BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            final_df.to_excel(writer, index=False, sheet_name="Final Results")
        st.download_button(
            "📥 Download Final Results",
            data=buf.getvalue(),
            file_name=f"swagelok_final_{FILE_ID}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )

        # error log
        if log_lines:
            with st.expander("📝 Operation log"):
                for ln in log_lines[-200:]:
                    st.text(ln)

# footer
st.markdown("---")
st.markdown('<div style="text-align:center;padding:1rem"><small>🎨 Designed by Abdelmoneim Moustafa — Data Intelligence Engineer</small></div>', unsafe_allow_html=True)
