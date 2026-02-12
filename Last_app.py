import pandas as pd
import requests
from bs4 import BeautifulSoup
import streamlit as st
from io import BytesIO
import time

# Constants
TIMEOUT = 20  # seconds for HTTP requests
COMPANY_NAME = "Swagelok"
CHECKPOINT_INTERVAL = 100  # save checkpoint every 100 URLs

# Page config and custom styles for a professional look
st.set_page_config(page_title="Swagelok UNSPSC Scraper", page_icon="🔍", layout="wide")
st.markdown("""
    <style>
    /* ==================== THEME VARIABLES ==================== */
    /* Light Mode (Default) */
    :root {
        --info-bg: #e3f2fd;
        --card-bg: #ffffff;
        --border-color: #e0e0e0;
        --error-bg: #ffebee;
        --text-color: #333333;
        --shadow: rgba(0, 0, 0, 0.1);
    }
    
    /* Dark Mode - System Preference */
    @media (prefers-color-scheme: dark) {
        :root {
            --info-bg: #1a237e;
            --card-bg: #1e1e1e;
            --border-color: #424242;
            --error-bg: #b71c1c;
            --text-color: #e0e0e0;
            --shadow: rgba(0, 0, 0, 0.4);
        }
    }
    
    /* Dark Mode - Streamlit Theme */
    [data-theme="dark"] {
        --info-bg: #1a237e;
        --card-bg: #1e1e1e;
        --border-color: #424242;
        --error-bg: #b71c1c;
        --text-color: #e0e0e0;
        --shadow: rgba(0, 0, 0, 0.4);
    }
    
    /* ==================== HIDE UNWANTED ELEMENTS ==================== */
    /* Remove Streamlit branding and menu */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    .stDeployButton {display: none;}
    
    /* Remove horizontal lines after dataframes */
    hr {display: none !important;}
    .stHorizontalBlock {border: none !important;}
    
    /* ==================== CUSTOM COMPONENTS ==================== */
    .main-header {
        background: linear-gradient(135deg, #667eea, #764ba2);
        padding: 2.5rem;
        border-radius: 15px;
        color: white;
        text-align: center;
        margin-bottom: 2rem;
        box-shadow: 0 8px 20px rgba(102, 126, 234, 0.3);
    }
    
    .info-box {
        background: var(--info-bg);
        border-left: 5px solid #2196f3;
        padding: 1.5rem;
        border-radius: 12px;
        margin: 1rem 0;
        color: var(--text-color);
        box-shadow: 0 2px 8px var(--shadow);
    }
    
    .success-box {
        background: linear-gradient(135deg, #11998e, #38ef7d);
        padding: 2rem;
        border-radius: 15px;
        color: white;
        text-align: center;
        margin: 1.5rem 0;
        box-shadow: 0 8px 20px rgba(17, 153, 142, 0.3);
    }
    
    .progress-card {
        background: var(--card-bg);
        padding: 1.5rem;
        border-radius: 12px;
        margin: 1rem 0;
        border: 1px solid var(--border-color);
        color: var(--text-color);
        box-shadow: 0 2px 8px var(--shadow);
    }
    
    .error-card {
        background: var(--error-bg);
        border-left: 5px solid #f44336;
        padding: 1rem;
        border-radius: 8px;
        margin: 0.5rem 0;
        color: var(--text-color);
        box-shadow: 0 2px 8px var(--shadow);
    }
    
    /* ==================== STREAMLIT COMPONENT STYLING ==================== */
    /* Progress bar */
    .stProgress > div > div > div > div {
        background: linear-gradient(90deg, #667eea, #764ba2);
    }
    
    /* Metrics */
    [data-testid="stMetricValue"] {
        color: var(--text-color);
        font-size: 2rem;
        font-weight: 700;
    }
    
    [data-testid="stMetricLabel"] {
        color: var(--text-color);
        opacity: 0.8;
    }
    
    /* Buttons */
    .stButton > button {
        background: linear-gradient(135deg, #667eea, #764ba2);
        color: white;
        border: none;
        padding: 0.75rem 2rem;
        border-radius: 10px;
        font-weight: 600;
        transition: all 0.3s ease;
        box-shadow: 0 4px 12px rgba(102, 126, 234, 0.3);
    }
    
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 20px rgba(102, 126, 234, 0.5);
    }
    
    /* Download buttons */
    .stDownloadButton > button {
        background: linear-gradient(135deg, #11998e, #38ef7d);
        color: white;
        border: none;
        padding: 0.75rem 2rem;
        border-radius: 10px;
        font-weight: 600;
        transition: all 0.3s ease;
        box-shadow: 0 4px 12px rgba(17, 153, 142, 0.3);
    }
    
    .stDownloadButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 20px rgba(17, 153, 142, 0.5);
    }
    
    /* Radio buttons */
    .stRadio > label {
        color: var(--text-color);
    }
    
    /* File uploader */
    .stFileUploader {
        background: var(--card-bg);
        border: 2px dashed var(--border-color);
        border-radius: 12px;
        padding: 2rem;
    }
    
    /* Dataframe styling */
    .dataframe {
        border: 1px solid var(--border-color) !important;
        background: var(--card-bg);
    }
    
    .dataframe th {
        background: linear-gradient(135deg, #667eea, #764ba2) !important;
        color: white !important;
        font-weight: 600;
    }
    
    .dataframe td {
        color: var(--text-color);
    }
    
    /* Remove unwanted spacing and lines */
    .block-container {
        padding-top: 2rem;
        padding-bottom: 0;
    }
    
    .element-container {
        margin-bottom: 1rem;
    }
    
    /* Clean layout */
    section[data-testid="stSidebar"] {
        background: var(--card-bg);
        border-right: 1px solid var(--border-color);
    }
    </style>
    """, unsafe_allow_html=True)
st.markdown('<div class="main-header"><h1>🔍 Swagelok UNSPSC Platform</h1>'
            '<p>Extract Part - UNSPSC feature & code from Swagelok product pages</p></div>', unsafe_allow_html=True)
st.markdown('<div class="info-box"><strong>🔄 Workflow:</strong> Upload Excel → Process rows → Download results</div>', unsafe_allow_html=True)

# Mode selection: new upload vs resume
mode = st.radio("Choose mode:", ("New upload", "Resume from checkpoint"))
file_label = "Upload Excel file (URLs only)" if mode == "New upload" else "Upload checkpoint Excel"
uploaded_file = st.file_uploader(file_label, type=["xlsx", "xls"])

if uploaded_file:
    try:
        df = pd.read_excel(uploaded_file)
    except Exception as e:
        st.error(f"❌ Failed to read file: {e}")
        st.stop()

    # Identify the URL column (looking for a cell containing 'http')
    url_col = next((c for c in df.columns if df[c].astype(str).str.contains("http", na=False, case=False).any()), None)
    if not url_col:
        st.error("❌ No URL column found. Please provide an Excel with product-page URLs.")
        st.stop()
    st.success(f"✅ URL column detected: **{url_col}**")

    # Standardize column names and add missing columns
    df = df.rename(columns={url_col: "URL"})
    if "Company" not in df.columns:
        df["Company"] = COMPANY_NAME
    for col in ["Part", "UNSPSC Feature (Latest)", "UNSPSC Code", "Status", "Error"]:
        if col not in df.columns:
            df[col] = ""
    
    # Determine start index (for resume mode)
    start_idx = 0
    if mode == "Resume from checkpoint":
        last_idx = df["Status"].last_valid_index()
        start_idx = last_idx + 1 if pd.notna(last_idx) else 0

    urls = df["URL"].astype(str).tolist()
    total = len(urls)
    valid_count = sum(1 for u in urls if u and u.lower().startswith("http"))
    
    c1, c2, c3 = st.columns(3)
    c1.metric("📊 Total URLs", total)
    c2.metric("🔗 Valid URLs", valid_count)
    est_time = int(valid_count * 0.25)  # rough estimate: 0.25s per row
    c3.metric("⏱️ Est. time (s)", est_time)
    
    if st.button("🚀 Start Extraction", type="primary"):
        session = requests.Session()
        session.headers.update({"User-Agent": "Mozilla/5.0"})
        progress_box = st.empty()
        error_box = st.empty()
        checkpoint_box = st.empty()
        
        start_time = time.time()
        errors = []
        
        for idx in range(start_idx, total):
            url = urls[idx]
            row_num = idx + 1
            st.progress((idx+1) / total)  # update progress bar
            
            # Initialize default result
            row_result = {
                "Part": "Not Found",
                "UNSPSC Feature (Latest)": "Not Found",
                "UNSPSC Code": "Not Found",
                "Status": "Success",
                "Error": ""
            }
            
            # Validate URL
            if not url or not url.lower().startswith("http"):
                row_result["Status"] = "Invalid URL"
                row_result["Error"] = "Empty or invalid URL"
            else:
                try:
                    resp = session.get(url, timeout=TIMEOUT)
                    if resp.status_code != 200:
                        row_result["Status"] = f"HTTP {resp.status_code}"
                        row_result["Error"] = f"Status {resp.status_code}"
                    else:
                        soup = BeautifulSoup(resp.text, "html.parser")
                        html = resp.text
                        # Extract Part Number (regex search)
                        import re
                        m = re.search(r'Part\s*#\s*:\s*([A-Z0-9.\-_/]+)', html, re.IGNORECASE)
                        if m:
                            row_result["Part"] = m.group(1).strip()
                        # Extract all UNSPSC entries from the table
                        unspsc_entries = []
                        for tr in soup.find_all('tr'):
                            cells = tr.find_all('td')
                            if len(cells) >= 2:
                                attr = cells[0].text.strip()
                                val = cells[1].text.strip()
                                if attr.upper().startswith("UNSPSC") and re.match(r'^\d{6,8}$', val):
                                    # capture feature and code
                                    unspsc_entries.append((attr, val))
                        # Choose the latest UNSPSC by numeric version
                        if unspsc_entries:
                            # sort by version inside parentheses, e.g. (17.1001)
                            unspsc_entries.sort(key=lambda x: tuple(map(int, re.search(r'\(([\d\.]+)\)', x[0]).group(1).split('.'))), reverse=True)
                            row_result["UNSPSC Feature (Latest)"] = unspsc_entries[0][0]
                            row_result["UNSPSC Code"] = unspsc_entries[0][1]
                except Exception as e:
                    row_result["Status"] = "Error"
                    row_result["Error"] = str(e)[:100]
            
            # Write results into the DataFrame
            df.at[idx, "Part"] = row_result["Part"]
            df.at[idx, "UNSPSC Feature (Latest)"] = row_result["UNSPSC Feature (Latest)"]
            df.at[idx, "UNSPSC Code"] = row_result["UNSPSC Code"]
            df.at[idx, "Status"] = row_result["Status"]
            df.at[idx, "Error"] = row_result["Error"]
            
            # Update the progress status on UI
            elapsed = time.time() - start_time
            speed = (idx - start_idx + 1) / elapsed if elapsed > 0 else 0
            remaining = int((total - (idx+1)) / (speed or 1))
            progress_box.markdown(
                f'<div class="progress-card"><strong>Row {row_num}/{total}</strong><br>'
                f'Speed: {speed:.1f} rows/s | Remaining: {remaining}s<br>'
                f'<strong>Part:</strong> {row_result["Part"]} | '
                f'<strong>Code:</strong> {row_result["UNSPSC Code"]} | '
                f'<strong>Status:</strong> {row_result["Status"]}</div>', unsafe_allow_html=True)
            
            if row_result["Status"] != "Success":
                errors.append(f"Row {row_num}: {row_result['Status']} - {row_result['Error']}")
                error_box.markdown(
                    f'<div class="error-card">⚠️ <strong>Errors:</strong> {len(errors)}<br>Latest: {errors[-1]}</div>',
                    unsafe_allow_html=True)

            # Checkpoint: save every N rows or at end
            if ((row_num) % CHECKPOINT_INTERVAL) == 0 or (idx == total - 1):
                buf = BytesIO()
                with pd.ExcelWriter(buf, engine="openpyxl") as writer:
                    df.to_excel(writer, index=False)
                checkpoint_box.download_button(
                    label=f"💾 Checkpoint ({row_num})",
                    data=buf.getvalue(),
                    file_name=f"checkpoint_{row_num}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key=f"cp_{row_num}"
                )
        
        # Summary of results
        total_processed = total - start_idx
        success_count = (df.loc[start_idx:, "Status"] == "Success").sum()
        parts_found = (df.loc[start_idx:, "Part"] != "Not Found").sum()
        unspsc_found = (df.loc[start_idx:, "UNSPSC Code"] != "Not Found").sum()
        run_time = int(time.time() - start_time)
        st.markdown(
            f'<div class="success-box"><h2>✅ Complete!</h2>'
            f'<p>Processed: {total_processed} rows in {run_time//60}m {run_time%60}s</p>'
            f'<p><strong>Success:</strong> {success_count}/{total_processed} | '
            f'<strong>Parts found:</strong> {parts_found} | '
            f'<strong>UNSPSC found:</strong> {unspsc_found} | '
            f'<strong>Errors:</strong> {len(errors)}</p></div>', unsafe_allow_html=True)
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("✅ Success", success_count)
        c2.metric("✅ Parts", parts_found)
        c3.metric("✅ UNSPSC", unspsc_found)
        c4.metric("⚠️ Errors", len(errors))
        
        # Offer final results for download (dropping Status/Error columns)
        final_df = df.drop(columns=["Status", "Error"])
        final_buf = BytesIO()
        with pd.ExcelWriter(final_buf, engine="openpyxl") as writer:
            final_df.to_excel(writer, index=False, sheet_name="Results")
        st.download_button(
            "📥 Download Final Results", final_buf.getvalue(),
            file_name=f"swagelok_unspsc_results_{int(time.time())}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )
        
    # Show a quick preview of the dataframe
    st.markdown("### 📋 Sample of Results")
    st.dataframe(df.head(5))
