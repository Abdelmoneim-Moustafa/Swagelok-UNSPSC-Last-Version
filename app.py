"""
🔍 Swagelok UNSPSC Intelligence Platform - PRODUCTION VERSION
Professional data extraction tool for procurement teams

Features:
- Handles ALL part formats (2507-600-1-4, SS-4-TA, MS-TL-BGC, CWS-C.040-.405-P)
- Auto-save progress every 50 URLs
- Latest UNSPSC version detection
- Zero duplicates guarantee
- Professional user interface

Created by: Abdelmoneim Moustafa
Data Intelligence Engineer • Procurement Systems Expert
"""

import re
import time
import pandas as pd
import requests
import streamlit as st
from bs4 import BeautifulSoup
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Tuple, Optional, Set

# ==================== CONFIG ====================
MAX_WORKERS = 8  # Increased for better speed
COMPANY_NAME = "Swagelok"
CHECKPOINT_INTERVAL = 50
TIMEOUT = 15  # Reduced to fail faster on bad URLs
MAX_RETRIES = 2  # Added retry logic

# ==================== PAGE CONFIG ====================
st.set_page_config(
    page_title="Swagelok UNSPSC Intelligence Platform",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==================== ENHANCED CSS ====================
st.markdown("""
<style>
    .main { background: #f8f9fa; }
    .main-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 3rem 2rem;
        border-radius: 20px;
        color: white;
        text-align: center;
        margin-bottom: 2rem;
        box-shadow: 0 10px 40px rgba(102, 126, 234, 0.3);
    }
    .main-header h1 { font-size: 2.8rem; font-weight: 800; margin-bottom: 0.5rem; }
    .main-header h3 { font-size: 1.3rem; font-weight: 400; opacity: 0.95; margin-top: 0.5rem; }
    .feature-box {
        background: white;
        padding: 1.5rem;
        border-radius: 15px;
        border-left: 5px solid #667eea;
        margin: 1rem 0;
        box-shadow: 0 4px 15px rgba(0,0,0,0.08);
    }
    .feature-box h4 { color: #667eea; font-weight: 700; margin-bottom: 0.5rem; }
    .success-box {
        background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
        padding: 2rem;
        border-radius: 20px;
        color: white;
        text-align: center;
        margin: 2rem 0;
        box-shadow: 0 8px 30px rgba(17, 153, 142, 0.3);
    }
    .success-box h2 { font-size: 2rem; font-weight: 700; margin-bottom: 1rem; }
    .checkpoint-box {
        background: #fff3e0;
        border-left: 5px solid #ff9800;
        padding: 1.5rem;
        border-radius: 15px;
        margin: 1rem 0;
        box-shadow: 0 4px 15px rgba(255, 152, 0, 0.1);
    }
    .info-box {
        background: #e3f2fd;
        border-left: 5px solid #2196f3;
        padding: 1.5rem;
        border-radius: 15px;
        margin: 1rem 0;
    }
    .stat-card {
        background: white;
        padding: 1.5rem;
        border-radius: 12px;
        text-align: center;
        box-shadow: 0 4px 12px rgba(0,0,0,0.06);
        border-top: 3px solid #667eea;
    }
    .stButton>button {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        font-weight: 600;
        border: none;
        border-radius: 12px;
        padding: 0.75rem 2rem;
        box-shadow: 0 4px 15px rgba(102, 126, 234, 0.3);
        transition: all 0.3s ease;
    }
    .stButton>button:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 20px rgba(102, 126, 234, 0.4);
    }
    .stDownloadButton>button {
        background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
        color: white;
        font-weight: 600;
        border-radius: 12px;
        padding: 0.75rem 2rem;
        box-shadow: 0 4px 15px rgba(17, 153, 142, 0.3);
    }
    .progress-text {
        font-size: 1.2rem;
        font-weight: 600;
        color: #667eea;
        text-align: center;
        padding: 1rem;
    }
    .eta-box {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 1rem;
        border-radius: 12px;
        text-align: center;
        margin: 1rem 0;
        font-weight: 600;
    }
    .footer {
        text-align: center;
        padding: 2rem;
        margin-top: 3rem;
        border-top: 2px solid #e0e0e0;
        color: #666;
    }
    .footer-name { color: #667eea; font-weight: 700; font-size: 1.1rem; }
</style>
""", unsafe_allow_html=True)

# ==================== PRODUCTION EXTRACTOR ====================
class ProductionExtractor:
    """Production-grade extractor with all fixes applied"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })
        self.processed_urls: Set[str] = set()
        self.extracted_parts: Set[str] = set()

    def extract(self, url: str, retry_count: int = 0) -> Optional[Dict]:
        """Extract with comprehensive validation and retry logic"""

        url = str(url).strip()

        if url in self.processed_urls:
            return None

        result = {
            "Part": "Not Found",
            "Company": COMPANY_NAME,
            "URL": url,
            "UNSPSC Feature (Latest)": "Not Found",
            "UNSPSC Code": "Not Found"
        }

        if not self._is_valid_url(url):
            self.processed_urls.add(url)
            return result

        try:
            response = self.session.get(url, timeout=TIMEOUT)

            if response.status_code != 200:
                if response.status_code >= 500 and retry_count < MAX_RETRIES:
                    time.sleep(1)
                    return self.extract(url, retry_count + 1)
                self.processed_urls.add(url)
                return result

            soup = BeautifulSoup(response.text, "html.parser")
            html_text = response.text

            part_from_page = self._extract_part_from_page(soup, html_text)
            part_from_url = self._extract_part_from_url(url)

            selected_part = None

            if part_from_page and part_from_page not in self.extracted_parts:
                selected_part = part_from_page
            elif part_from_url and part_from_url not in self.extracted_parts:
                selected_part = part_from_url

            if part_from_page and self._normalize(part_from_page) == self._normalize(part_from_url):
                selected_part = part_from_page

            if selected_part:
                result["Part"] = selected_part
                self.extracted_parts.add(selected_part)

            feature, code = self._extract_latest_unspsc(soup, html_text)
            if feature and code:
                result["UNSPSC Feature (Latest)"] = feature
                result["UNSPSC Code"] = code

            self.processed_urls.add(url)
            return result

        except requests.Timeout:
            if retry_count < MAX_RETRIES:
                time.sleep(1)
                return self.extract(url, retry_count + 1)
            self.processed_urls.add(url)
            return result
        except Exception:
            self.processed_urls.add(url)
            return result

    def _is_valid_url(self, url: str) -> bool:
        return isinstance(url, str) and url.startswith("http") and "swagelok.com" in url.lower()

    def _extract_part_from_page(self, soup, html_text) -> Optional[str]:
        """
        FIXED: Handles ALL part formats including letter-only parts like MS-TL-BGC
        """
        patterns = [
            r"Part\s*#\s*:\s*([A-Za-z0-9][A-Za-z0-9.\-_/]*)",
            r"Part\s+Number\s*:\s*([A-Za-z0-9][A-Za-z0-9.\-_/]*)",
            r"\"partNumber\"\s*:\s*\"([A-Za-z0-9.\-_/]+)\"",
            r"data-part-number=\"([A-Za-z0-9.\-_/]+)\"",
        ]

        for pattern in patterns:
            matches = re.findall(pattern, html_text, re.IGNORECASE)
            for match in matches:
                cleaned = match.strip()
                if self._is_valid_part(cleaned):
                    return cleaned

        breadcrumb = soup.find("nav", {"aria-label": "breadcrumb"})
        if breadcrumb:
            links = breadcrumb.find_all("a")
            if links:
                last_link = links[-1].get_text(strip=True)
                if self._is_valid_part(last_link):
                    return last_link

        return None

    def _extract_part_from_url(self, url) -> Optional[str]:
        """Extract part from URL - FIXED to handle /p/ format"""
        patterns = [
            r"[?&]part=([A-Za-z0-9.\-_/%]+)",
            r"/p/([A-Za-z0-9.\-_/%]+)",
            r"/product/([A-Za-z0-9.\-_/%]+)",
        ]
        for pattern in patterns:
            match = re.search(pattern, url, re.IGNORECASE)
            if match:
                part = match.group(1).replace("%2F", "/").replace("%252F", "/").strip()
                if self._is_valid_part(part):
                    return part
        return None

    def _normalize(self, part: str) -> str:
        if not part:
            return ""
        return re.sub(r"[.\-/\s]", "", part).lower()

    def _is_valid_part(self, part: str) -> bool:
        """
        FIXED: Accepts letter-only parts (MS-TL-BGC), number-only, or alphanumeric
        """
        if not isinstance(part, str):
            return False

        cleaned = part.strip()
        if not (2 <= len(cleaned) <= 100):
            return False

        if not any(c.isalnum() for c in cleaned):
            return False

        exclude = ["charset", "utf", "html", "text", "http", "https", "www", "com", "json", "xml"]
        lower_cleaned = cleaned.lower()
        if any(ex in lower_cleaned for ex in exclude):
            return False

        alnum_count = sum(1 for c in cleaned if c.isalnum())
        return alnum_count >= 2

    def _extract_latest_unspsc(self, soup, html_text) -> Tuple[Optional[str], Optional[str]]:
        """Extract LATEST UNSPSC version from specifications table"""
        versions = []

        for row in soup.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) >= 2:
                attribute = cells[0].get_text(strip=True)
                value = cells[1].get_text(strip=True)
                vm = re.search(r"UNSPSC\s*\(([\d.]+)\)", attribute, re.IGNORECASE)
                if vm and re.match(r"^\d{6,8}$", value):
                    versions.append({
                        "version": self._parse_version(vm.group(1)),
                        "feature": attribute,
                        "code": value
                    })

        if not versions:
            pattern = r"UNSPSC\s*\(([\d.]+)\)[^\d]*(\d{6,8})"
            matches = re.findall(pattern, html_text, re.IGNORECASE)
            for version_str, code in matches:
                versions.append({
                    "version": self._parse_version(version_str),
                    "feature": f"UNSPSC ({version_str})",
                    "code": code
                })

        if not versions:
            return None, None

        versions.sort(key=lambda x: x["version"], reverse=True)
        return versions[0]["feature"], versions[0]["code"]

    def _parse_version(self, v: str) -> Tuple[int, ...]:
        try:
            return tuple(int(p) for p in v.split("."))
        except:
            return (0,)

# ==================== STREAMLIT UI ====================
st.markdown("""
<div class="main-header">
    <h1>🔍 Swagelok UNSPSC Intelligence Platform</h1>
    <h3>Professional Data Extraction • Enterprise Grade • Production Ready</h3>
</div>
""", unsafe_allow_html=True)

st.markdown("""
<div class="feature-box">
    <h4>✨ Platform Features</h4>
    • <strong>Universal Part Support:</strong> Handles all formats including letter-only parts (MS-TL-BGC)<br>
    • <strong>Smart Extraction:</strong> Latest UNSPSC version from specifications table<br>
    • <strong>Auto-Save:</strong> Progress saved every 50 URLs - never lose work<br>
    • <strong>Zero Duplicates:</strong> Guaranteed unique results<br>
    • <strong>Fast & Stable:</strong> Optimized with retry logic and ETA tracking
</div>
""", unsafe_allow_html=True)

with st.sidebar:
    st.markdown("### 📊 Platform Settings")
    st.markdown(f"""
    <div class="info-box">
        <strong>Current Configuration:</strong><br>
        • Workers: {MAX_WORKERS}<br>
        • Timeout: {TIMEOUT}s<br>
        • Max Retries: {MAX_RETRIES}<br>
        • Checkpoint: Every {CHECKPOINT_INTERVAL} URLs<br>
        • Status: Production Ready ✅
    </div>
    """, unsafe_allow_html=True)

    st.markdown("### 💡 Best Practices")
    st.markdown("""
    <div class="feature-box">
        <strong>For Best Results:</strong><br>
        ✅ Process 100-500 URLs per batch<br>
        ✅ Download checkpoints regularly<br>
        ✅ Check preview before processing<br>
        ✅ Verify results after download
    </div>
    """, unsafe_allow_html=True)

    st.markdown("---")
    st.markdown("""
    <div style="text-align: center;">
        <strong style="color: #667eea;">Created by</strong><br>
        <strong>Abdelmoneim Moustafa</strong><br>
        <small>Data Intelligence Engineer</small>
    </div>
    """, unsafe_allow_html=True)

st.markdown("### 📤 Upload Your Data")
uploaded_file = st.file_uploader(
    "Choose Excel file containing Swagelok product URLs",
    type=["xlsx", "xls"],
    help="File must contain at least one column with URLs"
)

if uploaded_file:
    df = pd.read_excel(uploaded_file)

    url_column = None
    for col in df.columns:
        if df[col].astype(str).str.contains("http", na=False, case=False).any():
            url_column = col
            break

    if not url_column:
        st.error("❌ No URL column detected in file")
        st.stop()

    urls_series = df[url_column].dropna().astype(str)
    unique_urls = urls_series.drop_duplicates().tolist()

    st.success(f"✅ File loaded successfully! Detected column: **{url_column}**")

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("📊 Total URLs", len(urls_series))
    with col2:
        st.metric("⭐ Unique URLs", len(unique_urls))
    with col3:
        st.metric("🗑️ Duplicates Removed", len(urls_series) - len(unique_urls))

    if len(unique_urls) > 500:
        st.markdown(f"""
        <div class="checkpoint-box">
            <strong>⚠️ Large File Detected</strong><br>
            You have {len(unique_urls)} URLs. Progress will be auto-saved every 50 URLs.
        </div>
        """, unsafe_allow_html=True)

    with st.expander("👁️ Preview Sample URLs"):
        st.dataframe(pd.DataFrame({url_column: unique_urls[:10]}), use_container_width=True)

    st.markdown("---")

    if st.button("🚀 Start Intelligent Extraction", type="primary", use_container_width=True):
        extractor = ProductionExtractor()
        results = []
        failed_urls = []

        progress_bar = st.progress(0)
        status = st.empty()
        eta_box = st.empty()
        checkpoint_info = st.empty()
        results_placeholder = st.empty()

        start_time = time.time()
        checkpoint_count = 0

        batch_size = CHECKPOINT_INTERVAL
        total_batches = (len(unique_urls) + batch_size - 1) // batch_size

        for batch_idx in range(total_batches):
            batch_start = batch_idx * batch_size
            batch_end = min((batch_idx + 1) * batch_size, len(unique_urls))
            batch_urls = unique_urls[batch_start:batch_end]

            status.markdown(f"""
            <div class="progress-text">
                📦 Processing Batch {batch_idx + 1} of {total_batches}
            </div>
            """, unsafe_allow_html=True)

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                future_to_url = {executor.submit(extractor.extract, url): url for url in batch_urls}

                for i, future in enumerate(as_completed(future_to_url), 1):
                    url = future_to_url[future]
                    try:
                        result = future.result(timeout=30)
                        if result is not None:
                            results.append(result)
                            if result["Part"] == "Not Found":
                                failed_urls.append({"URL": url, "Reason": "Part not found on page"})
                        else:
                            failed_urls.append({"URL": url, "Reason": "Duplicate or invalid URL"})
                    except Exception as e:
                        failed_urls.append({"URL": url, "Reason": f"Exception: {str(e)}"})

                    total_processed = batch_start + i
                    progress = total_processed / len(unique_urls)
                    progress_bar.progress(progress)

                    elapsed = time.time() - start_time
                    speed = total_processed / elapsed if elapsed > 0 else 0
                    remaining_urls = len(unique_urls) - total_processed
                    eta_seconds = remaining_urls / speed if speed > 0 else 0

                    if eta_seconds > 60:
                        eta_str = f"ETA: {int(eta_seconds // 60)}m {int(eta_seconds % 60)}s"
                    else:
                        eta_str = f"ETA: {int(eta_seconds)}s"

                    eta_box.markdown(f"""
                    <div class="eta-box">
                        ⚡ {total_processed}/{len(unique_urls)} | {speed:.1f} URLs/sec | {eta_str}
                    </div>
                    """, unsafe_allow_html=True)

                    status.markdown(f"""
                    <div class="progress-text">
                        Processing: {total_processed}/{len(unique_urls)} URLs
                    </div>
                    """, unsafe_allow_html=True)

            checkpoint_count += 1
            if len(results) > 0:
                checkpoint_df = pd.DataFrame(results)
                checkpoint_df = checkpoint_df.drop_duplicates(subset=["Part"], keep="first")
                checkpoint_df = checkpoint_df.drop_duplicates(subset=["URL"], keep="first")

                checkpoint_info.markdown(f"""
                <div class="checkpoint-box">
                    ✅ <strong>Checkpoint {checkpoint_count}:</strong> {len(results)} results | Failed: {len(failed_urls)}
                </div>
                """, unsafe_allow_html=True)

                buffer = BytesIO()
                with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
                    checkpoint_df.to_excel(writer, index=False, sheet_name="UNSPSC Results")

                results_placeholder.download_button(
                    label=f"💾 Download Checkpoint {checkpoint_count} ({len(results)} results)",
                    data=buffer.getvalue(),
                    file_name=f"swagelok_checkpoint_{checkpoint_count}_{int(time.time())}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key=f"checkpoint_{checkpoint_count}"
                )

        total_time = int(time.time() - start_time)

        output_df = pd.DataFrame(results)
        output_df = output_df.drop_duplicates(subset=["Part"], keep="first")
        output_df = output_df.drop_duplicates(subset=["URL"], keep="first")

        parts_found = (output_df["Part"] != "Not Found").sum()
        unspsc_found = (output_df["UNSPSC Code"] != "Not Found").sum()
        total_processed_count = len(output_df)

        st.markdown(f"""
        <div class="success-box">
            <h2>✅ Extraction Complete!</h2>
            <p style="font-size: 1.2rem; margin: 1rem 0;">
                <strong>{total_processed_count}</strong> products processed in <strong>{total_time}</strong> seconds
            </p>
            <p style="font-size: 1.1rem;">
                Parts Found: <strong>{parts_found}</strong> • UNSPSC Found: <strong>{unspsc_found}</strong> • Failed: <strong>{len(failed_urls)}</strong>
            </p>
        </div>
        """, unsafe_allow_html=True)

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("✅ Parts Found", parts_found, f"{parts_found/total_processed_count*100:.1f}%")
        with col2:
            st.metric("✅ UNSPSC Found", unspsc_found, f"{unspsc_found/total_processed_count*100:.1f}%")
        with col3:
            st.metric("⏱️ Total Time", f"{total_time}s")
        with col4:
            st.metric("🚀 Speed", f"{total_processed_count/total_time:.1f}/s" if total_time > 0 else "N/A")

        if failed_urls:
            with st.expander(f"⚠️ Failed URLs ({len(failed_urls)})"):
                failed_df = pd.DataFrame(failed_urls)
                st.dataframe(failed_df, use_container_width=True)

                failed_buffer = BytesIO()
                with pd.ExcelWriter(failed_buffer, engine="openpyxl") as writer:
                    failed_df.to_excel(writer, index=False, sheet_name="Failed URLs")

                st.download_button(
                    label="📥 Download Failed URLs List",
                    data=failed_buffer.getvalue(),
                    file_name=f"swagelok_failed_urls_{int(time.time())}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            output_df.to_excel(writer, index=False, sheet_name="UNSPSC Results")

        st.download_button(
            label="📥 Download Complete Results (Excel)",
            data=buffer.getvalue(),
            file_name=f"swagelok_complete_{int(time.time())}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )

        st.markdown("### 📋 Results Preview")
        st.dataframe(output_df, use_container_width=True, height=400)

st.markdown("---")
st.markdown("""
<div class="footer">
    <p class="footer-name">🎨 Designed & Developed by Abdelmoneim Moustafa</p>
    <p>Data Intelligence Engineer • Automation Specialist • Procurement Systems Expert</p>
    <p style="margin-top: 1rem; font-size: 0.9rem; color: #999;">
        © 2025 Swagelok UNSPSC Intelligence Platform • Production Version
    </p>
</div>
""", unsafe_allow_html=True)
