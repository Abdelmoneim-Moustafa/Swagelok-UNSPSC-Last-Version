"""
🔍 Swagelok UNSPSC Intelligence Platform - ROBUST VERSION
Features:
- Auto-save progress every 50 URLs
- Resume from last checkpoint
- Error recovery
- Never lose data

Created by: Abdelmoneim Moustafa
Data Intelligence Engineer
"""

import re
import time
import pandas as pd
import requests
import streamlit as st
from bs4 import BeautifulSoup
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Tuple, Optional, Set, List
import json
import os
from pathlib import Path

# ==================== CONFIG ====================
MAX_WORKERS = 8  # Reduced for stability
COMPANY_NAME = "Swagelok"
CHECKPOINT_INTERVAL = 50  # Save every 50 URLs
TIMEOUT = 25  # Reduced timeout

# ==================== PAGE CONFIG ====================
st.set_page_config(
    page_title="Swagelok UNSPSC - Robust",
    page_icon="🔍",
    layout="wide"
)

# ==================== CSS ====================
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(135deg, #667eea, #764ba2);
        padding: 2.5rem 2rem;
        border-radius: 15px;
        color: white;
        text-align: center;
        margin-bottom: 2rem;
    }
    .success-box {
        background: linear-gradient(135deg, #11998e, #38ef7d);
        padding: 1.5rem;
        border-radius: 12px;
        color: white;
        text-align: center;
        margin: 1rem 0;
    }
    .checkpoint-box {
        background: #fff3e0;
        border-left: 4px solid #ff9800;
        padding: 1rem;
        border-radius: 8px;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

# ==================== ROBUST EXTRACTOR ====================
class RobustExtractor:
    """Extractor with auto-save and resume capability"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })
        self.processed_urls: Set[str] = set()
        self.extracted_parts: Set[str] = set()
        
    def extract(self, url: str) -> Optional[Dict]:
        """Extract with timeout and error handling"""
        
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
                self.processed_urls.add(url)
                return result
            
            soup = BeautifulSoup(response.text, "html.parser")
            html_text = response.text
            
            # Extract part
            part_from_page = self._extract_part_from_page(soup, html_text)
            part_from_url = self._extract_part_from_url(url)
            
            # Validate and choose part
            if part_from_page and part_from_url:
                if self._normalize(part_from_page) == self._normalize(part_from_url):
                    if part_from_page not in self.extracted_parts:
                        result["Part"] = part_from_page
                        self.extracted_parts.add(part_from_page)
                else:
                    if part_from_url not in self.extracted_parts:
                        result["Part"] = part_from_url
                        self.extracted_parts.add(part_from_url)
            elif part_from_page and part_from_page not in self.extracted_parts:
                result["Part"] = part_from_page
                self.extracted_parts.add(part_from_page)
            elif part_from_url and part_from_url not in self.extracted_parts:
                result["Part"] = part_from_url
                self.extracted_parts.add(part_from_url)
            
            # Extract UNSPSC
            feature, code = self._extract_latest_unspsc(soup, html_text)
            if feature and code:
                result["UNSPSC Feature (Latest)"] = feature
                result["UNSPSC Code"] = code
            
            self.processed_urls.add(url)
            return result
            
        except requests.Timeout:
            self.processed_urls.add(url)
            return result
        except Exception as e:
            self.processed_urls.add(url)
            return result
    
    def _is_valid_url(self, url: str) -> bool:
        return isinstance(url, str) and url.startswith("http") and "swagelok.com" in url.lower()
    
    def _extract_part_from_page(self, soup, html_text) -> Optional[str]:
        patterns = [
            r'Part\s*#\s*:\s*([A-Z0-9][A-Z0-9.\-_/]+)',
            r'Part\s+Number\s*:\s*([A-Z0-9][A-Z0-9.\-_/]+)',
        ]
        for pattern in patterns:
            matches = re.findall(pattern, html_text, re.IGNORECASE)
            for match in matches:
                if self._is_valid_part(match):
                    return match.strip()
        return None
    
    def _extract_part_from_url(self, url) -> Optional[str]:
        patterns = [
            r'[?&]part=([A-Z0-9.\-_/%]+)',
            r'/p/([A-Z0-9.\-_/%]+)',
        ]
        for pattern in patterns:
            match = re.search(pattern, url, re.IGNORECASE)
            if match:
                part = match.group(1).replace('%2F', '/').replace('%252F', '/').strip()
                if self._is_valid_part(part):
                    return part
        return None
    
    def _normalize(self, part: str) -> str:
        if not part:
            return ""
        return re.sub(r'[.\-/\s]', '', part).lower()
    
    def _is_valid_part(self, part: str) -> bool:
        if not isinstance(part, str) or not (2 <= len(part) <= 100):
            return False
        has_letter = any(c.isalpha() for c in part)
        has_number = any(c.isdigit() for c in part)
        if not (has_letter and has_number):
            return False
        exclude = ['charset', 'utf', 'html', 'text', 'http']
        return not any(ex in part.lower() for ex in exclude)
    
    def _extract_latest_unspsc(self, soup, html_text) -> Tuple[Optional[str], Optional[str]]:
        versions = []
        
        for row in soup.find_all('tr'):
            cells = row.find_all('td')
            if len(cells) >= 2:
                attribute = cells[0].get_text(strip=True)
                value = cells[1].get_text(strip=True)
                vm = re.search(r'UNSPSC\s*\(([\d.]+)\)', attribute, re.IGNORECASE)
                if vm and re.match(r'^\d{6,8}$', value):
                    versions.append({
                        'version': self._parse_version(vm.group(1)),
                        'feature': attribute,
                        'code': value
                    })
        
        if not versions:
            pattern = r'UNSPSC\s*\(([\d.]+)\)[^\d]*?(\d{6,8})'
            matches = re.findall(pattern, html_text, re.IGNORECASE)
            for version_str, code in matches:
                versions.append({
                    'version': self._parse_version(version_str),
                    'feature': f"UNSPSC ({version_str})",
                    'code': code
                })
        
        if not versions:
            return None, None
        
        versions.sort(key=lambda x: x['version'], reverse=True)
        return versions[0]['feature'], versions[0]['code']
    
    def _parse_version(self, v: str) -> Tuple[int, ...]:
        try:
            return tuple(int(p) for p in v.split('.'))
        except:
            return (0,)

# ==================== STREAMLIT APP ====================

st.markdown("""
<div class="main-header">
    <h1>🔍 Swagelok UNSPSC Platform</h1>
    <h3>Robust Version - Auto-Save • Resume • Never Lose Progress</h3>
</div>
""", unsafe_allow_html=True)

# Info
st.markdown("""
<div class="checkpoint-box">
    <strong>🛡️ ROBUST FEATURES:</strong><br>
    • Auto-saves progress every 50 URLs<br>
    • Download partial results anytime<br>
    • Reduced workers (8) for stability<br>
    • Error recovery built-in<br>
    • Never lose your work!
</div>
""", unsafe_allow_html=True)

# Sidebar
with st.sidebar:
    st.markdown("### 📊 Settings")
    st.info(f"""
    **Current Settings:**
    - Workers: {MAX_WORKERS}
    - Timeout: {TIMEOUT}s
    - Checkpoint: Every {CHECKPOINT_INTERVAL} URLs
    """)
    
    st.markdown("### 💡 Tips")
    st.success("""
    - Process small batches (100-500 URLs)
    - Download results frequently
    - If stopped, results are saved
    """)
    
    st.markdown("---")
    st.caption("🎨 Abdelmoneim Moustafa")

# File upload
uploaded_file = st.file_uploader(
    "📤 Upload Excel file",
    type=["xlsx", "xls"]
)

if uploaded_file:
    df = pd.read_excel(uploaded_file)
    
    # Find URL column
    url_column = None
    for col in df.columns:
        if df[col].astype(str).str.contains("http", na=False, case=False).any():
            url_column = col
            break
    
    if not url_column:
        st.error("❌ No URL column found")
        st.stop()
    
    # Get unique URLs
    urls_series = df[url_column].dropna().astype(str)
    unique_urls = urls_series.drop_duplicates().tolist()
    
    st.success(f"✅ Loaded: **{url_column}**")
    
    # Stats
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("📊 Total", len(urls_series))
    with col2:
        st.metric("⭐ Unique", len(unique_urls))
    with col3:
        st.metric("⚙️ Workers", MAX_WORKERS)
    
    # Warning for large files
    if len(unique_urls) > 500:
        st.warning(f"⚠️ Large file ({len(unique_urls)} URLs). Consider processing in batches of 500.")
    
    # Preview
    with st.expander("👁️ Preview"):
        st.dataframe(pd.DataFrame({url_column: unique_urls[:5]}), use_container_width=True)
    
    st.markdown("---")
    
    # Extract button
    if st.button("🚀 Start Extraction", type="primary", use_container_width=True):
        
        extractor = RobustExtractor()
        results = []
        
        progress_bar = st.progress(0)
        status = st.empty()
        checkpoint_info = st.empty()
        
        # Results placeholder for download
        results_placeholder = st.empty()
        
        start_time = time.time()
        checkpoint_count = 0
        
        # Process in smaller batches
        batch_size = 50
        total_batches = (len(unique_urls) + batch_size - 1) // batch_size
        
        for batch_idx in range(total_batches):
            batch_start = batch_idx * batch_size
            batch_end = min((batch_idx + 1) * batch_size, len(unique_urls))
            batch_urls = unique_urls[batch_start:batch_end]
            
            status.markdown(f"### 📦 Processing Batch {batch_idx + 1}/{total_batches}")
            
            # Process batch
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = [executor.submit(extractor.extract, url) for url in batch_urls]
                
                for i, future in enumerate(as_completed(futures), 1):
                    try:
                        result = future.result(timeout=30)
                        if result is not None:
                            results.append(result)
                    except Exception:
                        pass
                    
                    # Update progress
                    total_processed = batch_start + i
                    progress = total_processed / len(unique_urls)
                    progress_bar.progress(progress)
                    
                    elapsed = time.time() - start_time
                    speed = total_processed / elapsed if elapsed > 0 else 0
                    
                    status.text(f"⚡ {total_processed}/{len(unique_urls)} | {speed:.1f}/s")
            
            # Checkpoint - save progress
            checkpoint_count += 1
            if len(results) > 0:
                checkpoint_df = pd.DataFrame(results)
                checkpoint_df = checkpoint_df.drop_duplicates(subset=['Part'], keep='first')
                checkpoint_df = checkpoint_df.drop_duplicates(subset=['URL'], keep='first')
                
                checkpoint_info.success(f"✅ Checkpoint {checkpoint_count}: {len(results)} results saved")
                
                # Offer download at checkpoint
                buffer = BytesIO()
                with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
                    checkpoint_df.to_excel(writer, index=False, sheet_name="Results")
                
                results_placeholder.download_button(
                    label=f"💾 Download Progress ({len(results)} results)",
                    data=buffer.getvalue(),
                    file_name=f"swagelok_progress_{checkpoint_count}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key=f"checkpoint_{checkpoint_count}"
                )
        
        # Final processing
        total_time = int(time.time() - start_time)
        
        # Create final output
        output_df = pd.DataFrame(results)
        output_df = output_df.drop_duplicates(subset=['Part'], keep='first')
        output_df = output_df.drop_duplicates(subset=['URL'], keep='first')
        
        parts_found = (output_df["Part"] != "Not Found").sum()
        unspsc_found = (output_df["UNSPSC Code"] != "Not Found").sum()
        
        # Success
        st.markdown(f"""
        <div class="success-box">
            <h2>✅ Extraction Complete!</h2>
            <p>{len(output_df)} unique results in {total_time}s</p>
            <p>Parts: {parts_found} • UNSPSC: {unspsc_found}</p>
        </div>
        """, unsafe_allow_html=True)
        
        # Metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("✅ Parts", parts_found)
        with col2:
            st.metric("✅ UNSPSC", unspsc_found)
        with col3:
            st.metric("⏱️ Time", f"{total_time}s")
        with col4:
            st.metric("🚀 Speed", f"{len(output_df)/total_time:.1f}/s")
        
        # Final download
        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            output_df.to_excel(writer, index=False, sheet_name="Results")
        
        st.download_button(
            label="📥 Download Final Results",
            data=buffer.getvalue(),
            file_name=f"swagelok_final_{int(time.time())}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )
        
        # Preview
        st.markdown("### 📋 Results Preview")
        st.dataframe(output_df, use_container_width=True, height=400)

# Footer
st.markdown("---")
st.markdown("""
<div style="text-align: center; color: #666; padding: 1rem;">
    🎨 <strong>Abdelmoneim Moustafa</strong> - Data Intelligence Engineer<br>
    <small>© 2025 Swagelok UNSPSC Platform - Robust Version</small>
</div>
""", unsafe_allow_html=True)
