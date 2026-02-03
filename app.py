"""
🔍 Swagelok UNSPSC Intelligence Platform
Clean, Simple, Professional

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
from typing import Dict, Tuple, Optional

# ==================== CONFIG ====================
MAX_WORKERS = 10
COMPANY_NAME = "Swagelok"

# ==================== PAGE CONFIG ====================
st.set_page_config(
    page_title="Swagelok UNSPSC Platform",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==================== SIMPLE CSS ====================
st.markdown("""
<style>
    /* Clean and simple styling */
    .main-header {
        background: linear-gradient(135deg, #667eea, #764ba2);
        padding: 3rem 2rem;
        border-radius: 15px;
        color: white;
        text-align: center;
        margin-bottom: 2rem;
        box-shadow: 0 4px 15px rgba(0,0,0,0.1);
    }
    
    .main-header h1 {
        font-size: 2.5rem;
        margin-bottom: 0.5rem;
    }
    
    .main-header p {
        font-size: 1.2rem;
        opacity: 0.95;
    }
    
    .info-card {
        background: #f8f9ff;
        padding: 1.5rem;
        border-radius: 10px;
        border-left: 4px solid #667eea;
        margin: 1rem 0;
    }
    
    .success-box {
        background: linear-gradient(135deg, #11998e, #38ef7d);
        padding: 2rem;
        border-radius: 12px;
        color: white;
        text-align: center;
        margin: 1.5rem 0;
    }
    
    .metric-card {
        background: white;
        padding: 1.5rem;
        border-radius: 10px;
        text-align: center;
        box-shadow: 0 2px 10px rgba(0,0,0,0.05);
    }
    
    .metric-number {
        font-size: 2.5rem;
        font-weight: bold;
        color: #667eea;
    }
    
    .metric-label {
        color: #666;
        font-size: 0.9rem;
        text-transform: uppercase;
        margin-top: 0.5rem;
    }
</style>
""", unsafe_allow_html=True)

# ==================== EXTRACTOR CLASS ====================
class SwagelokUNSPSCExtractor:
    """Smart UNSPSC extractor with validation"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })
        self.processed_urls = set()
    
    def extract(self, url: str) -> Dict:
        """Extract part number and UNSPSC from URL"""
        result = {
            "Part": "Not Found",
            "Company": COMPANY_NAME,
            "URL": url,
            "UNSPSC Feature (Latest)": "Not Found",
            "UNSPSC Code": "Not Found"
        }
        
        if not self._is_valid_url(url):
            return result
        
        if url in self.processed_urls:
            return None
        
        try:
            response = self.session.get(url, timeout=30)
            if response.status_code != 200:
                return result
            
            soup = BeautifulSoup(response.text, "html.parser")
            html_text = response.text
            
            # Extract part number
            part_number = self._extract_part_number(soup, html_text, url)
            if part_number:
                result["Part"] = part_number
            
            # Extract latest UNSPSC
            feature, code = self._extract_latest_unspsc(soup, html_text)
            if feature and code:
                result["UNSPSC Feature (Latest)"] = feature
                result["UNSPSC Code"] = code
            
            self.processed_urls.add(url)
            return result
            
        except Exception:
            return result
    
    def _is_valid_url(self, url: str) -> bool:
        """Validate URL"""
        return isinstance(url, str) and url.startswith("http") and "swagelok.com" in url.lower()
    
    def _extract_part_number(self, soup: BeautifulSoup, html_text: str, url: str) -> str:
        """Extract complete part number"""
        # Strategy 1: "Part #:" label
        part_pattern = r'Part\s*#\s*:\s*([A-Z0-9][A-Z0-9.\-_%]+)'
        matches = re.findall(part_pattern, html_text, re.IGNORECASE)
        for match in matches:
            if self._is_valid_part(match):
                return match.strip()
        
        # Strategy 2: URL path
        url_patterns = [r'/p/([A-Z0-9.\-_%]+)', r'part=([A-Z0-9.\-_%]+)']
        for pattern in url_patterns:
            match = re.search(pattern, url, re.IGNORECASE)
            if match:
                part = match.group(1).replace('%2F', '/').replace('%252F', '/')
                if self._is_valid_part(part):
                    return part
        
        # Strategy 3: Page title
        title_tag = soup.find('title')
        if title_tag:
            match = re.search(r'([A-Z0-9]+[.\-][A-Z0-9.\-]+)', title_tag.get_text())
            if match and self._is_valid_part(match.group(1)):
                return match.group(1)
        
        return "Not Found"
    
    def _is_valid_part(self, part: str) -> bool:
        """Validate part number"""
        if not isinstance(part, str) or not (3 <= len(part) <= 100):
            return False
        has_letter = any(c.isalpha() for c in part)
        has_number = any(c.isdigit() for c in part)
        if not (has_letter and has_number):
            return False
        exclude = ['charset', 'utf', 'html', 'text', 'http']
        return not any(term in part.lower() for term in exclude)
    
    def _extract_latest_unspsc(self, soup: BeautifulSoup, html_text: str) -> Tuple[Optional[str], Optional[str]]:
        """Extract latest UNSPSC version"""
        versions = []
        
        # Parse table
        for row in soup.find_all('tr'):
            cells = row.find_all('td')
            if len(cells) >= 2:
                label = cells[0].get_text(strip=True)
                value = cells[1].get_text(strip=True)
                vm = re.search(r'UNSPSC\s*\(([\d.]+)\)', label, re.IGNORECASE)
                if vm and re.match(r'^\d{6,8}$', value):
                    versions.append({
                        'version': self._parse_version(vm.group(1)),
                        'feature': label,
                        'code': value
                    })
        
        # Regex fallback
        if not versions:
            for v, c in re.findall(r'UNSPSC\s*\(([\d.]+)\)[^\d]*?(\d{6,8})', html_text, re.IGNORECASE):
                versions.append({
                    'version': self._parse_version(v),
                    'feature': f"UNSPSC ({v})",
                    'code': c
                })
        
        if not versions:
            return None, None
        
        versions.sort(key=lambda x: x['version'], reverse=True)
        return versions[0]['feature'], versions[0]['code']
    
    def _parse_version(self, v: str) -> Tuple[int, ...]:
        """Parse version to tuple"""
        try:
            return tuple(int(p) for p in v.split('.'))
        except:
            return (0,)

# ==================== WELCOME SECTION ====================
st.markdown("""
<div class="main-header">
    <h1>🔍 Swagelok UNSPSC Intelligence Platform</h1>
    <p>Extract Product Codes & UNSPSC Data Automatically</p>
</div>
""", unsafe_allow_html=True)

# ==================== SIDEBAR INFO ====================
with st.sidebar:
    st.markdown("### 📖 About")
    st.info("""
    **Smart Features:**
    - ✅ Complete part numbers
    - ✅ Latest UNSPSC codes
    - ✅ Fast parallel processing
    - ✅ No duplicates
    - ✅ Excel output
    """)
    
    st.markdown("### 🎯 How to Use")
    st.markdown("""
    1. Upload Excel file
    2. File must have URL column
    3. Click "Start Extraction"
    4. Download results
    """)
    
    st.markdown("---")
    st.markdown("**Created by:**  \n🎨 Abdelmoneim Moustafa")
    st.caption("Data Intelligence Engineer")

# ==================== MAIN CONTENT ====================

# Welcome message
st.markdown("""
<div class="info-card">
    <h3>📤 Quick Start Guide</h3>
    <p><strong>Step 1:</strong> Prepare your Excel file with Swagelok product URLs</p>
    <p><strong>Step 2:</strong> Upload the file below</p>
    <p><strong>Step 3:</strong> Click "Start Extraction"</p>
    <p><strong>Step 4:</strong> Download your results</p>
</div>
""", unsafe_allow_html=True)

# File upload
st.markdown("### 📂 Upload Your File")
uploaded_file = st.file_uploader(
    "Choose Excel file (.xlsx or .xls)",
    type=["xlsx", "xls"],
    help="File must contain a column with Swagelok product URLs"
)

if uploaded_file:
    # Read file
    df = pd.read_excel(uploaded_file)
    
    # Auto-detect URL column
    url_column = None
    for col in df.columns:
        if df[col].astype(str).str.contains("http", na=False, case=False).any():
            url_column = col
            break
    
    if not url_column:
        st.error("❌ No URL column found in file")
        st.stop()
    
    # Remove duplicates
    urls = df[url_column].dropna().drop_duplicates().tolist()
    
    st.success(f"✅ File loaded successfully! Column: **{url_column}**")
    
    # Show basic stats
    st.markdown("### 📊 File Summary")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        <div class="metric-card">
            <div class="metric-number">{}</div>
            <div class="metric-label">Total URLs</div>
        </div>
        """.format(len(urls)), unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="metric-card">
            <div class="metric-number">{}</div>
            <div class="metric-label">Company</div>
        </div>
        """.format(COMPANY_NAME), unsafe_allow_html=True)
    
    with col3:
        st.markdown("""
        <div class="metric-card">
            <div class="metric-number">{}</div>
            <div class="metric-label">Workers</div>
        </div>
        """.format(MAX_WORKERS), unsafe_allow_html=True)
    
    # Preview
    with st.expander("👁️ Preview URLs (first 5)"):
        st.dataframe(pd.DataFrame({url_column: urls[:5]}), use_container_width=True)
    
    st.markdown("---")
    
    # Extract button
    if st.button("🚀 Start Extraction", type="primary", use_container_width=True):
        extractor = SwagelokUNSPSCExtractor()
        results = []
        
        progress_bar = st.progress(0)
        status = st.empty()
        
        start_time = time.time()
        
        # Process URLs
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(extractor.extract, url) for url in urls]
            
            for i, future in enumerate(as_completed(futures), 1):
                result = future.result()
                if result is not None:
                    results.append(result)
                
                progress = i / len(urls)
                progress_bar.progress(progress)
                
                elapsed = time.time() - start_time
                speed = i / elapsed if elapsed > 0 else 0
                remaining = int((len(urls) - i) / speed) if speed > 0 else 0
                
                status.text(f"⚡ Processing: {i}/{len(urls)} | Speed: {speed:.1f}/s | Remaining: {remaining}s")
        
        total_time = int(time.time() - start_time)
        output_df = pd.DataFrame(results)
        
        # Statistics
        parts_found = (output_df["Part"] != "Not Found").sum()
        unspsc_found = (output_df["UNSPSC Code"] != "Not Found").sum()
        
        # Success message
        st.markdown(f"""
        <div class="success-box">
            <h2>✅ Extraction Complete!</h2>
            <p><strong>{len(results)}</strong> products processed in <strong>{total_time}</strong> seconds</p>
            <p>Parts: {parts_found} found | UNSPSC: {unspsc_found} found</p>
        </div>
        """, unsafe_allow_html=True)
        
        # Results summary
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("✅ Parts Found", parts_found)
        with col2:
            st.metric("✅ UNSPSC Found", unspsc_found)
        with col3:
            st.metric("⏱️ Time", f"{total_time}s")
        with col4:
            st.metric("🚀 Speed", f"{len(results)/total_time:.1f}/s")
        
        # Download button
        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            output_df.to_excel(writer, index=False, sheet_name="Results")
        
        st.download_button(
            label="📥 Download Excel Results",
            data=buffer.getvalue(),
            file_name=f"swagelok_results_{int(time.time())}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )
        
        # Show results
        st.markdown("### 📋 Results Preview")
        st.dataframe(output_df, use_container_width=True, height=400)

else:
    # Show example before upload
    st.markdown("### 💡 Example")
    st.info("""
    **Your Excel file should look like this:**
    
    | URL Column Name |
    |----------------|
    | https://products.swagelok.com/en/c/straights/p/SS-8-RSD-2V |
    | https://products.swagelok.com/en/c/fixed-pressure/p/SS-CHS12-1%252F3 |
    | ... |
    
    The column name can be anything (we auto-detect it).
    """)

# Footer
st.markdown("---")
st.markdown("""
<div style="text-align: center; color: #666; padding: 1rem;">
    🎨 <strong>Designed & Developed by Abdelmoneim Moustafa</strong><br>
    Data Intelligence Engineer • Procurement Systems Expert<br>
    <small>© 2025 Swagelok UNSPSC Intelligence Platform</small>
</div>
""", unsafe_allow_html=True)
