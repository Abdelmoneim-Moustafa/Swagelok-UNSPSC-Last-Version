"""
🔍 Swagelok UNSPSC Intelligence Platform
Created by: Abdelmoneim Moustafa
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
    page_title="Swagelok UNSPSC Platform - Fixed",
    page_icon="🔍",
    layout="wide"
)

# ==================== CSS ====================
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(135deg, #667eea, #764ba2);
        padding: 3rem 2rem;
        border-radius: 15px;
        color: white;
        text-align: center;
        margin-bottom: 2rem;
    }
    .success-box {
        background: linear-gradient(135deg, #11998e, #38ef7d);
        padding: 2rem;
        border-radius: 12px;
        color: white;
        text-align: center;
        margin: 1.5rem 0;
    }
    .warning-box {
        background: #fff3e0;
        border-left: 4px solid #ff9800;
        padding: 1.5rem;
        border-radius: 8px;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

# ==================== EXTRACTOR (COMPLETELY FIXED) ====================
class SwagelokExtractor:
    """Extract part number and UNSPSC from Swagelok pages - FIXED VERSION"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })
    
    def extract(self, url: str) -> Optional[Dict]:
        """Extract data from URL - returns None for invalid URLs"""
        
        # Initialize result
        result = {
            "Part": "Not Found",
            "Company": COMPANY_NAME,
            "URL": url,
            "UNSPSC Feature (Latest)": "Not Found",
            "UNSPSC Code": "Not Found"
        }
        
        # Validate URL
        if not isinstance(url, str):
            return None
        
        url = str(url).strip()
        
        if not url.startswith("http"):
            return None
        
        if "swagelok.com" not in url.lower():
            return None
        
        try:
            # Fetch the page
            response = self.session.get(url, timeout=30)
            
            if response.status_code != 200:
                return result
            
            soup = BeautifulSoup(response.text, "html.parser")
            html_text = response.text
            
            # STEP 1: Extract part number FROM THE PAGE (not from URL)
            part_number = self._extract_part_from_page(soup, html_text, url)
            if part_number:
                result["Part"] = part_number
            
            # STEP 2: Extract latest UNSPSC
            feature, code = self._extract_latest_unspsc(soup, html_text)
            if feature and code:
                result["UNSPSC Feature (Latest)"] = feature
                result["UNSPSC Code"] = code
            
            return result
            
        except Exception as e:
            print(f"Error processing {url}: {str(e)}")
            return result
    
    def _extract_part_from_page(self, soup: BeautifulSoup, html_text: str, url: str) -> str:
        """
        Extract part number using PRIORITY ORDER:
        1. "Part #:" label on the page (MOST RELIABLE)
        2. URL parameter
        3. Page title
        4. Meta tags
        """
        
        # ================================
        # PRIORITY 1: "Part #:" label on page
        # ================================
        # Pattern: Part #: CWS-C.040-.405-P
        patterns = [
            r'Part\s*#?\s*:?\s*([A-Z0-9][A-Z0-9.\-_/]+)',
            r'Part\s+Number\s*:?\s*([A-Z0-9][A-Z0-9.\-_/]+)',
            r'Product\s*#?\s*:?\s*([A-Z0-9][A-Z0-9.\-_/]+)',
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, html_text, re.IGNORECASE)
            for match in matches:
                candidate = match.strip()
                if self._is_valid_part(candidate):
                    return candidate
        
        # ================================
        # PRIORITY 2: URL parameter
        # ================================
        # Extract from: ?part=ABC-123 or /p/ABC-123
        url_patterns = [
            r'[?&]part=([A-Z0-9.\-_/%]+)',
            r'/p/([A-Z0-9.\-_/%]+)',
        ]
        
        for pattern in url_patterns:
            match = re.search(pattern, url, re.IGNORECASE)
            if match:
                candidate = match.group(1)
                # URL decode
                candidate = candidate.replace('%2F', '/').replace('%252F', '/')
                candidate = candidate.replace('%20', ' ').strip()
                if self._is_valid_part(candidate):
                    return candidate
        
        # ================================
        # PRIORITY 3: Page title
        # ================================
        title_tag = soup.find('title')
        if title_tag:
            title_text = title_tag.get_text()
            # Look for part-like patterns
            match = re.search(r'([A-Z0-9]{2,}[.\-][A-Z0-9.\-]+)', title_text)
            if match:
                candidate = match.group(1)
                if self._is_valid_part(candidate):
                    return candidate
        
        # ================================
        # PRIORITY 4: Meta tags
        # ================================
        for meta in soup.find_all("meta"):
            content = meta.get("content", "")
            if content and self._is_valid_part(content):
                return content
        
        return "Not Found"
    
    def _is_valid_part(self, part: str) -> bool:
        """
        Strict validation for part numbers
        Must have:
        - Letters AND numbers
        - Length 3-100
        - Not common false positives
        """
        if not isinstance(part, str):
            return False
        
        part = part.strip()
        
        # Length check
        if not (3 <= len(part) <= 100):
            return False
        
        # Must have both letters and numbers
        has_letter = any(c.isalpha() for c in part)
        has_number = any(c.isdigit() for c in part)
        
        if not (has_letter and has_number):
            return False
        
        # Exclude false positives
        false_positives = [
            'charset', 'utf-8', 'utf8', 'html', 'text/html',
            'content-type', 'application', 'javascript', 'css',
            'http', 'https', 'www', 'com', 'swagelok'
        ]
        
        part_lower = part.lower()
        for fp in false_positives:
            if fp in part_lower:
                return False
        
        return True
    
    def _extract_latest_unspsc(self, soup: BeautifulSoup, html_text: str) -> Tuple[Optional[str], Optional[str]]:
        """Extract the LATEST UNSPSC version from specifications table"""
        
        versions = []
        
        # METHOD 1: Table parsing (PRIMARY)
        for row in soup.find_all('tr'):
            cells = row.find_all('td')
            
            if len(cells) >= 2:
                label = cells[0].get_text(strip=True)
                value = cells[1].get_text(strip=True)
                
                # Check for UNSPSC pattern
                version_match = re.search(r'UNSPSC\s*\(([\d.]+)\)', label, re.IGNORECASE)
                
                # Validate code (6-8 digits)
                if version_match and re.match(r'^\d{6,8}$', value):
                    version_str = version_match.group(1)
                    version_tuple = self._parse_version(version_str)
                    
                    versions.append({
                        'version': version_tuple,
                        'feature': label,
                        'code': value
                    })
        
        # METHOD 2: Regex fallback (SECONDARY)
        if not versions:
            pattern = r'UNSPSC\s*\(([\d.]+)\)[^\d]*?(\d{6,8})'
            matches = re.findall(pattern, html_text, re.IGNORECASE)
            
            for version_str, code in matches:
                version_tuple = self._parse_version(version_str)
                versions.append({
                    'version': version_tuple,
                    'feature': f"UNSPSC ({version_str})",
                    'code': code
                })
        
        if not versions:
            return None, None
        
        # Sort by version (LATEST first)
        versions.sort(key=lambda x: x['version'], reverse=True)
        
        latest = versions[0]
        return latest['feature'], latest['code']
    
    def _parse_version(self, version_str: str) -> Tuple[int, ...]:
        """
        Parse version string to tuple for comparison
        Examples:
        - "17.1001" -> (17, 1001)
        - "24.0701" -> (24, 701)
        """
        try:
            parts = version_str.split('.')
            return tuple(int(p) for p in parts)
        except:
            return (0,)

# ==================== STREAMLIT APP ====================

st.markdown("""
<div class="main-header">
    <h1>🔍 Swagelok UNSPSC Intelligence Platform</h1>
    <h3>FIXED VERSION - Extracts from Page Content</h3>
    <p>Extracts part number directly from product pages (not from URL parameter)</p>
</div>
""", unsafe_allow_html=True)

# Warning box
st.markdown("""
<div class="warning-box">
    <strong>⚠️ IMPORTANT - HOW THIS WORKS:</strong><br>
    This tool extracts the part number shown on the product page (from "Part #:" label).<br>
    It does NOT use the URL parameter or any other column in your file.<br>
    <br>
    <strong>Example:</strong><br>
    URL: https://www.swagelok.com/en/catalog/Product/Detail?part=T-200-SET<br>
    Part extracted: Whatever is shown on the page under "Part #:"<br>
    <br>
    If the page shows "Part #: T-200-SET", that's what you'll get in the output.
</div>
""", unsafe_allow_html=True)

# Sidebar
with st.sidebar:
    st.markdown("### 📖 About")
    st.info("""
    **This tool:**
    - Visits each product page
    - Finds "Part #:" label
    - Extracts complete part number
    - Gets latest UNSPSC version
    - No duplicates
    """)
    
    st.markdown("### 🎯 Priority Order")
    st.markdown("""
    1. "Part #:" label on page
    2. URL parameter
    3. Page title
    4. Meta tags
    """)
    
    st.markdown("---")
    st.caption("Created by: Abdelmoneim Moustafa")

# File upload
uploaded_file = st.file_uploader(
    "📤 Upload Excel file with URL column",
    type=["xlsx", "xls"],
    help="File must contain URLs to Swagelok products"
)

if uploaded_file:
    # Read file
    df = pd.read_excel(uploaded_file)
    
    # Auto-detect URL column
    url_column = None
    for col in df.columns:
        sample_values = df[col].astype(str).head(20)
        if sample_values.str.contains("http", case=False, na=False).any():
            url_column = col
            break
    
    if not url_column:
        st.error("❌ No URL column detected in file")
        st.stop()
    
    # Get unique URLs
    all_urls = df[url_column].dropna().astype(str).tolist()
    unique_urls = list(dict.fromkeys(all_urls))  # Preserve order, remove duplicates
    
    st.success(f"✅ File loaded! Detected URL column: **{url_column}**")
    
    # Stats
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("📊 Total URLs", len(all_urls))
    with col2:
        st.metric("⭐ Unique URLs", len(unique_urls))
    with col3:
        st.metric("🗑️ Duplicates", len(all_urls) - len(unique_urls))
    
    # Preview
    with st.expander("👁️ Preview URLs (first 10)"):
        preview_df = pd.DataFrame({url_column: unique_urls[:10]})
        st.dataframe(preview_df, use_container_width=True)
    
    st.markdown("---")
    
    # Extract button
    if st.button("🚀 Start Extraction (Fixed Algorithm)", type="primary", use_container_width=True):
        
        extractor = SwagelokExtractor()
        results = []
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        start_time = time.time()
        
        # Process URLs
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(extractor.extract, url) for url in unique_urls]
            
            for i, future in enumerate(as_completed(futures), 1):
                result = future.result()
                
                if result is not None:
                    results.append(result)
                
                # Update progress
                progress = i / len(unique_urls)
                progress_bar.progress(progress)
                
                elapsed = time.time() - start_time
                speed = i / elapsed if elapsed > 0 else 0
                remaining = int((len(unique_urls) - i) / speed) if speed > 0 else 0
                
                status_text.text(f"⚡ {i}/{len(unique_urls)} | Speed: {speed:.1f}/s | Remaining: {remaining}s")
        
        total_time = int(time.time() - start_time)
        
        # Create output dataframe
        output_df = pd.DataFrame(results)
        
        # Calculate stats
        parts_found = (output_df["Part"] != "Not Found").sum()
        unspsc_found = (output_df["UNSPSC Code"] != "Not Found").sum()
        
        # Success message
        st.markdown(f"""
        <div class="success-box">
            <h2>✅ Extraction Complete!</h2>
            <p>{len(results)} URLs processed in {total_time} seconds</p>
            <p>Parts: {parts_found} found | UNSPSC: {unspsc_found} found</p>
        </div>
        """, unsafe_allow_html=True)
        
        # Metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("✅ Parts Found", parts_found, f"{parts_found/len(results)*100:.1f}%")
        with col2:
            st.metric("✅ UNSPSC Found", unspsc_found, f"{unspsc_found/len(results)*100:.1f}%")
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
            file_name=f"swagelok_fixed_{int(time.time())}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )
        
        # Show results
        st.markdown("### 📋 Results Preview")
        st.dataframe(output_df, use_container_width=True, height=400)
        
        # Show sample comparisons
        with st.expander("🔍 Sample Extraction Details"):
            st.markdown("**First 5 results:**")
            for i, row in output_df.head(5).iterrows():
                st.markdown(f"""
                **{i+1}. URL:** {row['URL'][:60]}...  
                **Part Found:** `{row['Part']}`  
                **UNSPSC:** {row['UNSPSC Feature (Latest)']} = {row['UNSPSC Code']}
                ---
                """)

# Footer
st.markdown("---")
st.markdown("""
<div style="text-align: center; color: #666; padding: 1rem;">
    🎨 <strong>Designed & Developed by Abdelmoneim Moustafa</strong><br>
    Data Intelligence Engineer • Procurement Systems Expert<br>
    <small>© 2026 Swagelok UNSPSC Platform - FIXED VERSION</small>
</div>
""", unsafe_allow_html=True)
