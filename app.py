"""
🔍 Swagelok UNSPSC Intelligence Platform - FIXED VERSION
Completely rewritten from scratch

FIXES:
- ✅ Extracts complete part number from "Part #:" label
- ✅ Gets LATEST UNSPSC version (highest version number)
- ✅ No duplicates (uses URL as unique key)
- ✅ Validates all data before output
- ✅ Clean, fast, reliable

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

# ==================== EXTRACTOR (COMPLETELY REWRITTEN) ====================
class SwagelokUNSPSCExtractor:
    """
    Complete rewrite with focus on:
    1. Accurate Part # extraction
    2. Latest UNSPSC version detection
    3. Data validation
    4. No duplicates
    """
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        })
        self.processed_urls = set()  # Track processed URLs to avoid duplicates
    
    def extract(self, url: str) -> Dict:
        """Main extraction method with validation"""
        
        # Initialize result
        result = {
            "Part": "Not Found",
            "Company": COMPANY_NAME,
            "URL": url,
            "UNSPSC Feature (Latest)": "Not Found",
            "UNSPSC Code": "Not Found"
        }
        
        # Validate URL
        if not self._is_valid_url(url):
            return result
        
        # Check for duplicates
        if url in self.processed_urls:
            return None  # Will be filtered out
        
        try:
            # Fetch page
            response = self.session.get(url, timeout=30)
            
            if response.status_code != 200:
                return result
            
            # Parse HTML
            soup = BeautifulSoup(response.text, "html.parser")
            html_text = response.text
            
            # ===== EXTRACT PART NUMBER (FIXED) =====
            part_number = self._extract_part_number(soup, html_text, url)
            if part_number and part_number != "Not Found":
                result["Part"] = part_number
            
            # ===== EXTRACT LATEST UNSPSC (FIXED) =====
            unspsc_feature, unspsc_code = self._extract_latest_unspsc(soup, html_text)
            if unspsc_feature and unspsc_code:
                result["UNSPSC Feature (Latest)"] = unspsc_feature
                result["UNSPSC Code"] = unspsc_code
            
            # Mark URL as processed
            self.processed_urls.add(url)
            
            return result
            
        except Exception as e:
            print(f"Error processing {url}: {str(e)}")
            return result
    
    def _is_valid_url(self, url: str) -> bool:
        """Validate URL format"""
        if not isinstance(url, str):
            return False
        if not url.startswith("http"):
            return False
        if "swagelok.com" not in url.lower():
            return False
        return True
    
    def _extract_part_number(self, soup: BeautifulSoup, html_text: str, url: str) -> str:
        """
        Extract complete part number using multiple strategies
        Priority: Part #: label > URL > Title
        """
        
        # ========================================
        # STRATEGY 1: Find "Part #:" label (MOST RELIABLE)
        # ========================================
        # Look for exact pattern: "Part #:" followed by the part number
        part_pattern = r'Part\s*#\s*:\s*([A-Z0-9][A-Z0-9.\-_%]+)'
        matches = re.findall(part_pattern, html_text, re.IGNORECASE)
        
        for match in matches:
            # Clean the match
            part = match.strip()
            # Validate it looks like a real part number
            if self._is_valid_part_number(part):
                return part
        
        # ========================================
        # STRATEGY 2: Search in specific HTML elements
        # ========================================
        # Look for <dt> or <strong> tags containing "Part #"
        for tag in soup.find_all(['dt', 'strong', 'span', 'div']):
            text = tag.get_text()
            if re.search(r'Part\s*#', text, re.IGNORECASE):
                # Get the next sibling or parent's text
                next_element = tag.find_next_sibling()
                if next_element:
                    part = next_element.get_text(strip=True)
                    if self._is_valid_part_number(part):
                        return part
                # Or check if the part is in the same tag
                match = re.search(r'Part\s*#\s*:\s*([A-Z0-9.\-_%]+)', text)
                if match and self._is_valid_part_number(match.group(1)):
                    return match.group(1)
        
        # ========================================
        # STRATEGY 3: Extract from URL path (BACKUP)
        # ========================================
        # URL format: .../p/PART-NUMBER or ...part=PART-NUMBER
        url_patterns = [
            r'/p/([A-Z0-9.\-_%]+)',
            r'part=([A-Z0-9.\-_%]+)',
        ]
        
        for pattern in url_patterns:
            match = re.search(pattern, url, re.IGNORECASE)
            if match:
                part = match.group(1)
                # URL decode
                part = part.replace('%2F', '/').replace('%252F', '/')
                if self._is_valid_part_number(part):
                    return part
        
        # ========================================
        # STRATEGY 4: Page title
        # ========================================
        title_tag = soup.find('title')
        if title_tag:
            title = title_tag.get_text()
            # Extract part number from title
            match = re.search(r'([A-Z0-9]+[.\-][A-Z0-9.\-]+)', title)
            if match and self._is_valid_part_number(match.group(1)):
                return match.group(1)
        
        return "Not Found"
    
    def _is_valid_part_number(self, part: str) -> bool:
        """
        Validate if string is a real part number
        Rules:
        - Length 3-100 characters
        - Contains letters AND numbers
        - Not a common HTML/CSS term
        - Not just "charset" or "utf-8"
        """
        if not isinstance(part, str):
            return False
        
        part = part.strip()
        
        # Length check
        if not (3 <= len(part) <= 100):
            return False
        
        # Must have at least one letter AND one number
        has_letter = any(c.isalpha() for c in part)
        has_number = any(c.isdigit() for c in part)
        
        if not (has_letter and has_number):
            return False
        
        # Exclude common false positives
        exclude_terms = [
            'charset', 'utf', 'html', 'text', 'http', 'www',
            'content-type', 'application', 'javascript', 'css'
        ]
        
        part_lower = part.lower()
        for term in exclude_terms:
            if term in part_lower:
                return False
        
        return True
    
    def _extract_latest_unspsc(self, soup: BeautifulSoup, html_text: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Extract LATEST UNSPSC version from specifications table
        
        Table format:
        Attribute          | Value
        UNSPSC (4.03)      | 23171515
        UNSPSC (17.1001)   | 39120000  <- We want the LATEST (highest version)
        """
        
        unspsc_versions = []
        
        # ========================================
        # METHOD 1: Parse specification table
        # ========================================
        for row in soup.find_all('tr'):
            cells = row.find_all('td')
            
            if len(cells) >= 2:
                attribute = cells[0].get_text(strip=True)
                value = cells[1].get_text(strip=True)
                
                # Check if this is UNSPSC row
                version_match = re.search(r'UNSPSC\s*\(([\d.]+)\)', attribute, re.IGNORECASE)
                
                # Validate code (6-8 digits)
                if version_match and re.match(r'^\d{6,8}$', value):
                    version_str = version_match.group(1)
                    version_tuple = self._parse_version(version_str)
                    
                    unspsc_versions.append({
                        'version_tuple': version_tuple,
                        'version_str': version_str,
                        'feature': attribute,
                        'code': value
                    })
        
        # ========================================
        # METHOD 2: Regex fallback (if table parsing fails)
        # ========================================
        if not unspsc_versions:
            # Pattern: UNSPSC (version) followed by 6-8 digit code
            pattern = r'UNSPSC\s*\(([\d.]+)\)[^\d]*?(\d{6,8})'
            matches = re.findall(pattern, html_text, re.IGNORECASE)
            
            for version_str, code in matches:
                version_tuple = self._parse_version(version_str)
                unspsc_versions.append({
                    'version_tuple': version_tuple,
                    'version_str': version_str,
                    'feature': f"UNSPSC ({version_str})",
                    'code': code
                })
        
        # ========================================
        # SELECT LATEST VERSION
        # ========================================
        if not unspsc_versions:
            return None, None
        
        # Sort by version (highest first)
        unspsc_versions.sort(key=lambda x: x['version_tuple'], reverse=True)
        
        latest = unspsc_versions[0]
        return latest['feature'], latest['code']
    
    def _parse_version(self, version_str: str) -> Tuple[int, ...]:
        """
        Parse version string to tuple for comparison
        Examples:
        - "17.1001" -> (17, 1001)
        - "4.03" -> (4, 3)
        - "20.0601" -> (20, 601)
        """
        try:
            parts = version_str.split('.')
            return tuple(int(p) for p in parts)
        except:
            return (0,)

# ==================== STREAMLIT UI ====================
st.set_page_config(
    page_title="Swagelok UNSPSC Platform - Fixed",
    page_icon="🔍",
    layout="wide"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(135deg, #667eea, #764ba2);
        padding: 2rem;
        border-radius: 12px;
        color: white;
        text-align: center;
        margin-bottom: 2rem;
        box-shadow: 0 8px 20px rgba(102,126,234,0.3);
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
        background: #fff3cd;
        border-left: 4px solid #ffc107;
        padding: 1rem;
        border-radius: 5px;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

# Header
st.markdown("""
<div class="main-header">
    <h1>🔍 Swagelok UNSPSC Intelligence Platform</h1>
    <h3>FIXED VERSION - Complete Part Numbers • Latest UNSPSC • No Duplicates</h3>
    <p>Rewritten from scratch for maximum accuracy</p>
</div>
""", unsafe_allow_html=True)

# Important notes
st.markdown("""
<div class="warning-box">
    <strong>✅ WHAT'S FIXED:</strong><br>
    • Complete part numbers (e.g., "CWS-C.040-.405-P" not just "CWS-C")<br>
    • Latest UNSPSC version (highest version number from table)<br>
    • No duplicate URLs processed<br>
    • Proper validation of all data<br>
    • Each row is unique
</div>
""", unsafe_allow_html=True)

# File upload
uploaded_file = st.file_uploader(
    "📤 Upload Excel file with URL column",
    type=["xlsx", "xls"],
    help="File must contain Swagelok product URLs"
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
        st.error("❌ No URL column detected")
        st.stop()
    
    # Get unique URLs (remove duplicates from input)
    urls = df[url_column].dropna().drop_duplicates().tolist()
    
    st.success(f"✅ Detected URL column: **{url_column}**")
    
    # Show stats
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("📊 Total URLs", len(df[url_column]))
    with col2:
        st.metric("⭐ Unique URLs", len(urls))
    with col3:
        st.metric("🗑️ Duplicates Removed", len(df[url_column]) - len(urls))
    
    # Preview
    with st.expander("🔍 Preview URLs"):
        st.dataframe(pd.DataFrame({url_column: urls[:10]}), use_container_width=True)
    
    st.markdown("---")
    
    # Extract button
    if st.button("🚀 Start Extraction ", use_container_width=True, type="primary"):
        
        extractor = SwagelokUNSPSCExtractor()
        results = []
        
        progress_bar = st.progress(0.0)
        status = st.empty()
        
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(extractor.extract, url) for url in urls]
            
            for i, future in enumerate(as_completed(futures), 1):
                result = future.result()
                
                # Only add non-None results (filters out duplicates)
                if result is not None:
                    results.append(result)
                
                # Update progress
                progress = i / len(urls)
                progress_bar.progress(progress)
                
                elapsed = time.time() - start_time
                speed = i / elapsed if elapsed > 0 else 0
                remaining = int((len(urls) - i) / speed) if speed > 0 else 0
                
                status.markdown(f"""
                **Processing:** {i}/{len(urls)} URLs  
                **Speed:** {speed:.1f} URLs/sec  
                **Remaining:** ~{remaining}s
                """)
        
        total_time = int(time.time() - start_time)
        
        # Create output dataframe
        output_df = pd.DataFrame(results)
        
        # Calculate statistics
        parts_found = (output_df["Part"] != "Not Found").sum()
        unspsc_found = (output_df["UNSPSC Code"] != "Not Found").sum()
        
        # Success message
        st.markdown(f"""
        <div class="success-box">
            <h2>✅ Extraction Complete!</h2>
            <p>Processed {len(results)} unique URLs in {total_time} seconds</p>
            <p>Parts: {parts_found} found • UNSPSC: {unspsc_found} found</p>
        </div>
        """, unsafe_allow_html=True)
        
        # Summary metrics
        st.markdown("### 📊 Results Summary")
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
            output_df.to_excel(writer, index=False, sheet_name="UNSPSC Data")
        
        st.download_button(
            label="📥 Download Results (Excel)",
            data=buffer.getvalue(),
            file_name=f"swagelok_unspsc_fixed_{int(time.time())}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )
        
        # Preview results
        st.markdown("### 📋 Results Preview")
        st.dataframe(output_df, use_container_width=True, height=400)
        
        # Detailed analysis
        with st.expander("📈 Detailed Analysis"):
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**Part Number Extraction:**")
                st.write(f"- Found: {parts_found}")
                st.write(f"- Not found: {len(results) - parts_found}")
                st.write(f"- Success rate: {parts_found/len(results)*100:.1f}%")
                
                # Show sample of found parts
                found_parts = output_df[output_df["Part"] != "Not Found"]["Part"].head(5).tolist()
                st.write(f"\n**Sample parts found:**")
                for p in found_parts:
                    st.write(f"  • {p}")
            
            with col2:
                st.markdown("**UNSPSC Extraction:**")
                st.write(f"- Found: {unspsc_found}")
                st.write(f"- Not found: {len(results) - unspsc_found}")
                st.write(f"- Success rate: {unspsc_found/len(results)*100:.1f}%")
                
                # Show unique UNSPSC versions found
                versions = output_df[output_df["UNSPSC Feature (Latest)"] != "Not Found"]["UNSPSC Feature (Latest)"].unique()
                st.write(f"\n**UNSPSC versions found:**")
                for v in versions[:5]:
                    st.write(f"  • {v}")

# Footer
st.markdown("---")
st.markdown("""
<div style="text-align: center; color: #666; padding: 2rem;">
    🎨 <strong style="color: #667eea;">Designed & Developed by Abdelmoneim Moustafa</strong><br>
    Data Intelligence Engineer • Automation Specialist • Procurement Systems Expert<br>
    <small>© 2025 Swagelok UNSPSC Intelligence Platform - FIXED VERSION</small>
</div>
""", unsafe_allow_html=True)
