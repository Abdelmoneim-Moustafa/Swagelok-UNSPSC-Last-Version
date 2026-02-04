"""
Swagelok UNSPSC Data Extraction Tool
Professional platform for procurement teams
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

# Configuration
MAX_WORKERS = 10
COMPANY_NAME = "Swagelok"
TIMEOUT = 12

# Page configuration
st.set_page_config(
    page_title="Swagelok Data Extraction",
    page_icon="📊",
    layout="centered"
)

# Clean professional styling
st.markdown("""
<style>
    .main {
        background-color: #f5f5f5;
    }
    .stButton>button {
        width: 100%;
        background-color: #0066cc;
        color: white;
        font-weight: 500;
        border: none;
        padding: 0.5rem 1rem;
        border-radius: 4px;
    }
    .stButton>button:hover {
        background-color: #0052a3;
    }
    h1 {
        color: #1a1a1a;
        font-weight: 600;
        margin-bottom: 0.5rem;
    }
    h3 {
        color: #4a4a4a;
        font-weight: 400;
        margin-top: 0;
    }
    .info-box {
        background-color: white;
        padding: 1.5rem;
        border-radius: 8px;
        border-left: 4px solid #0066cc;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

# Session state initialization
if 'processing' not in st.session_state:
    st.session_state.processing = False
if 'results' not in st.session_state:
    st.session_state.results = []
if 'progress' not in st.session_state:
    st.session_state.progress = 0
if 'total' not in st.session_state:
    st.session_state.total = 0
if 'start_time' not in st.session_state:
    st.session_state.start_time = 0
if 'completed' not in st.session_state:
    st.session_state.completed = False
if 'final_df' not in st.session_state:
    st.session_state.final_df = None

# Data extraction class
class DataExtractor:
    """Extracts product data from Swagelok product pages"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1"
        })
        
    def extract(self, url: str) -> Dict:
        """Extract product information from URL"""
        url = str(url).strip()
        
        result = {
            "Part": "Not Found",
            "Company": COMPANY_NAME,
            "URL": url,
            "UNSPSC Feature (Latest)": "Not Found",
            "UNSPSC Code": "Not Found"
        }
        
        if not (isinstance(url, str) and url.startswith("http") and "swagelok.com" in url.lower()):
            return result
        
        try:
            response = self.session.get(url, timeout=TIMEOUT, allow_redirects=True)
            
            if response.status_code != 200:
                return result
            
            soup = BeautifulSoup(response.text, "html.parser")
            html_text = response.text
            
            # Extract part number
            part = self._extract_part(soup, html_text, url)
            if part:
                result["Part"] = part
            
            # Extract UNSPSC information
            feature, code = self._extract_unspsc(soup, html_text)
            if feature and code:
                result["UNSPSC Feature (Latest)"] = feature
                result["UNSPSC Code"] = code
            
            return result
            
        except Exception:
            return result
    
    def _extract_part(self, soup, html_text, url) -> Optional[str]:
        """Extract part number using multiple methods"""
        
        # Method 1: Direct "Part #:" label (most reliable for Swagelok)
        patterns = [
            r'Part\s*#\s*:\s*([A-Z0-9][A-Z0-9.\-_/]+)',
            r'Part\s*Number\s*:\s*([A-Z0-9][A-Z0-9.\-_/]+)',
            r'Part\s*#:\s*([A-Z0-9][A-Z0-9.\-_/]+)',
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, html_text, re.IGNORECASE)
            for match in matches:
                clean_part = match.strip()
                if self._is_valid_part(clean_part):
                    return clean_part
        
        # Method 2: Breadcrumb navigation (last item often contains part number)
        breadcrumb_selectors = [
            'nav ol li', 
            '.breadcrumb li', 
            'nav[aria-label="breadcrumb"] li',
            'ol.breadcrumb li'
        ]
        
        for selector in breadcrumb_selectors:
            elements = soup.select(selector)
            if elements and len(elements) > 0:
                last_item = elements[-1].get_text(strip=True)
                if self._is_valid_part(last_item):
                    return last_item
        
        # Method 3: Page title
        title = soup.find('title')
        if title:
            title_text = title.get_text()
            part_match = re.search(r'\b([A-Z]{2,}[-\.][A-Z0-9\-\.]+)\b', title_text, re.IGNORECASE)
            if part_match and self._is_valid_part(part_match.group(1)):
                return part_match.group(1)
        
        # Method 4: H1 and H2 heading tags
        for heading in soup.find_all(['h1', 'h2'], limit=5):
            text = heading.get_text(strip=True)
            part_match = re.search(r'\b([A-Z]{2,}[-\.][A-Z0-9\-\.]+)\b', text, re.IGNORECASE)
            if part_match and self._is_valid_part(part_match.group(1)):
                return part_match.group(1)
        
        # Method 5: URL parameter extraction
        url_patterns = [
            r'[?&]part=([A-Z0-9.\-_/%]+)',
            r'/p/([A-Z0-9.\-_/%]+)',
            r'/part/([A-Z0-9.\-_/%]+)',
        ]
        
        for pattern in url_patterns:
            match = re.search(pattern, url, re.IGNORECASE)
            if match:
                part = match.group(1).replace('%2F', '/').replace('%252F', '/').strip()
                if self._is_valid_part(part):
                    return part
        
        # Method 6: Meta tags
        meta_tags = soup.find_all('meta', attrs={'name': re.compile(r'part|product', re.I)})
        for meta in meta_tags:
            content = meta.get('content', '')
            if self._is_valid_part(content):
                return content
        
        # Method 7: JSON-LD structured data
        scripts = soup.find_all('script', type='application/ld+json')
        for script in scripts:
            try:
                import json
                data = json.loads(script.string)
                if isinstance(data, dict):
                    sku = data.get('sku') or data.get('productID') or data.get('mpn')
                    if sku and self._is_valid_part(str(sku)):
                        return str(sku)
            except:
                pass
        
        # Method 8: Pattern search in all text
        all_text_matches = re.findall(r'\b([A-Z]{2,}[-\.][A-Z0-9\-\.]{2,})\b', html_text, re.IGNORECASE)
        for match in all_text_matches:
            if self._is_valid_part(match) and 4 <= len(match) <= 50:
                return match
        
        return None
    
    def _is_valid_part(self, part: str) -> bool:
        """Validate part number format for Swagelok products"""
        if not isinstance(part, str):
            return False
        
        part = part.strip()
        
        # Length check
        if not (3 <= len(part) <= 100):
            return False
        
        # Must contain letters or numbers
        has_letter = any(c.isalpha() for c in part)
        has_number = any(c.isdigit() for c in part)
        
        if not (has_letter or has_number):
            return False
        
        # Must have dash or dot (typical Swagelok format)
        if not ('-' in part or '.' in part):
            return False
        
        # Character validation
        if not re.match(r'^[A-Z0-9.\-_/]+$', part, re.IGNORECASE):
            return False
        
        # Exclude common non-part strings
        exclude_keywords = [
            'charset', 'utf-8', 'html', 'javascript', 'http', 'https',
            'www.', '.com', '.net', 'email', 'phone', 'address',
            'swagelok.com', 'product', 'catalog', 'lorem', 'ipsum'
        ]
        
        part_lower = part.lower()
        if any(keyword in part_lower for keyword in exclude_keywords):
            return False
        
        # Don't accept if it's just numbers and dashes (dates, etc.)
        if re.match(r'^[\d\-]+$', part):
            return False
        
        return True
    
    def _extract_unspsc(self, soup, html_text) -> Tuple[Optional[str], Optional[str]]:
        """Extract UNSPSC information using multiple methods"""
        versions = []
        
        # Method 1: Table extraction (most common in Swagelok pages)
        for row in soup.find_all('tr'):
            cells = row.find_all('td')
            if len(cells) >= 2:
                attr = cells[0].get_text(strip=True)
                val = cells[1].get_text(strip=True)
                
                # Look for UNSPSC with version number
                version_match = re.search(r'UNSPSC\s*\(([\d.]+)\)', attr, re.IGNORECASE)
                if version_match and re.match(r'^\d{6,8}$', val):
                    versions.append({
                        'version': self._parse_version(version_match.group(1)),
                        'feature': attr,
                        'code': val
                    })
        
        # Method 2: Text pattern matching
        if not versions:
            pattern = r'UNSPSC\s*\(([\d.]+)\)[^\d]*?(\d{6,8})'
            matches = re.findall(pattern, html_text, re.IGNORECASE)
            for version_str, code in matches:
                versions.append({
                    'version': self._parse_version(version_str),
                    'feature': f"UNSPSC ({version_str})",
                    'code': code
                })
        
        # Method 3: Meta tags
        if not versions:
            meta_tags = soup.find_all('meta', attrs={'name': re.compile(r'unspsc', re.I)})
            for meta in meta_tags:
                content = meta.get('content', '')
                code_match = re.search(r'\d{6,8}', content)
                if code_match:
                    versions.append({
                        'version': (99, 0),
                        'feature': "UNSPSC",
                        'code': code_match.group(0)
                    })
        
        # Method 4: JSON-LD structured data
        if not versions:
            scripts = soup.find_all('script', type='application/ld+json')
            for script in scripts:
                try:
                    import json
                    data = json.loads(script.string)
                    if isinstance(data, dict):
                        for key, value in data.items():
                            if 'unspsc' in key.lower():
                                code_match = re.search(r'\d{6,8}', str(value))
                                if code_match:
                                    versions.append({
                                        'version': (99, 0),
                                        'feature': "UNSPSC",
                                        'code': code_match.group(0)
                                    })
                except:
                    pass
        
        # Return the latest version
        if not versions:
            return None, None
        
        versions.sort(key=lambda x: x['version'], reverse=True)
        return versions[0]['feature'], versions[0]['code']
    
    def _parse_version(self, v: str) -> Tuple[int, ...]:
        """Parse version string to tuple for comparison"""
        try:
            parts = v.split('.')
            return tuple(int(p) for p in parts)
        except:
            return (0,)

# Header
st.title("Swagelok Data Extraction")
st.markdown("### UNSPSC Product Information Tool")

# About Swagelok
with st.expander("About Swagelok"):
    st.write("""
    **Swagelok Company** is a leading global manufacturer of fluid system solutions, serving critical industries worldwide.
    
    - **Founded:** 1947 in Ohio, USA
    - **Headquarters:** Solon, Ohio
    - **Revenue:** Approximately $2 billion annually
    - **Global Reach:** 200+ sales and service centers in 70 countries
    - **Employees:** 5,700+ associates at 20 manufacturing facilities
    
    **Products:** Tube fittings, valves, regulators, hoses, filters, gauges, and engineered assemblies
    
    **Industries Served:** Oil & gas, chemical processing, semiconductor manufacturing, clean energy, transportation, power generation, and research & development
    """)

# File upload
uploaded_file = st.file_uploader("Upload Excel File", type=["xlsx", "xls"])

if uploaded_file:
    df = pd.read_excel(uploaded_file)
    
    # Find URL column
    url_column = None
    for col in df.columns:
        if df[col].astype(str).str.contains("http", na=False, case=False).any():
            url_column = col
            break
    
    if not url_column:
        st.error("No URL column found in the uploaded file.")
        st.stop()
    
    all_urls = df[url_column].dropna().astype(str).tolist()
    
    st.success(f"File loaded successfully. Found {len(all_urls)} URLs in column: {url_column}")
    
    # Display metrics
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total URLs", len(all_urls))
    with col2:
        st.metric("Workers", MAX_WORKERS)
    with col3:
        st.metric("Timeout", f"{TIMEOUT}s")
    
    # Processing status
    if st.session_state.processing:
        st.info("Processing in progress...")
        
        if st.session_state.total > 0:
            progress_pct = st.session_state.progress / st.session_state.total
            st.progress(progress_pct)
            
            elapsed = time.time() - st.session_state.start_time
            speed = st.session_state.progress / elapsed if elapsed > 0 else 0
            remaining = (st.session_state.total - st.session_state.progress) / speed if speed > 0 else 0
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Progress", f"{st.session_state.progress}/{st.session_state.total}")
            with col2:
                st.metric("Speed", f"{speed:.1f}/s")
            with col3:
                st.metric("Time Remaining", f"{int(remaining)}s")
    
    # Completed results
    if st.session_state.completed and st.session_state.final_df is not None:
        total_time = int(time.time() - st.session_state.start_time) if st.session_state.start_time > 0 else 0
        
        parts_found = (st.session_state.final_df["Part"] != "Not Found").sum()
        unspsc_found = (st.session_state.final_df["UNSPSC Code"] != "Not Found").sum()
        
        st.success(f"Processing complete. Total time: {total_time} seconds")
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Parts Found", parts_found)
        with col2:
            st.metric("UNSPSC Found", unspsc_found)
        with col3:
            st.metric("Total Time", f"{total_time}s")
        with col4:
            st.metric("Avg Speed", f"{len(st.session_state.final_df)/max(total_time, 1):.1f}/s")
        
        # Download button
        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            st.session_state.final_df.to_excel(writer, index=False, sheet_name="Results")
        
        st.download_button(
            label=f"Download Results ({len(st.session_state.final_df)} rows)",
            data=buffer.getvalue(),
            file_name=f"swagelok_results_{int(time.time())}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        
        # Results preview
        st.subheader("Results Preview")
        st.dataframe(st.session_state.final_df, use_container_width=True, height=400)
    
    # Start button
    if not st.session_state.processing and not st.session_state.completed:
        if st.button("Start Extraction"):
            st.session_state.processing = True
            st.session_state.completed = False
            st.session_state.results = []
            st.session_state.progress = 0
            st.session_state.total = len(all_urls)
            st.session_state.start_time = time.time()
            
            extractor = DataExtractor()
            
            progress_bar = st.progress(0)
            status = st.empty()
            
            start_time = time.time()
            
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {executor.submit(extractor.extract, url): url for url in all_urls}
                
                results = []
                for i, future in enumerate(as_completed(futures), 1):
                    try:
                        result = future.result(timeout=30)
                        results.append(result)
                    except Exception:
                        url = futures[future]
                        results.append({
                            "Part": "Not Found",
                            "Company": COMPANY_NAME,
                            "URL": url,
                            "UNSPSC Feature (Latest)": "Not Found",
                            "UNSPSC Code": "Not Found"
                        })
                    
                    # Update progress
                    progress = i / len(all_urls)
                    progress_bar.progress(progress)
                    
                    st.session_state.progress = i
                    
                    elapsed = time.time() - start_time
                    speed = i / elapsed if elapsed > 0 else 0
                    remaining = int((len(all_urls) - i) / speed) if speed > 0 else 0
                    
                    status.write(f"Processing: {i}/{len(all_urls)} | Speed: {speed:.1f}/s | Remaining: {remaining}s")
            
            st.session_state.processing = False
            st.session_state.completed = True
            st.session_state.final_df = pd.DataFrame(results)
            
            st.rerun()
    
    # Reset button
    if st.session_state.completed:
        if st.button("Process New File"):
            st.session_state.processing = False
            st.session_state.completed = False
            st.session_state.results = []
            st.session_state.progress = 0
            st.session_state.total = 0
            st.session_state.start_time = 0
            st.session_state.final_df = None
            st.rerun()

# Footer
st.markdown("---")
st.markdown("""
<div style="text-align: center; color: #666; padding: 1rem;">
    <p>Swagelok UNSPSC Data Extraction Tool</p>
    <p style="font-size: 0.9rem;">Developed by Abdelmoneim Moustafa | Data Intelligence Engineer</p>
</div>
""", unsafe_allow_html=True)
