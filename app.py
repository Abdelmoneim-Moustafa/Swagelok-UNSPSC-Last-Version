"""
🔍 Swagelok UNSPSC Extractor 

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
from typing import Dict, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
from pathlib import Path

# ==================== CONFIG ====================
MAX_WORKERS = 10  # Fast parallel processing
TIMEOUT = 15
BATCH_SIZE = 50
COMPANY_NAME = "Swagelok"

# Save directory
SAVE_DIR = Path("swagelok_data")
SAVE_DIR.mkdir(exist_ok=True)

# ==================== PAGE CONFIG ====================
st.set_page_config(
    page_title="Swagelok UNSPSC Extractor",
    page_icon="🔍",
    layout="wide"
)

# ==================== THEME CSS ====================
st.markdown("""
<style>
    :root {
        --info-bg: #e3f2fd; --success-bg: #e8f5e9; --warning-bg: #fff3e0;
        --card-bg: #ffffff; --border: #e0e0e0; --text: #333333;
    }
    @media (prefers-color-scheme: dark) {
        :root {
            --info-bg: #1a237e; --success-bg: #1b5e20; --warning-bg: #e65100;
            --card-bg: #1e1e1e; --border: #424242; --text: #e0e0e0;
        }
    }
    [data-theme="dark"] {
        --info-bg: #1a237e; --success-bg: #1b5e20; --warning-bg: #e65100;
        --card-bg: #1e1e1e; --border: #424242; --text: #e0e0e0;
    }
    .main-header {
        background: linear-gradient(135deg, #667eea, #764ba2);
        padding: 2.5rem; border-radius: 15px; color: white;
        text-align: center; margin-bottom: 2rem;
        box-shadow: 0 8px 20px rgba(102,126,234,0.3);
    }
    .info-box, .warning-box, .progress-card {
        padding: 1.5rem; border-radius: 10px; margin: 1rem 0; color: var(--text);
    }
    .info-box { background: var(--info-bg); border-left: 5px solid #2196f3; }
    .warning-box { background: var(--warning-bg); border-left: 5px solid #ff9800; }
    .success-box {
        background: linear-gradient(135deg, #11998e, #38ef7d);
        padding: 2rem; border-radius: 15px; color: white;
        text-align: center; margin: 1.5rem 0;
        box-shadow: 0 8px 20px rgba(17,153,142,0.3);
    }
    .progress-card {
        background: var(--card-bg); border: 1px solid var(--border);
        border-radius: 12px;
    }
</style>
""", unsafe_allow_html=True)

# ==================== DATA STORAGE ====================
class DataStorage:
    def __init__(self, session_id: str):
        self.session_id = session_id
        self.data_file = SAVE_DIR / f"{session_id}.jsonl"
        self.progress_file = SAVE_DIR / f"{session_id}_progress.json"
    
    def save_row(self, row: Dict):
        with open(self.data_file, 'a', encoding='utf-8') as f:
            f.write(json.dumps(row, ensure_ascii=False) + '\n')
    
    def save_progress(self, info: Dict):
        with open(self.progress_file, 'w', encoding='utf-8') as f:
            json.dump(info, f)
    
    def load_all(self):
        if not self.data_file.exists():
            return []
        data = []
        with open(self.data_file, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    data.append(json.loads(line))
        return data
    
    def clear(self):
        if self.data_file.exists():
            self.data_file.unlink()
        if self.progress_file.exists():
            self.progress_file.unlink()

# ==================== SIMPLE EXTRACTOR ====================
class SimpleExtractor:
    """Fast extractor using only requests + BeautifulSoup"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
        })
    
    def extract(self, url: str, row_num: int) -> Dict:
        """Extract Part and UNSPSC from URL"""
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
            result["Status"] = "Invalid"
            result["Error"] = "Invalid URL"
            return result
        
        try:
            # Fetch page
            response = self.session.get(url, timeout=TIMEOUT)
            
            if response.status_code != 200:
                result["Status"] = f"HTTP {response.status_code}"
                result["Error"] = f"Status code {response.status_code}"
                return result
            
            soup = BeautifulSoup(response.text, 'html.parser')
            html_text = response.text
            
            # Extract part
            part = self._extract_part(soup, html_text, url)
            if part:
                result["Part"] = part
            else:
                result["Error"] = "Part not found"
            
            # Extract UNSPSC
            feature, code = self._extract_unspsc(soup, html_text)
            if feature and code:
                result["UNSPSC Feature (Latest)"] = feature
                result["UNSPSC Code"] = code
            else:
                if result["Error"]:
                    result["Error"] += "; UNSPSC not found"
                else:
                    result["Error"] = "UNSPSC not found"
            
            return result
            
        except requests.Timeout:
            result["Status"] = "Timeout"
            result["Error"] = "Request timed out"
            return result
        except requests.ConnectionError:
            result["Status"] = "Connection Error"
            result["Error"] = "Cannot connect to server"
            return result
        except Exception as e:
            result["Status"] = "Error"
            result["Error"] = str(e)[:100]
            return result
    
    def _extract_part(self, soup, html_text, url) -> Optional[str]:
        """Extract part number"""
        
        # Method 1: From H1/H2
        for tag in ['h1', 'h2']:
            elem = soup.find(tag)
            if elem:
                text = elem.get_text(strip=True)
                # Extract first part-like string
                match = re.search(r'^([A-Z0-9][A-Z0-9.\-_/]+)', text, re.IGNORECASE)
                if match and self._valid_part(match.group(1)):
                    return match.group(1).strip()
        
        # Method 2: Search for "Part #:" in HTML
        patterns = [
            r'Part\s*#\s*:?\s*([A-Z0-9][A-Z0-9.\-_/]{1,50})',
            r'Part\s+Number\s*:?\s*([A-Z0-9][A-Z0-9.\-_/]{1,50})',
        ]
        for pattern in patterns:
            matches = re.findall(pattern, html_text, re.IGNORECASE)
            for match in matches:
                if self._valid_part(match):
                    return match.strip()
        
        # Method 3: From URL parameter
        url_patterns = [
            r'part=([A-Z0-9.\-_/%]+)',
            r'/p/([A-Z0-9.\-_/%]+)',
        ]
        for pattern in url_patterns:
            match = re.search(pattern, url, re.IGNORECASE)
            if match:
                part = match.group(1).replace('%2F', '/').replace('%252F', '/')
                if self._valid_part(part):
                    return part.strip()
        
        return None
    
    def _extract_unspsc(self, soup, html_text) -> Tuple[Optional[str], Optional[str]]:
        """
        Extract UNSPSC code - takes LAST occurrence of highest version
        """
        all_unspsc = []
        
        # Method 1: Parse tables
        tables = soup.find_all('table')
        for table in tables:
            rows = table.find_all('tr')
            for idx, row in enumerate(rows):
                cells = row.find_all('td')
                if len(cells) >= 2:
                    attr = cells[0].get_text(strip=True)
                    val = cells[1].get_text(strip=True)
                    
                    # Only UNSPSC rows
                    if not attr.upper().startswith('UNSPSC'):
                        continue
                    
                    # Extract version and code
                    version_match = re.search(r'UNSPSC\s*\(([0-9.]+)\)', attr, re.IGNORECASE)
                    if version_match and re.match(r'^\d{6,8}$', val):
                        v_str = version_match.group(1)
                        v_tuple = tuple(map(int, v_str.split('.')))
                        all_unspsc.append({
                            'version': v_tuple,
                            'feature': attr,
                            'code': val,
                            'order': idx
                        })
        
        # Method 2: Regex on full HTML
        if not all_unspsc:
            for idx, (v_str, code) in enumerate(re.findall(r'UNSPSC\s*\(([0-9.]+)\)[^\d]*?(\d{6,8})', html_text, re.IGNORECASE)):
                v_tuple = tuple(map(int, v_str.split('.')))
                all_unspsc.append({
                    'version': v_tuple,
                    'feature': f"UNSPSC ({v_str})",
                    'code': code,
                    'order': idx
                })
        
        if not all_unspsc:
            return None, None
        
        # Get highest version
        max_version = max(e['version'] for e in all_unspsc)
        highest = [e for e in all_unspsc if e['version'] == max_version]
        
        # Return LAST occurrence
        last = max(highest, key=lambda x: x['order'])
        return last['feature'], last['code']
    
    def _valid_part(self, part: str) -> bool:
        """Validate part number"""
        if not isinstance(part, str) or not (2 <= len(part) <= 100):
            return False
        has_alpha = any(c.isalpha() for c in part)
        has_digit = any(c.isdigit() for c in part)
        if not (has_alpha or (has_digit and len(part) > 3)):
            return False
        exclude = ['charset', 'utf', 'html', 'http', 'www', 'catalog', 'product', 'detail']
        return not any(ex in part.lower() for ex in exclude)

# ==================== UI ====================

st.markdown("""
<div class="main-header" style="padding:2rem; border-radius:12px; text-align:center;
            background: linear-gradient(135deg,#1e3a8a,#2563eb); color: #ffffff;">
  <h1 style="margin-bottom:0.5rem;">🔎 Swagelok UNSPSC Extractor</h1>
  <p style="font-size:1.05rem; margin:0; opacity:0.95;">
    Intelligent and automated extraction of accurate UNSPSC codes — fast, reliable, and production-ready.
  </p>
</div>
""", unsafe_allow_html=True)

st.markdown("""
<div style="margin-top:1.2rem; padding:1.5rem; border-radius:10px;
            background:#f8fafc; border:1px solid #e2e8f0; color:#0f172a;">
  <h4 style="margin-top:0;">✨ Key Benefits</h4>
  <ul style="line-height:1.6;">
    <li><strong>High Accuracy:</strong> Extracts the correct and latest UNSPSC classification directly from official product data.</li>
    <li><strong>Reliable Processing:</strong> Designed for stable performance across large datasets.</li>
    <li><strong>Automated Workflow:</strong> Upload your file and receive structured results instantly.</li>
    <li><strong>Secure Handling:</strong> Data is processed safely within the session.</li>
    <li><strong>Optimized Performance:</strong> Built for speed without compromising quality.</li>
  </ul>
</div>
""", unsafe_allow_html=True)

with st.sidebar:
    st.markdown("### ⚙️ Processing Overview")
    st.info("""
    • Automated product analysis  
    • Structured UNSPSC output  
    • Ready for export and integration  
    """)
    
    st.markdown("---")
    st.caption("Powered by Intelligent Data Automation")


uploaded_file = st.file_uploader("📤 Upload Excel File", type=["xlsx", "xls"])

if uploaded_file:
    try:
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
        
        st.success(f"✅ URL column: **{url_column}**")
        
        # Get URLs
        urls = [str(x).strip() if pd.notna(x) and str(x).strip() else None for x in df[url_column]]
        valid_urls = [u for u in urls if u and u.startswith('http')]
        
        col1, col2, col3 = st.columns(3)
        col1.metric("📊 Total", len(urls))
        col2.metric("✅ Valid", len(valid_urls))
        col3.metric("⏱️ Est.", f"~{int(len(valid_urls)/(MAX_WORKERS*2)/60)}m")
        
        with st.expander("👁️ Preview"):
            st.dataframe(pd.DataFrame({"Row": range(1, 6), url_column: [u or "Empty" for u in urls[:5]]}))
        
        st.markdown("---")
        
        if st.button("🚀 Start Fast Extraction", type="primary", use_container_width=True):
            
            session_id = f"session_{int(time.time())}"
            storage = DataStorage(session_id)
            
            extractor = SimpleExtractor()
            all_results = []
            
            progress_bar = st.progress(0)
            status_container = st.empty()
            download_container = st.empty()
            
            start_time = time.time()
            
            # Process in batches with parallel workers
            num_batches = (len(valid_urls) + BATCH_SIZE - 1) // BATCH_SIZE
            
            for batch_idx in range(num_batches):
                batch_start = batch_idx * BATCH_SIZE
                batch_end = min((batch_idx + 1) * BATCH_SIZE, len(valid_urls))
                batch_urls = valid_urls[batch_start:batch_end]
                
                batch_results = []
                
                # Parallel processing
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    future_to_url = {}
                    for i, url in enumerate(batch_urls):
                        row_num = batch_start + i + 1
                        future = executor.submit(extractor.extract, url, row_num)
                        future_to_url[future] = (url, row_num)
                    
                    for future in as_completed(future_to_url):
                        try:
                            result = future.result(timeout=TIMEOUT + 5)
                            batch_results.append(result)
                            storage.save_row(result)
                        except:
                            url, row_num = future_to_url[future]
                            error_result = {
                                "Row": row_num,
                                "Part": "Not Found",
                                "Company": COMPANY_NAME,
                                "URL": url,
                                "UNSPSC Feature (Latest)": "Not Found",
                                "UNSPSC Code": "Not Found",
                                "Status": "Error",
                                "Error": "Processing failed"
                            }
                            batch_results.append(error_result)
                            storage.save_row(error_result)
                        
                        # Update progress
                        total_done = len(all_results) + len(batch_results)
                        progress_bar.progress(min(total_done / len(valid_urls), 1.0))
                        
                        elapsed = time.time() - start_time
                        speed = total_done / elapsed if elapsed > 0 else 0
                        remaining = int((len(valid_urls) - total_done) / speed) if speed > 0 else 0
                        
                        status_container.markdown(f"""
                        <div class="progress-card">
                            <strong>{total_done}/{len(valid_urls)}</strong> • Speed: <strong>{speed:.1f}/s</strong><br>
                            Remaining: {remaining//60}m {remaining%60}s
                        </div>
                        """, unsafe_allow_html=True)
                
                all_results.extend(batch_results)
                storage.save_progress({'completed': len(all_results), 'total': len(valid_urls)})
                
                # Offer download
                if (batch_idx + 1) % 2 == 0 or batch_idx == num_batches - 1:
                    current_df = pd.DataFrame(all_results)
                    output_df = current_df[["Part", "Company", "URL", "UNSPSC Feature (Latest)", "UNSPSC Code"]]
                    
                    buf = BytesIO()
                    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
                        output_df.to_excel(writer, index=False)
                    
                    download_container.download_button(
                        f"💾 Download Progress ({len(all_results)} rows)",
                        buf.getvalue(),
                        f"progress_{len(all_results)}.xlsx",
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key=f"dl_{batch_idx}"
                    )
            
            # Final results
            total_time = int(time.time() - start_time)
            final_df = pd.DataFrame(all_results)
            output_df = final_df[["Part", "Company", "URL", "UNSPSC Feature (Latest)", "UNSPSC Code"]]
            
            parts_found = (final_df["Part"] != "Not Found").sum()
            unspsc_found = (final_df["UNSPSC Code"] != "Not Found").sum()
            success = (final_df["Status"] == "Success").sum()
            
            st.markdown(f"""
            <div class="success-box">
                <h2>✅ Complete!</h2>
                <p><strong>Processed:</strong> {len(final_df)} rows in {total_time//60}m {total_time%60}s</p>
                <p><strong>Speed:</strong> {len(final_df)/total_time:.1f} URLs/second</p>
                <p><strong>Success:</strong> {success} ({success/len(final_df)*100:.1f}%)</p>
                <p><strong>Parts:</strong> {parts_found} | <strong>UNSPSC:</strong> {unspsc_found}</p>
            </div>
            """, unsafe_allow_html=True)
            
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("✅ Success", success)
            col2.metric("✅ Parts", parts_found)
            col3.metric("✅ UNSPSC", unspsc_found)
            col4.metric("⚡ Speed", f"{len(final_df)/total_time:.1f}/s")
            
            buf = BytesIO()
            with pd.ExcelWriter(buf, engine="openpyxl") as writer:
                output_df.to_excel(writer, index=False)
            
            st.download_button(
                "📥 Download Final Results",
                buf.getvalue(),
                f"swagelok_final_{int(time.time())}.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
            
            st.markdown("### 📋 Results Preview")
            st.dataframe(output_df.head(20), use_container_width=True)
            
            errors = final_df[final_df["Status"] != "Success"]
            if len(errors) > 0:
                with st.expander(f"⚠️ Errors ({len(errors)})"):
                    st.dataframe(errors[["Row", "URL", "Status", "Error"]])
    
    except Exception as e:
        st.error(f"❌ Error: {str(e)}")
        st.exception(e)

st.markdown("---")
st.markdown("""
<div style="text-align: center; padding: 2rem;">
    <p style="font-size: 1.2rem; font-weight: 600;">🎨 Abdelmoneim Moustafa</p>
    <p>Data Intelligence Engineer</p>
    <p style="font-size: 0.9rem; margin-top: 1rem; opacity: 0.7;">© 2025 Swagelok UNSPSC Platform</p>
</div>
""", unsafe_allow_html=True)
