import re
import time
import pandas as pd
import requests
import streamlit as st
from bs4 import BeautifulSoup
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed

MAX_WORKERS = 12

# ==================================
# Swagelok UNSPSC Engine
# ==================================
class SwagelokExtractor:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })

    def extract(self, url):
        try:
            r = self.session.get(url, timeout=20)
            r.raise_for_status()
            return self._parse_html(r.text, url)
        except Exception:
            return {
                "Part Number": self._part_from_url(url),
                "Feature": "N/A",
                "UNSPSC": "N/A"
            }

    def _part_from_url(self, url):
        m = re.search(r"part=([A-Z0-9-]+)", url, re.IGNORECASE)
        return m.group(1) if m else "N/A"

    def _parse_html(self, html, url):
        soup = BeautifulSoup(html, "html.parser")
        part_number = self._part_from_url(url)

        unspsc_map = {}
        for row in soup.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) >= 2:
                key = cells[0].get_text(strip=True)
                val = cells[1].get_text(strip=True)
                if "UNSPSC" in key and "(" in key and ")" in key and val.isdigit():
                    unspsc_map[key] = val

        if not unspsc_map:
            for v, c in re.findall(r"UNSPSC\s*\(([\d.]+)\).*?(\d{6,8})", html):
                unspsc_map[f"UNSPSC ({v})"] = c

        feature, code = self._latest_unspsc(unspsc_map)

        return {
            "Part Number": part_number,
            "Feature": feature or "N/A",
            "UNSPSC": code or "N/A"
        }

    def _latest_unspsc(self, data):
        parsed = []
        for label, code in data.items():
            m = re.search(r"\(([\d.]+)\)", label)
            if m:
                parts = m.group(1).split(".")
                parsed.append((int(parts[0]), int(parts[1]) if len(parts) > 1 else 0, label, code))
        if not parsed:
            return None, None
        parsed.sort(reverse=True)
        return parsed[0][2], parsed[0][3]


# ==================================
# PROFESSIONAL UI - BEAUTIFUL DESIGN
# ==================================
st.set_page_config(
    page_title="Swagelok UNSPSC Intelligence Platform",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for beautiful design
st.markdown("""
<style>
    /* Main header styling */
    .main-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 2rem;
        border-radius: 15px;
        margin-bottom: 2rem;
        box-shadow: 0 10px 30px rgba(0,0,0,0.1);
    }
    
    .main-title {
        color: white;
        font-size: 2.5rem;
        font-weight: 700;
        margin: 0;
        text-align: center;
    }
    
    .subtitle {
        color: #E8E8F8;
        font-size: 1.2rem;
        text-align: center;
        margin-top: 0.5rem;
    }
    
    /* Stats cards */
    .stat-card {
        background: white;
        padding: 1.5rem;
        border-radius: 10px;
        box-shadow: 0 4px 15px rgba(0,0,0,0.08);
        border-left: 4px solid #667eea;
        margin-bottom: 1rem;
    }
    
    .stat-number {
        font-size: 2rem;
        font-weight: 700;
        color: #667eea;
    }
    
    .stat-label {
        color: #666;
        font-size: 0.9rem;
        text-transform: uppercase;
        letter-spacing: 1px;
    }
    
    /* Success box */
    .success-box {
        background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
        padding: 1.5rem;
        border-radius: 10px;
        color: white;
        text-align: center;
        margin: 1rem 0;
        box-shadow: 0 5px 20px rgba(17,153,142,0.3);
    }
    
    /* Footer */
    .footer {
        text-align: center;
        padding: 2rem;
        color: #666;
        border-top: 2px solid #f0f0f0;
        margin-top: 3rem;
    }
    
    .creator-name {
        color: #667eea;
        font-weight: 700;
        font-size: 1.1rem;
    }
    
    /* Upload section */
    .upload-section {
        background: #f8f9ff;
        padding: 2rem;
        border-radius: 15px;
        border: 2px dashed #667eea;
        margin: 1rem 0;
    }
    
    /* Button styling */
    .stButton>button {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        font-weight: 600;
        padding: 0.75rem 2rem;
        border-radius: 10px;
        border: none;
        box-shadow: 0 4px 15px rgba(102,126,234,0.3);
        transition: all 0.3s ease;
    }
    
    .stButton>button:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 20px rgba(102,126,234,0.4);
    }
</style>
""", unsafe_allow_html=True)

# Header
st.markdown("""
<div class="main-header">
    <h1 class="main-title">🔍 Swagelok UNSPSC Intelligence Platform</h1>
    <p class="subtitle">Smart Data Extraction • Real-Time Processing • Enterprise Grade</p>
</div>
""", unsafe_allow_html=True)
st.markdown(
    """
## About Swagelok 
Swagelok is a global leader in fluid system solutions, serving industries such as
oil & gas, semiconductors, pharmaceuticals, and clean energy.  
This tool focuses exclusively on **Swagelok product pages** to ensure
high-confidence UNSPSC classification aligned with official product specifications.
"""
)

# Sidebar
with st.sidebar:
    st.markdown("### 📊 About This Platform")
    st.markdown("""
    **Intelligent Features:**
    - 🚀 **Ultra-Fast Processing**: Parallel extraction with 12 workers
    - 🧹 **Smart Deduplication**: Removes duplicates automatically
    - 🎯 **High Accuracy**: Latest UNSPSC version detection
    - 📈 **Real-Time Progress**: Live status updates
    - 💾 **Clean Output**: Professional Excel format
    """)
    
    st.markdown("---")
    st.markdown("### 🔒 Data Privacy")
    st.info("All processing happens in real-time. No data is stored on our servers.")
    
    st.markdown("---")
    st.markdown("### 📚 Requirements")
    st.markdown("""
    **Input File:**
    - Excel format (.xlsx, .xls)
    - Column name: **COnlineUrl**
    - Contains Swagelok product URLs
    
    **Output:**
    - Part Number
    - Feature (UNSPSC version)
    - UNSPSC (code)
    """)

# Main content
col1, col2 = st.columns([2, 1])

with col1:
    st.markdown("### 📤 Upload Your Data")
    uploaded_file = st.file_uploader(
        "Drop your Excel file here or click to browse",
        type=["xlsx", "xls"],
        help="File must contain a column named 'COnlineUrl' with Swagelok product URLs"
    )

with col2:
    st.markdown("### 💡 Quick Guide")
    st.markdown("""
    1. **Upload** Excel file
    2. **Review** statistics
    3. **Click** Extract button
    4. **Download** results
    """)

if uploaded_file:
    # Read file
    df = pd.read_excel(uploaded_file)

    if "COnlineUrl" not in df.columns:
        st.error("❌ **Error:** Column 'COnlineUrl' not found in your file!")
        st.info("💡 **Tip:** Make sure your Excel file has a column named exactly 'COnlineUrl' (case-sensitive)")
        st.stop()

    # -------- SMART DATA CLEANING - FIXED DEDUPLICATION --------
    raw_urls = df["COnlineUrl"].dropna().astype(str)
    
    # Clean and normalize
    cleaned_urls = raw_urls.str.strip()
    
    # Filter only Swagelok URLs (case-insensitive check)
    swagelok_mask = cleaned_urls.str.lower().str.contains(
        "swagelok.com/en/catalog/product/detail", 
        na=False
    )
    
    swagelok_urls = cleaned_urls[swagelok_mask]
    
    # Remove duplicates (keeping original case for URLs)
    unique_urls = swagelok_urls.drop_duplicates()
    
    # Convert to list
    url_list = unique_urls.tolist()

    # Display statistics
    st.markdown("---")
    st.markdown("### 📊 Data Analysis")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown(f"""
        <div class="stat-card">
            <div class="stat-number">{len(raw_urls)}</div>
            <div class="stat-label">Total Rows</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
        <div class="stat-card">
            <div class="stat-number">{len(swagelok_urls)}</div>
            <div class="stat-label">Swagelok URLs</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown(f"""
        <div class="stat-card">
            <div class="stat-number">{len(unique_urls)}</div>
            <div class="stat-label">Unique Products</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        duplicates = len(swagelok_urls) - len(unique_urls)
        st.markdown(f"""
        <div class="stat-card">
            <div class="stat-number">{duplicates}</div>
            <div class="stat-label">Duplicates Removed</div>
        </div>
        """, unsafe_allow_html=True)

    # Show sample data
    with st.expander("🔍 View Sample Data", expanded=False):
        st.dataframe(unique_urls.head(10).to_frame(name="Unique URLs"), use_container_width=True)

    st.markdown("---")

    # Extract button
    if st.button("🚀 **Start Intelligent Extraction**", use_container_width=True):
        extractor = SwagelokExtractor()
        results = []

        # Progress tracking
        progress_bar = st.progress(0)
        status_text = st.empty()
        metrics_placeholder = st.empty()
        
        start_time = time.time()

        status_text.markdown("### 🧠 Initializing extraction engine...")
        time.sleep(0.5)

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(extractor.extract, url): url
                for url in url_list
            }

            total = len(url_list)
            for i, future in enumerate(as_completed(futures), 1):
                results.append(future.result())
                progress = i / total
                progress_bar.progress(progress)

                elapsed = time.time() - start_time
                avg_time = elapsed / i
                remaining = int(avg_time * (total - i))
                
                # Calculate speed
                speed = i / elapsed if elapsed > 0 else 0

                # Update status
                status_text.markdown(f"""
                ### 🔄 Processing in Progress
                **Current:** {i} / {total} products  
                **Speed:** {speed:.1f} products/second  
                **Time Elapsed:** {int(elapsed)}s  
                **Estimated Remaining:** {remaining}s
                """)

        # Success message
        total_time = int(time.time() - start_time)
        st.markdown(f"""
        <div class="success-box">
            <h2>✅ Extraction Completed Successfully!</h2>
            <p>Processed {len(results)} products in {total_time} seconds</p>
            <p>Average speed: {len(results)/total_time:.1f} products/second</p>
        </div>
        """, unsafe_allow_html=True)

        # Create output dataframe
        output_df = pd.DataFrame(results)
        
        # Summary statistics
        st.markdown("### 📈 Extraction Summary")
        col1, col2, col3 = st.columns(3)
        
        successful = output_df[output_df["UNSPSC"] != "N/A"].shape[0]
        failed = output_df[output_df["UNSPSC"] == "N/A"].shape[0]
        success_rate = (successful / len(results) * 100) if len(results) > 0 else 0
        
        with col1:
            st.metric("✅ Successful", successful, delta=f"{success_rate:.1f}%")
        with col2:
            st.metric("⚠️ Not Found", failed)
        with col3:
            st.metric("⏱️ Total Time", f"{total_time}s")

        # Download button
        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            output_df.to_excel(writer, index=False, sheet_name="UNSPSC Data")
        
        st.download_button(
            label="📥 **Download Excel Results**",
            data=buffer.getvalue(),
            file_name=f"swagelok_unspsc_output_{int(time.time())}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )

        # Display results
        st.markdown("### 📋 Results Preview")
        st.dataframe(output_df, use_container_width=True, height=400)

# Footer
st.markdown("---")
st.markdown("""
<div class="footer">
    <p>🎨 <span class="creator-name">Designed & Developed by Abdelmoneim Moustafa</span></p>
    <p>Data Intelligence Engineer • Automation Specialist • Procurement Systems Expert</p>
    <p style="font-size: 0.8rem; color: #999; margin-top: 1rem;">
        © 2025 Swagelok UNSPSC Intelligence Platform • Enterprise Grade Solution
    </p>
</div>
""", unsafe_allow_html=True)
