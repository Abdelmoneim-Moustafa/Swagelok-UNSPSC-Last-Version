"""
🔍 Swagelok UNSPSC Extractor - FIXED VERSION
Correctly extracts part from "Part #:" label

Created by: Abdelmoneim Moustafa
Data Intelligence Engineer
"""

import re
import time
import pandas as pd
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Optional, Tuple

# Config
MAX_WORKERS = 6
TIMEOUT = 20
BATCH_SIZE = 100

class SwagelokExtractor:
    """Fixed extractor that correctly finds Part #: label"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })
    
    def extract(self, url: str) -> Dict:
        """Extract with correct part identification"""
        result = {
            "Part": "Not Found",
            "Company": "Swagelok",
            "URL": url if url else "Empty",
            "UNSPSC Feature (Latest)": "Not Found",
            "UNSPSC Code": "Not Found"
        }
        
        if not url or not isinstance(url, str) or not url.startswith("http"):
            return result
        
        try:
            response = self.session.get(url, timeout=TIMEOUT)
            if response.status_code != 200:
                return result
            
            soup = BeautifulSoup(response.text, "html.parser")
            html_text = response.text
            
            # CRITICAL: Extract CORRECT part
            part = self._extract_correct_part(soup, html_text, url)
            if part:
                result["Part"] = part
            
            # Extract UNSPSC
            feature, code = self._extract_unspsc(soup, html_text)
            if feature and code:
                result["UNSPSC Feature (Latest)"] = feature
                result["UNSPSC Code"] = code
            
            return result
            
        except Exception as e:
            return result
    
    def _extract_correct_part(self, soup, html_text, url) -> Optional[str]:
        """
        FIXED: Extract the CORRECT part from Part #: label
        Problem: Old code was finding OTHER parts on page
        Solution: Find the MAIN part near "Part #:" label
        """
        
        # Get the part from URL first (this is our REFERENCE)
        url_part = self._get_url_part(url)
        
        # STRATEGY 1: Find "Part #:" in HTML and get value RIGHT AFTER it
        # This should match the URL part
        
        # Look for pattern: Part #: <something>MS-TL-BGC</something>
        pattern1 = r'Part\s*#\s*:\s*(?:<[^>]+>)?\s*([A-Z0-9][A-Z0-9.\-_/]*)'
        matches = re.findall(pattern1, html_text, re.IGNORECASE)
        
        for match in matches:
            clean = match.strip()
            # If this matches URL part, use it!
            if url_part and self._parts_match(clean, url_part):
                return clean
            # Or if it's valid and we have no URL part
            if not url_part and self._is_valid_part(clean):
                return clean
        
        # STRATEGY 2: Find element with "Part #" text and get next sibling
        try:
            # Find all text containing "Part #"
            for elem in soup.find_all(text=re.compile(r'Part\s*#', re.I)):
                parent = elem.parent
                # Get the next element or text
                next_elem = parent.find_next()
                if next_elem:
                    text = next_elem.get_text(strip=True)
                    if self._is_valid_part(text):
                        if url_part and self._parts_match(text, url_part):
                            return text
                        if not url_part:
                            return text
        except:
            pass
        
        # STRATEGY 3: If we have URL part and it's valid, use it
        if url_part and self._is_valid_part(url_part):
            return url_part
        
        return None
    
    def _get_url_part(self, url) -> Optional[str]:
        """Extract part from URL"""
        patterns = [
            r'/p/([A-Z0-9.\-_/%]+)',
            r'[?&]part=([A-Z0-9.\-_/%]+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, url, re.IGNORECASE)
            if match:
                part = match.group(1).replace('%2F', '/').replace('%252F', '/')
                return part.strip()
        return None
    
    def _parts_match(self, part1: str, part2: str) -> bool:
        """Check if two parts are the same (normalized)"""
        if not part1 or not part2:
            return False
        
        # Normalize: remove dots, dashes, slashes, lowercase
        p1 = re.sub(r'[.\-/]', '', part1).lower()
        p2 = re.sub(r'[.\-/]', '', part2).lower()
        
        return p1 == p2
    
    def _is_valid_part(self, part: str) -> bool:
        """Validate part number"""
        if not isinstance(part, str) or not (2 <= len(part) <= 100):
            return False
        
        has_letter = any(c.isalpha() for c in part)
        has_number = any(c.isdigit() for c in part)
        
        # Accept if has letters, or has numbers with length > 3
        if not (has_letter or (has_number and len(part) > 3)):
            return False
        
        # Exclude garbage
        exclude = ['charset', 'utf', 'html', 'text', 'http', 'www', 'catalog', 'products']
        part_lower = part.lower()
        
        return not any(ex in part_lower for ex in exclude)
    
    def _extract_unspsc(self, soup, html_text) -> Tuple[Optional[str], Optional[str]]:
        """Extract LATEST UNSPSC"""
        versions = []
        
        for row in soup.find_all('tr'):
            cells = row.find_all('td')
            if len(cells) >= 2:
                attr = cells[0].get_text(strip=True)
                val = cells[1].get_text(strip=True)
                vm = re.search(r'UNSPSC\s*\(([\d.]+)\)', attr, re.IGNORECASE)
                if vm and re.match(r'^\d{6,8}$', val):
                    versions.append({
                        'v': tuple(map(int, vm.group(1).split('.'))),
                        'f': attr,
                        'c': val
                    })
        
        if not versions:
            for v, c in re.findall(r'UNSPSC\s*\(([\d.]+)\)[^\d]*?(\d{6,8})', html_text, re.IGNORECASE):
                versions.append({
                    'v': tuple(map(int, v.split('.'))),
                    'f': f"UNSPSC ({v})",
                    'c': c
                })
        
        if not versions:
            return None, None
        
        versions.sort(key=lambda x: x['v'], reverse=True)
        return versions[0]['f'], versions[0]['c']

def process_file(input_file: str, output_file: str):
    """Process file and extract data"""
    
    print("="*70)
    print(" "*15 + "🔍 SWAGELOK UNSPSC EXTRACTOR - FIXED")
    print("="*70)
    
    # Load
    print("\n📂 Loading file...")
    df = pd.read_excel(input_file)
    
    # Find URL column
    url_col = None
    for col in df.columns:
        if df[col].astype(str).str.contains("http", na=False, case=False).any():
            url_col = col
            break
    
    if not url_col:
        print("❌ No URL column found")
        return
    
    print(f"✅ URL column: {url_col}")
    
    # Get URLs
    urls = [str(x).strip() if pd.notna(x) and str(x).strip() else None for x in df[url_col]]
    valid_urls = [u for u in urls if u]
    
    print(f"\n📊 Input: {len(df)} rows, {len(valid_urls)} valid URLs")
    print(f"🔄 Batches: {(len(valid_urls) + BATCH_SIZE - 1) // BATCH_SIZE}")
    print(f"\n🚀 Processing...\n")
    
    # Process
    extractor = SwagelokExtractor()
    all_results = []
    start_time = time.time()
    
    for batch_num in range(0, len(valid_urls), BATCH_SIZE):
        batch = valid_urls[batch_num:min(batch_num + BATCH_SIZE, len(valid_urls))]
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(extractor.extract, url): url for url in batch}
            
            for future in as_completed(futures):
                try:
                    all_results.append(future.result(timeout=30))
                except:
                    all_results.append({
                        "Part": "Not Found",
                        "Company": "Swagelok",
                        "URL": futures[future],
                        "UNSPSC Feature (Latest)": "Not Found",
                        "UNSPSC Code": "Not Found"
                    })
                
                done = len(all_results)
                elapsed = time.time() - start_time
                speed = done / elapsed if elapsed > 0 else 0
                remaining = int((len(valid_urls) - done) / speed) if speed > 0 else 0
                
                print(f"\r⚡ {done}/{len(valid_urls)} | {speed:.1f}/s | Remaining: {remaining//60}m {remaining%60}s", end='')
        
        # Checkpoint
        pd.DataFrame(all_results).to_excel(f"checkpoint_{batch_num//BATCH_SIZE + 1}.xlsx", index=False)
    
    print("\n")
    
    # Save
    results_df = pd.DataFrame(all_results)
    
    # Merge with original
    df_out = df.copy()
    df_out = df_out.merge(
        results_df,
        left_on=url_col,
        right_on='URL',
        how='left',
        suffixes=('_original', '')
    )
    
    df_out.to_excel(output_file, index=False)
    
    total_time = int(time.time() - start_time)
    parts_found = (results_df["Part"] != "Not Found").sum()
    unspsc_found = (results_df["UNSPSC Code"] != "Not Found").sum()
    
    print("="*70)
    print(" "*28 + "✅ COMPLETE!")
    print("="*70)
    print(f"\n📊 Results: {parts_found}/{len(results_df)} parts ({parts_found/len(results_df)*100:.1f}%)")
    print(f"📊 UNSPSC: {unspsc_found}/{len(results_df)} codes ({unspsc_found/len(results_df)*100:.1f}%)")
    print(f"\n⏱️  Time: {total_time//60}m {total_time%60}s ({len(results_df)/total_time:.1f}/s)")
    print(f"\n📥 Saved: {output_file}\n")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        input_file = "/mnt/user-data/uploads/SW_clean.xlsx"
        output_file = "/mnt/user-data/outputs/SW_output_fixed.xlsx"
    else:
        input_file = sys.argv[1]
        output_file = sys.argv[2] if len(sys.argv) > 2 else "output_fixed.xlsx"
    
    process_file(input_file, output_file)
