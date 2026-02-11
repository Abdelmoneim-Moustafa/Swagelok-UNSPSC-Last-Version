import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
from urllib.parse import urlparse, parse_qs

st.set_page_config(page_title="Swagelok Product Scraper", layout="centered")

# Title and project description
st.title("Swagelok Product UNSPSC Scraper")
description = (
    "This app allows you to upload an Excel file of Swagelok product page URLs. "
    "It scrapes each page to extract the **correct part number** and the latest **UNSPSC classification** (feature and code). "
    "The results are displayed in a table with columns for URL, Part, UNSPSC Feature (Latest), UNSPSC Code, and Company. "
    "You can then download the results as a CSV file."
)
st.markdown(description)

uploaded_file = st.file_uploader("Upload Excel file with product page URLs", type=["xlsx"])
start_processing = False

if uploaded_file is not None:
    try:
        df_input = pd.read_excel(uploaded_file)
    except Exception as e:
        st.error(f"Error reading Excel file: {e}")
        df_input = None

    if df_input is not None:
        # Determine which column has URLs
        if 'URL' in df_input.columns:
            urls = df_input['URL'].dropna().astype(str).tolist()
        elif df_input.shape[1] == 1:
            urls = df_input.iloc[:, 0].dropna().astype(str).tolist()
        else:
            url_cols = [col for col in df_input.columns 
                        if df_input[col].astype(str).str.contains("http", na=False).any()]
            if url_cols:
                urls = df_input[url_cols[0]].dropna().astype(str).tolist()
            else:
                urls = []
                st.error("No column containing URLs found.")

        if urls:
            # Center the Start button
            col1, col2, col3 = st.columns([1, 2, 1])
            with col2:
                start_processing = st.button("Start")

            if start_processing:
                progress_bar = st.progress(0)
                results = []
                total = len(urls)
                for idx, url in enumerate(urls):
                    url = url.strip()
                    part = ""
                    unspsc_feat = ""
                    unspsc_code = ""
                    try:
                        headers = {"User-Agent": "Mozilla/5.0"}
                        response = requests.get(url, headers=headers, timeout=15)
                        response.raise_for_status()
                        soup = BeautifulSoup(response.text, 'html.parser')
                        # Extract part number
                        text = soup.get_text()
                        m = re.search(r"Part\s*#:\s*([\w\-]+)", text)
                        if m:
                            part = m.group(1).strip()
                        else:
                            # Fallback: try query param 'part'
                            q = urlparse(url).query
                            params = parse_qs(q)
                            if 'part' in params:
                                part = params['part'][0]
                        # Extract UNSPSC entries
                        unspsc_rows = []
                        for tr in soup.find_all('tr'):
                            cols = [td.get_text(strip=True) for td in tr.find_all('td')]
                            if cols and cols[0].startswith("UNSPSC"):
                                unspsc_rows.append(cols)
                        if unspsc_rows:
                            def ver_tuple(ver_str):
                                mm = re.search(r"UNSPSC\s*\(([\d\.]+)\)", ver_str)
                                if mm:
                                    parts = mm.group(1).split('.')
                                    try:
                                        return tuple(int(p.lstrip('0') or 0) for p in parts)
                                    except:
                                        return (0,)
                                return (0,)
                            max_row = max(unspsc_rows, key=lambda row: ver_tuple(row[0]))
                            unspsc_feat = max_row[0]
                            unspsc_code = max_row[1] if len(max_row) > 1 else ""
                    except Exception:
                        part = part or ""
                        unspsc_feat = unspsc_feat or ""
                        unspsc_code = unspsc_code or ""
                    company = "Swagelok"
                    results.append({
                        "URL": url,
                        "Part": part,
                        "UNSPSC Feature (Latest)": unspsc_feat,
                        "UNSPSC Code": unspsc_code,
                        "Company": company
                    })
                    progress = int((idx + 1) * 100 / total)
                    progress_bar.progress(progress)

                # Show results and provide download button
                df_results = pd.DataFrame(results, columns=[
                    "URL", "Part", "UNSPSC Feature (Latest)", "UNSPSC Code", "Company"
                ])
                st.dataframe(df_results)
                csv_data = df_results.to_csv(index=False).encode('utf-8')
                st.download_button("Download Results", data=csv_data,
                                    file_name="swagelok_results.csv", mime="text/csv")
