"""
🔍 Swagelok UNSPSC Intelligence Platform - Row-by-Row Version
Processes each URL individually with detailed error tracking

Created by: Abdelmoneim Moustafa
"""

import re, time, pandas as pd, requests, streamlit as st
from bs4 import BeautifulSoup
from io import BytesIO
from typing import Dict, Optional, Tuple

TIMEOUT, COMPANY_NAME, CHECKPOINT_INTERVAL = 20, "Swagelok", 100

st.set_page_config(page_title="Swagelok UNSPSC", page_icon="🔍", layout="wide")

st.markdown("""
<style>
.main-header{background:linear-gradient(135deg,#667eea,#764ba2);padding:2.5rem;border-radius:15px;color:white;text-align:center;margin-bottom:2rem;box-shadow:0 8px 20px rgba(102,126,234,0.3)}
.info-box{background:var(--info-bg,#e3f2fd);border-left:5px solid #2196f3;padding:1.5rem;border-radius:12px;margin:1rem 0}
.success-box{background:linear-gradient(135deg,#11998e,#38ef7d);padding:2rem;border-radius:15px;color:white;text-align:center;margin:1.5rem 0;box-shadow:0 8px 20px rgba(17,153,142,0.3)}
.progress-card{background:var(--card-bg,#fff);padding:1.5rem;border-radius:12px;margin:1rem 0;border:1px solid var(--border-color,#e0e0e0)}
.error-card{background:var(--error-bg,#ffebee);border-left:5px solid #f44336;padding:1rem;border-radius:8px;margin:0.5rem 0}
@media (prefers-color-scheme:dark){:root{--info-bg:#1a237e;--error-bg:#b71c1c;--card-bg:#1e1e1e;--border-color:#424242}}
</style>
""", unsafe_allow_html=True)

class SwagelokExtractor:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Mozilla/5.0"})
    
    def extract(self, url: str, row_num: int) -> Dict:
        r = {"Row":row_num,"Part":"Not Found","Company":COMPANY_NAME,"URL":url or "Empty","UNSPSC Feature (Latest)":"Not Found","UNSPSC Code":"Not Found","Status":"Success","Error":""}
        if not url or not isinstance(url,str) or not url.startswith("http"):
            r["Status"],r["Error"]="Invalid URL","URL is empty or invalid"
            return r
        try:
            resp = self.session.get(url, timeout=TIMEOUT)
            if resp.status_code != 200:
                r["Status"],r["Error"]=f"HTTP {resp.status_code}",f"Status {resp.status_code}"
                return r
            soup,html = BeautifulSoup(resp.text,"html.parser"),resp.text
            if p:=self._part(soup,html,url): r["Part"]=p
            else: r["Error"]="Part not found"
            if (f,c:=self._unspsc(soup,html))[0]: r["UNSPSC Feature (Latest)"],r["UNSPSC Code"]=f,c
            else: r["Error"]=r["Error"]+";UNSPSC not found" if r["Error"] else "UNSPSC not found"
            return r
        except requests.Timeout: r["Status"],r["Error"]="Timeout",f"Timeout after {TIMEOUT}s"; return r
        except Exception as e: r["Status"],r["Error"]="Error",str(e)[:100]; return r
    
    def _part(self,s,h,u):
        up=self._up(u)
        for m in re.findall(r'Part\s*#\s*:\s*(?:<[^>]+>)?\s*([A-Z0-9][A-Z0-9.\-_/]*)',h,re.I):
            if c:=m.strip():
                if up and self._pm(c,up): return c
                if not up and self._vp(c): return c
        return up if up and self._vp(up) else None
    
    def _up(self,u):
        for p in [r'/p/([A-Z0-9.\-_/%]+)',r'[?&]part=([A-Z0-9.\-_/%]+)']:
            if m:=re.search(p,u,re.I): return m.group(1).replace('%2F','/').replace('%252F','/').strip()
    
    def _pm(self,p1,p2): return bool(p1 and p2 and re.sub(r'[.\-/]','',p1).lower()==re.sub(r'[.\-/]','',p2).lower())
    def _vp(self,p): return isinstance(p,str) and 2<=len(p)<=100 and (any(c.isalpha() for c in p) or (any(c.isdigit() for c in p) and len(p)>3)) and not any(x in p.lower() for x in['charset','utf','html','http'])
    
    def _unspsc(self,s,h):
        v=[]
        for r in s.find_all('tr'):
            if (c:=r.find_all('td')) and len(c)>=2:
                a,val=c[0].text.strip(),c[1].text.strip()
                if not a.upper().startswith('UNSPSC'): continue
                if (m:=re.search(r'UNSPSC\s*\(([\d.]+)\)',a,re.I)) and re.match(r'^\d{6,8}$',val):
                    v.append({'v':tuple(map(int,m.group(1).split('.'))),'f':a,'c':val})
        if not v:
            for x,y in re.findall(r'UNSPSC\s*\(([\d.]+)\)[^\d]*?(\d{6,8})',h,re.I):
                v.append({'v':tuple(map(int,x.split('.'))),'f':f"UNSPSC ({x})",'c':y})
        if not v: return None,None
        v.sort(key=lambda x:x['v'],reverse=True)
        hv=[x for x in v if x['v']==v[0]['v']]
        return hv[-1]['f'],hv[-1]['c']

st.markdown('<div class="main-header"><h1>🔍 Swagelok UNSPSC Platform</h1><p>Row-by-Row • Adaptive Theme • Error Tracking</p></div>',unsafe_allow_html=True)
st.markdown('<div class="info-box"><strong>✨ FEATURES:</strong><br>✅ Row-by-Row Processing<br>✅ Dark/Light Theme<br>✅ Error Tracking<br>✅ Last UNSPSC (when duplicates)<br>✅ Auto-Save Every 100</div>',unsafe_allow_html=True)

with st.sidebar:
    st.markdown("### ⚙️ Config")
    st.code(f"Timeout: {TIMEOUT}s\nCheckpoint: {CHECKPOINT_INTERVAL}\nProcessing: Sequential")
    st.markdown("---\n**🎨 Abdelmoneim Moustafa**\n*Data Intelligence Engineer*")

if f:=st.file_uploader("📤 Upload Excel",type=["xlsx","xls"]):
    try:
        df=pd.read_excel(f)
        uc=next((c for c in df.columns if df[c].astype(str).str.contains("http",na=False,case=False).any()),None)
        if not uc: st.error("❌ No URL column"); st.stop()
        st.success(f"✅ URL column: **{uc}**")
        urls=[str(x).strip() if pd.notna(x) and str(x).strip() else None for x in df[uc]]
        vc=sum(1 for u in urls if u)
        c1,c2,c3=st.columns(3)
        c1.metric("📊 Total",len(urls)); c2.metric("✅ Valid",vc); c3.metric("⏱️ Est.",f"~{int(vc*0.25/60)}m")
        with st.expander("👁️ Preview"): st.dataframe(pd.DataFrame({"Row":range(1,6),uc:[u or"Empty"for u in urls[:5]]}))
        
        if st.button("🚀 Start Row-by-Row Extraction",type="primary"):
            ex,res,errs=SwagelokExtractor(),[],[]
            pb,sc,ec,dp=st.progress(0),st.empty(),st.empty(),st.empty()
            st_t=time.time()
            
            for i,url in enumerate(urls,1):
                pb.progress(i/len(urls))
                r=ex.extract(url,i)
                res.append(r)
                if r["Status"]!="Success": errs.append(f"Row {i}: {r['Status']} - {r['Error']}")
                el,sp=time.time()-st_t,i/(time.time()-st_t)if time.time()>st_t else 0
                rm=int((len(urls)-i)/sp)if sp>0 else 0
                sc.markdown(f'<div class="progress-card"><strong>Row {i}/{len(urls)}</strong><br>Speed: {sp:.1f}/s | Remaining: {rm//60}m {rm%60}s<br>Part: {r["Part"]} | UNSPSC: {r["UNSPSC Code"]} | Status: {r["Status"]}</div>',unsafe_allow_html=True)
                if errs: ec.markdown(f'<div class="error-card"><strong>⚠️ Errors: {len(errs)}</strong><br>Latest: {errs[-1]}</div>',unsafe_allow_html=True)
                if i%CHECKPOINT_INTERVAL==0:
                    buf=BytesIO()
                    with pd.ExcelWriter(buf,engine="openpyxl")as w: pd.DataFrame(res).to_excel(w,index=False)
                    dp.download_button(f"💾 Checkpoint ({i})",buf.getvalue(),f"cp_{i}.xlsx","application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",key=f"cp{i}")
            
            tt=int(time.time()-st_t)
            od=pd.DataFrame(res)
            of=od.drop(columns=['Row','Status','Error'])
            pf=(od["Part"]!="Not Found").sum()
            uf=(od["UNSPSC Code"]!="Not Found").sum()
            suc=(od["Status"]=="Success").sum()
            
            st.markdown(f'<div class="success-box"><h2>✅ Complete!</h2><p><strong>Processed:</strong> {len(urls)} rows in {tt//60}m {tt%60}s</p><p><strong>Success:</strong> {suc}/{len(urls)} ({suc/len(urls)*100:.1f}%)</p><p><strong>Parts:</strong> {pf} | <strong>UNSPSC:</strong> {uf} | <strong>Errors:</strong> {len(errs)}</p></div>',unsafe_allow_html=True)
            
            c1,c2,c3,c4=st.columns(4)
            c1.metric("✅ Success",suc); c2.metric("✅ Parts",pf); c3.metric("✅ UNSPSC",uf); c4.metric("⚠️ Errors",len(errs))
            
            buf=BytesIO()
            with pd.ExcelWriter(buf,engine="openpyxl")as w: of.to_excel(w,index=False,sheet_name="Results")
            st.download_button("📥 Download Final Results",buf.getvalue(),f"swagelok_{int(time.time())}.xlsx","application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",use_container_width=True)
            
            st.markdown("### 📋 Results"); st.dataframe(of.head(20),use_container_width=True)
            if errs:
                with st.expander(f"⚠️ Error Log ({len(errs)})"):
                    for e in errs: st.text(e)
    except Exception as e: st.error(f"❌ Error: {e}"); st.exception(e)

st.markdown('---\n<div style="text-align:center;padding:2rem"><p style="font-size:1.2rem;font-weight:600">🎨 Abdelmoneim Moustafa</p><p>Data Intelligence Engineer</p></div>',unsafe_allow_html=True)
