import requests
from bs4 import BeautifulSoup
import json
import logging
from datetime import datetime
import re
import time
import os
from urllib.parse import urljoin, urlparse

# ============ Mode, cache, fallback ============
import argparse, hashlib, pathlib
import cloudscraper  # fallback for anti-bot

def get_run_mode():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", default=os.getenv("RUN_MODE","nightly"))
    m = (ap.parse_args().mode or "nightly").lower()
    return "weekly" if m=="weekly" else ("light" if m=="light" else "nightly")

RUN_MODE = get_run_mode()
IS_LIGHT = (RUN_MODE == "light")

CACHE_DIR = pathlib.Path(".cache"); CACHE_DIR.mkdir(exist_ok=True)
CACHE_TTL = 24*3600
def cache_key(url): return CACHE_DIR / (hashlib.sha1(url.encode()).hexdigest()+".html")
def get_html(url, headers, timeout, allow_cache):
    ck = cache_key(url)
    if allow_cache and ck.exists() and (time.time()-ck.stat().st_mtime) < CACHE_TTL:
        return ck.read_bytes()
    try:
        r = requests.get(url, headers=headers, timeout=timeout)
        r.raise_for_status()
        ck.write_bytes(r.content)
        return r.content
    except Exception:
        try:
            scraper = cloudscraper.create_scraper()
            c = scraper.get(url, timeout=timeout+5).content
            ck.write_bytes(c)
            return c
        except Exception:
            return b""
# =================================================

# ---------------- Configuration ----------------
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
REQUEST_TIMEOUT = 15 if IS_LIGHT else 20
REQUEST_SLEEP_SECONDS = 1.2
FIRST_SUCCESS_MODE = True

def now_iso(): return datetime.now().isoformat()
def clean_text(x): return re.sub(r'\s+',' ', x or '').strip()
def normalize_url(base, href): return urljoin(base, href) if href else None
def slugify(text):
    t=(text or "").lower(); t=re.sub(r'[^a-z0-9]+','-',t).strip('-'); return t[:80] or 'job'
def derive_slug(title, link):
    s=slugify(title)
    if s=='job' and link:
        path=urlparse(link).path; s=slugify(path.split('/')[-1])
    return s or 'job'

# Host-aware ID to avoid collisions on root paths
def make_id(prefix, link):
    if not link: return f"{prefix}_no_link"
    p = urlparse(link)
    key = (p.netloc + p.path + ("?" + p.query if p.query else "")).strip("/") or "root"
    return f"{prefix}_{key}"

HEADERS = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36'}

def load_rules_file(path="rules.json"):
    try: return json.load(open(path,"r",encoding="utf-8"))
    except Exception: return {}
RULES_FILE = load_rules_file()
EXTRA_SEEDS = RULES_FILE.get("captureHints", []) if RUN_MODE in ("weekly","light") else []

SOURCES = [
    {"name":"freejobalert","url":"https://www.freejobalert.com/","parser":"parse_freejobalert"},
    {"name":"sarkarijobfind","url":"https://sarkarijobfind.com/","parser":"parse_sarkarijobfind"},
    {"name":"resultbharat","url":"https://www.resultbharat.com/","parser":"parse_resultbharat"},
    {"name":"adda247","url":"https://www.adda247.com/jobs/","parser":"parse_adda247"}
]
for i,u in enumerate(EXTRA_SEEDS[:25]):
    SOURCES.insert(0, {"name":f"hint{i+1}","url":u,"parser":"parse_adda247"})
if IS_LIGHT:
    SOURCES = [s for s in SOURCES if s["name"].startswith("hint")]

# ---------------- Policies ----------------
TEACHER_TERMS={"teacher","tgt","pgt","prt","school teacher","faculty","lecturer","assistant professor","professor","b.ed","bed ","d.el.ed","deled","ctet","tet "}
def education_band_from_text(text):
    t=(text or "").lower()
    if any(k in t for k in ["10th","matric","ssc "]): return "10th pass"
    if any(k in t for k in ["12th","intermediate","hsc"]): return "12th pass"
    if "graduate" in t or "bachelor" in t or "any degree" in t: return "Any graduate"
    return "N/A"
def violates_general_policy(text):
    t=(text or "").lower()
    if any(k in t for k in TEACHER_TERMS): return True
    disallow=["b.tech","btech","b.e"," be ","m.tech","mtech","m.e","mca","bca","b.sc (engg)","bsc (engg)","engineering degree","m.sc","msc","m.a"," m a ","m.com","mcom","mba","cma","cfa"," ca ","cs ","company secretary","pg ","post graduate","postgraduate","phd","m.phil","mphil","engineer","developer","scientist","specialist","analyst","technical manager","architect","research","research associate","data scientist","ml engineer","cloud engineer","sde","devops"]
    return any(k in t for k in disallow)
STATE_NAMES=["andhra pradesh","arunachal pradesh","assam","bihar","chhattisgarh","goa","gujarat","haryana","himachal pradesh","jharkhand","karnataka","kerala","madhya pradesh","maharashtra","manipur","meghalaya","mizoram","nagaland","odisha","punjab","rajasthan","sikkim","tamil nadu","telangana","tripura","uttar pradesh","uttarakhand","west bengal","jammu","kashmir","ladakh","delhi","puducherry","chandigarh","andaman","nicobar","dadra","nagar haveli","daman","diu","lakshadweep"]
OPEN_SIGNALS=["all india","any state","open to all","pan india","indian nationals","across india","from any state"]
CLOSE_SIGNALS=["domicile","resident","locals only","local candidates","state quota","only"]
def domicile_allow(title):
    t=(title or "").lower()
    if any(k in t for k in OPEN_SIGNALS): return True
    if "bihar" in t and any(k in t for k in CLOSE_SIGNALS): return True
    for st in STATE_NAMES:
        if st=="bihar": continue
        if st in t and any(k in t for k in CLOSE_SIGNALS): return False
    return True
def title_hits_excluded(title):
    t=(title or "").lower()
    for k in ["teacher","tgt","pgt","prt","b.ed","ctet","tet"]:
        if k in t: return True, f"Excluded by rule: {k}"
    return False, ""
def site_section_excluded(host, title):
    hint = {
        "www.adda247.com":{"excludeSections":["sarkari result","admit card","answer key"]},
        "www.resultbharat.com":{"excludeSections":["result","admit card","answer key"]},
        "sarkarijobfind.com":{"excludeSections":["result","admit card","answer key"]},
        "www.freejobalert.com":{"excludeSections":["admit card","result","answer key","syllabus"]}
    }.get(host,{})
    if not hint: return False
    t=(title or "").lower()
    for w in hint.get("excludeSections",[]): 
        if w in t: return True
    return False

# ---------------- Job builder ----------------
def build_job(prefix, source_name, base_url, title, href, deadline="N/A"):
    title=clean_text(title)
    href_abs = href if href and href.startswith("http") else normalize_url(base_url, href)
    hit,_=title_hits_excluded(title)
    if hit: return None
    if violates_general_policy(title): return None
    if not domicile_allow(title): return None
    edu=education_band_from_text(title)
    return {
        "id": make_id(prefix, href_abs or base_url),
        "title": title or "N/A",
        "organization": "N/A",
        "deadline": deadline or "N/A",
        "applyLink": href_abs or base_url,
        "slug": derive_slug(title, href_abs or base_url),
        "qualificationLevel": edu if edu in ["10th pass","12th pass","Any graduate"] else "N/A",
        "domicile": "All India",
        "source": "aggregator",
        "type": "VACANCY",
        "extractedAt": now_iso(),
        "meta": {"sourceUrl": base_url, "sourceSite": source_name}
    }

# ---------------- Parsers ----------------
def parse_freejobalert(content, source_name, base_url):
    soup = BeautifulSoup(content,'html.parser'); jobs=[]; host=urlparse(base_url).netloc
    def looks_job(t):
        tl=(t or "").lower()
        if any(x in tl for x in ["admit card","result","answer key","syllabus"]): return False
        return any(x in tl for x in ["recruit","vacancy","notification","apply online","corrigendum","extension","extended"])
    for tbl in soup.select('table'):
        for a in tbl.select('a[href]'):
            title=clean_text(a.get_text())
            if not looks_job(title): continue
            job=build_job('fja', source_name, base_url, title, a.get('href'))
            if job and not site_section_excluded(host, title): jobs.append(job)
    if not jobs:
        for a in soup.select('main a[href], .entry-content a[href], a[href]'):
            title=clean_text(a.get_text())
            if not looks_job(title): continue
            job=build_job('fja', source_name, base_url, title, a.get('href'))
            if job and not site_section_excluded(host, title): jobs.append(job)
    return jobs

def parse_sarkarijobfind(content, source_name, base_url):
    soup=BeautifulSoup(content,'html.parser'); jobs=[]; host=urlparse(base_url).netloc
    for h in soup.find_all(re.compile('^h[1-6]$')):
        if re.search(r'new\s*update', h.get_text(), re.I):
            ul=h.find_next_sibling(['ul','ol']); 
            if not ul: continue
            for li in ul.select('li'):
                a=li.find('a', href=True); 
                if not a: continue
                title=clean_text(a.get_text())
                if site_section_excluded(host, title): continue
                job=build_job('sjf', source_name, base_url, title, a['href'])
                if job: jobs.append(job)
            break
    return jobs

def parse_resultbharat(content, source_name, base_url):
    soup=BeautifulSoup(content,'html.parser'); jobs=[]; host=urlparse(base_url).netloc
    for table in soup.select('table'):
        headers=[clean_text(th.get_text()) for th in table.select('th')]
        if not headers: continue
        col_idx=-1
        for i,h in enumerate(headers):
            if re.search(r'latest\s*jobs', h, re.I): col_idx=i; break
        if col_idx==-1: continue
        for tr in soup.select('tr'):
            tds=tr.select('td')
            if col_idx < len(tds):
                a=tds[col_idx].find('a', href=True)
                if not a: continue
                title=clean_text(a.get_text())
                if site_section_excluded(host, title): continue
                job=build_job('rb', source_name, base_url, title, a['href'])
                if job: jobs.append(job)
    return jobs

def parse_adda247(content, source_name, base_url):
    soup=BeautifulSoup(content,'html.parser'); jobs=[]; host=urlparse(base_url).netloc
    # Seed (hint) harvesting: broad anchors + URL de-dup + keyword filter
    if source_name.startswith("hint"):
        anchors = soup.select('main a[href], section a[href], article a[href], a[href]')
        seen_urls = set()
        def looks_seed(t, h):
            tl=(t or "").lower(); hl=(h or "").lower()
            kw=["recruit","vacancy","notific","advert","employment","application","corrigendum","extension","extended"]
            return any(k in tl for k in kw) or any(k in hl for k in kw)
        for a in anchors:
            title=clean_text(a.get_text()); href=a.get('href')
            if not title or not href: continue
            abs_url = normalize_url(base_url, href)
            if not abs_url or abs_url in seen_urls: continue
            if not looks_seed(title, href): continue
            if site_section_excluded(host, title): continue
            job=build_job('hint', source_name, base_url, title, abs_url)
            if job:
                jobs.append(job); seen_urls.add(abs_url)
        return jobs
    # Regular Adda with broadened selectors
    for a in soup.select('article a[href], .post-card a[href], .card a[href], main a[href*="recruit"], main a[href*="notific"]'):
        title=clean_text(a.get_text()); href=a.get('href')
        if not title or not href: continue
        if site_section_excluded(host, title): continue
        job=build_job('adda', source_name, base_url, title, href)
        if job: jobs.append(job)
    return jobs

# ---------------- Fetch / Save / Main ----------------
def fetch_and_parse(source):
    parser_func=globals().get(source["parser"])
    if not parser_func:
        logging.error(f"Missing parser: {source['parser']}"); return []
    allow_cache = RUN_MODE in ("weekly","light")
    content = get_html(source["url"], HEADERS, REQUEST_TIMEOUT, allow_cache)
    if not content:
        logging.error(f"{source['name']} fetch failed (empty)."); return []
    try:
        jobs = parser_func(content, source["name"], source["url"])
        logging.info(f"[{RUN_MODE}] {source['name']}: {len(jobs)} jobs")
        return jobs
    except Exception as e:
        logging.error(f"{source['name']} parsing error: {e}")
        return []

def enforce_schema_defaults(jobs):
    for j in jobs:
        j.setdefault("slug", derive_slug(j.get("title"), j.get("applyLink")))
        j.setdefault("qualificationLevel","N/A")
        j.setdefault("domicile","All India")
        j.setdefault("source","aggregator")
        j.setdefault("type","VACANCY")
        j.setdefault("extractedAt", now_iso())
    return jobs

def save_outputs(jobs, sources_used):
    jobs=enforce_schema_defaults(jobs)
    output={"lastUpdated": now_iso(),"totalJobs": len(jobs),"jobListings": jobs,"transparencyInfo":{"notes":"General vacancies (10th/12th/Any graduate). Excludes teacher and technical/management/PG roles. Domicile: All‑India and Bihar‑only allowed; other states allowed only if title signals open to any state.","sourcesTried": sources_used if isinstance(sources_used,list) else [sources_used],"schemaVersion":"1.2","totalListings": len(jobs),"lastUpdated": now_iso(),"runMode": RUN_MODE}}
    open("data.json","w",encoding="utf-8").write(json.dumps(output,indent=4,ensure_ascii=False))
    health={"ok": bool(jobs),"lastChecked": now_iso(),"totalActive": len(jobs),"sourceUsed": (sources_used[0] if isinstance(sources_used,list) and sources_used else str(sources_used)),"runMode": RUN_MODE}
    open("health.json","w",encoding="utf-8").write(json.dumps(health,indent=4))

if __name__=="__main__":
    collected=[]; used=[]
    use_first_success = FIRST_SUCCESS_MODE and not IS_LIGHT
    if use_first_success:
        for src in SOURCES:
            jobs=fetch_and_parse(src)
            if jobs:
                collected=jobs; used=[src["name"]]; break
            time.sleep(REQUEST_SLEEP_SECONDS)
    else:
        for src in SOURCES:
            jobs=fetch_and_parse(src)
            if jobs:
                collected.extend(jobs); used.append(src["name"])
            time.sleep(REQUEST_SLEEP_SECONDS)

    if not collected and os.path.exists("data.json"):
        try:
            prev=json.load(open("data.json","r",encoding="utf-8"))
            prev["transparencyInfo"]["fallback_last_good"]=True
            prev["transparencyInfo"]["runMode"]=RUN_MODE
            open("data.json","w",encoding="utf-8").write(json.dumps(prev,indent=2,ensure_ascii=False))
            logging.warning("No fresh data; served last good snapshot.")
        except Exception:
            save_outputs(collected, used or ["None"])
    else:
        save_outputs(collected, used or ["None"])
