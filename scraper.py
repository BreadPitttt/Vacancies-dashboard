#!/usr/bin/env python3
# scraper.py — official-first + reopened detector + aggregator tie-breaks (keeps original two at top)
import requests
from bs4 import BeautifulSoup
import json, logging, re, os, time, argparse, hashlib, pathlib
from datetime import datetime
from urllib.parse import urljoin, urlparse

ap = argparse.ArgumentParser()
ap.add_argument("--mode", default=os.getenv("RUN_MODE","nightly"))
RUN_MODE = (ap.parse_args().mode or "nightly").lower()
IS_LIGHT = RUN_MODE == "light"

CACHE = pathlib.Path(".cache"); CACHE.mkdir(exist_ok=True)
def ck(u): return CACHE / (hashlib.sha1(u.encode()).hexdigest()+".html")
def get(u, ttl=0, timeout=20):
    f = ck(u)
    if ttl>0 and f.exists() and time.time()-f.stat().st_mtime < ttl:
        return f.read_bytes()
    try:
        r = requests.get(u, headers={"User-Agent":"Mozilla/5.0"}, timeout=timeout)
        r.raise_for_status()
        f.write_bytes(r.content)
        return r.content
    except Exception:
        return b""

TTL = 0 if RUN_MODE=="nightly" else (6*3600 if RUN_MODE=="light" else 72*3600)

def load_rules(path="rules.json"):
    try: return json.load(open(path,"r",encoding="utf-8"))
    except: return {"captureHints":[], "aggregatorScores":{}}
RULES = load_rules()
HINTS = RULES.get("captureHints", [])
AGG_SCORES = RULES.get("aggregatorScores", {})

# Keep original two aggregators at the top positions
BASE = [
  {"name":"freejobalert","url":"https://www.freejobalert.com/","parser":"parse_generic"},
  {"name":"sarkarijobfind","url":"https://sarkarijobfind.com/","parser":"parse_generic"},
  {"name":"resultbharat","url":"https://www.resultbharat.com/","parser":"parse_generic"},
  {"name":"rojgarresult","url":"https://www.rojgarresult.com/","parser":"parse_generic"},
  {"name":"adda247","url":"https://www.adda247.com/jobs/","parser":"parse_generic"}
]
SEEDS = [{"name":f"hint{i+1}", "url":u, "parser":"dispatch_seed"} for i,u in enumerate(HINTS[:100])]
SOURCES = SEEDS if IS_LIGHT else (SEEDS + BASE)

TEACHER_TERMS = {"teacher","tgt","pgt","prt","faculty","lecturer","assistant professor","professor","b.ed","ctet","tet "}
TECH_TERMS = {"b.tech","btech","b.e","m.tech","m.e","mca","bca","developer","architect","analyst","devops","cloud","ml","ai","research"}
PG_TERMS = {"mba","pg ","post graduate","postgraduate","phd","m.phil","mcom","m.com","ma ","m.a","msc","m.sc"}
REOPEN_TOK = re.compile(r"\b(re-?open|re-?opened|reopening|corrigendum|extension|extended|last\s*date|addendum|amendment)\b", re.I)

def clean(s): return re.sub(r"\s+"," ", (s or "").strip())
def education_band(text):
    t=(text or "").lower()
    if any(k in t for k in ["10th","matric","ssc "]): return "10th pass"
    if any(k in t for k in ["12th","intermediate","hsc"]): return "12th pass"
    if "any graduate" in t or "any degree" in t or "graduate" in t: return "Any graduate"
    return "N/A"
def disallowed(text):
    t=(text or "").lower()
    if any(k in t for k in TEACHER_TERMS): return True
    # allow “assistant” generic posts if qualification band fits
    if any(k in t for k in TECH_TERMS|PG_TERMS):
        if ("graduate" in t or "12th" in t or "10th") and "only" not in t:
            return False
        return True
    return False
def stable_id(url, title):
    p=urlparse(url or "")
    return "src_" + hashlib.sha1((f"{p.netloc}{p.path}|{title or ''}".lower()).encode()).hexdigest()[:16]

def build_job(source, base, title, href):
    title=clean(title)
    if not title: return None
    url = href if href and href.startswith("http") else urljoin(base, href or "")
    if not url: return None
    if disallowed(title): return None
    edu=education_band(title)
    if edu not in {"10th pass","12th pass","Any graduate"}: return None
    j = {
        "id": stable_id(url, title),
        "title": title,
        "deadline": "N/A",
        "applyLink": url,
        "detailLink": url,
        "qualificationLevel": edu,
        "domicile": "All India",
        "source": "official" if source.startswith("hint") else "aggregator",
        "type": "UPDATE" if REOPEN_TOK.search(title) else "VACANCY",
        "extractedAt": datetime.utcnow().isoformat()+"Z",
        "meta": {"sourceUrl": base, "sourceSite": source}
    }
    return j

def allow_link_text(t, h):
    tl=t.lower(); hl=(h or "").lower()
    # allow vacancies and reopened/extension notices
    if any(x in tl for x in ["recruit","vacan","notific","apply","advert","employment","assistant","officer","constable","clerk"]): return True
    if REOPEN_TOK.search(tl) or REOPEN_TOK.search(hl): return True
    return False

def parse_generic(content, source, base):
    soup=BeautifulSoup(content,"html.parser"); out=[]
    for a in soup.select("table a[href], main a[href], .entry-content a[href], a[href]"):
        t=clean(a.get_text()); h=a.get("href","")
        if not t or not h: continue
        if not allow_link_text(t,h): continue
        j=build_job(source, base, t, h)
        if j: out.append(j)
    return out

def parse_official_like(content, source, base):
    soup=BeautifulSoup(content,"html.parser"); out=[]
    for a in soup.select('a[href]'):
        t=clean(a.get_text()); h=a.get("href","")
        if not t or not h: continue
        if not allow_link_text(t,h): continue
        j=build_job(source, base, t, h)
        if j: out.append(j)
    return out

def dispatch_seed(content, source, base):
    host=urlparse(base).netloc.lower()
    # generic handler is fine; sites with tables/notice boards also work here
    return parse_official_like(content, source, base)

PARSERS={"parse_generic":parse_generic,"dispatch_seed":dispatch_seed}

def fetch_and_parse(src):
    html = get(src["url"], ttl=TTL, timeout=20)
    if not html: return []
    fn = PARSERS.get(src["parser"])
    if not fn: return []
    try: return fn(html, src["name"], src["url"])
    except Exception: return []

def atomic_write(obj):
    pending="data.pending.json"; final="data.json"
    with open(pending,"w",encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)
    ok = isinstance(obj.get("jobListings"), list) and all(j.get("applyLink") for j in obj["jobListings"])
    if ok: os.replace(pending, final)
    else: os.remove(pending)

def main():
    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
    collected=[]; used=[]; start=time.time(); N_MIN=25; T_MAX=60
    for s in SOURCES:
        items=fetch_and_parse(s)
        if items:
            collected.extend(items); used.append(s["name"])
        if not IS_LIGHT and (len(collected)>=N_MIN or (time.time()-start)>T_MAX): break
        time.sleep(0.8)
    # Prefer official; if aggregator duplicates exist, pick the one with higher aggregatorScores
    seen={}
    for j in collected:
        k = (j["title"].lower(), urlparse(j["applyLink"]).path.lower())
        if k not in seen:
            seen[k]=j
        else:
            a=seen[k]; b=j
            if a["source"]=="official" and b["source"]!="official": continue
            if b["source"]=="official" and a["source"]!="official": seen[k]=b; continue
            # both aggregators: prefer higher score; original two keep higher defaults via rules.json
            sa=AGG_SCORES.get(urlparse(a["meta"]["sourceUrl"]).netloc, 0.5)
            sb=AGG_SCORES.get(urlparse(b["meta"]["sourceUrl"]).netloc, 0.5)
            if sb>sa: seen[k]=b
    final=list(seen.values())
    for j in final:
        j.setdefault("domicile","All India")
    transp={"schemaVersion":"1.5","runMode":RUN_MODE,"totalListings":len(final),"sourcesTried":used,"lastUpdated":datetime.utcnow().isoformat()+"Z"}
    data={"jobListings":final,"archivedListings":[],"transparencyInfo":transp}
    atomic_write(data)
    json.dump({"ok":bool(final),**transp}, open("health.json","w",encoding="utf-8"), indent=2)

if __name__=="__main__": main()
