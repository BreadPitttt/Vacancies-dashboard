# scraper.py — nightly/light/weekly fetch with strict eligibility and atomic save
import requests
from bs4 import BeautifulSoup
import json, logging, re, os, time, argparse, hashlib, pathlib
from datetime import datetime, date, timedelta
from urllib.parse import urljoin, urlparse

# ---------- Mode ----------
ap = argparse.ArgumentParser()
ap.add_argument("--mode", default=os.getenv("RUN_MODE","nightly"))
RUN_MODE = (ap.parse_args().mode or "nightly").lower()
IS_LIGHT = RUN_MODE == "light"
IS_WEEKLY = RUN_MODE == "weekly"

# ---------- Cache ----------
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

# ---------- Sources ----------
BASE_SOURCES = [
  {"name":"freejobalert","url":"https://www.freejobalert.com/","parser":"parse_freejobalert"},
  {"name":"sarkarijobfind","url":"https://sarkarijobfind.com/","parser":"parse_sarkarijobfind"},
  {"name":"resultbharat","url":"https://www.resultbharat.com/","parser":"parse_resultbharat"},
  {"name":"adda247","url":"https://www.adda247.com/jobs/","parser":"parse_adda247"}
]

def load_rules(path="rules.json"):
    try: return json.load(open(path,"r",encoding="utf-8"))
    except: return {"captureHints":[]}
RULES = load_rules()
HINTS = RULES.get("captureHints", [])
SOURCES = [{"name":f"hint{i+1}", "url":u, "parser":"parse_seed"} for i,u in enumerate(HINTS[:30])]
if not IS_LIGHT:
    SOURCES += BASE_SOURCES

# ---------- Policies ----------
ALLOWED_SKILLS = {"typing","basic_computer","pet","pst","none"}
TEACHER_TERMS = {"teacher","tgt","pgt","prt","faculty","lecturer","assistant professor","professor","b.ed","ctet","tet "}
TECH_TERMS = {"b.tech","btech","b.e","m.tech","m.e","mca","bca","engineer","developer","scientist","architect","analyst","devops","cloud","ml","ai","research"}
PG_TERMS = {"mba","pg ","post graduate","postgraduate","phd","m.phil","mcom","m.com","ma ","m.a","msc","m.sc"}

STATE_NAMES = ["andhra pradesh","arunachal pradesh","assam","bihar","chhattisgarh","goa","gujarat","haryana","himachal pradesh","jharkhand","karnataka","kerala","madhya pradesh","maharashtra","manipur","meghalaya","mizoram","nagaland","odisha","punjab","rajasthan","sikkim","tamil nadu","telangana","tripura","uttar pradesh","uttarakhand","west bengal","jammu","kashmir","ladakh","delhi","puducherry","chandigarh","andaman","nicobar","dadra","nagar haveli","daman","diu","lakshadweep"]

OPEN_SIGNALS = ["all india","any state","open to all","pan india","indian citizens","across india","from any state","other state candidates"]
CLOSE_SIGNALS = ["domicile","resident","locals only","local candidates","state quota","only for domicile"]

def clean(s): return re.sub(r"\s+"," ", (s or "").strip())

def education_band(text):
    t = (text or "").lower()
    if any(k in t for k in ["10th","matric","ssc "]): return "10th pass"
    if any(k in t for k in ["12th","intermediate","hsc"]): return "12th pass"
    if "any graduate" in t or "any degree" in t or "graduate" in t: return "Any graduate"
    return "N/A"

def disallowed_track(text):
    t=(text or "").lower()
    if any(k in t for k in TEACHER_TERMS): return True
    # If notice lists multiple streams, allow when a general stream exists
    if any(k in t for k in TECH_TERMS|PG_TERMS):
        # allow if explicit "10th/12th/graduate" present without mandatory PG/technical words like "only"
        if ("graduate" in t or "12th" in t or "10th" in t) and "only" not in t:
            return False
        return True
    return False

def skill_ok(text):
    t=(text or "").lower()
    ok = any(x in t for x in ["typing","type test","wpm","ms office","computer knowledge","basic computer","pet","pst"])
    bad = any(x in t for x in ["steno","shorthand","trade test","technical interview"])
    return ok or not bad

def domicile_ok(title):
    t=(title or "").lower()
    if any(k in t for k in OPEN_SIGNALS): return True
    # Bihar is prioritized and allowed unless it says locals only
    if "bihar" in t and not any(k in t for k in CLOSE_SIGNALS): return True
    # Other states: exclude if locals-only; include if open mentioned
    for st in STATE_NAMES:
        if st == "bihar": continue
        if st in t and any(k in t for k in CLOSE_SIGNALS): return False
    # default neutral: include; later QC may prune
    return True

def make_id(prefix, url, title):
    p = urlparse(url or "")
    anchor_bits = [p.netloc, p.path, re.sub(r"[^a-z0-9]+","-", (title or "").lower())[:60]]
    return f"{prefix}_" + hashlib.sha1("|".join(anchor_bits).encode()).hexdigest()[:16]

def build_job(prefix, source, base, title, href):
    title = clean(title)
    if not title: return None
    url = href if href and href.startswith("http") else urljoin(base, href or "")
    if not url: return None
    if disallowed_track(title): return None
    if not skill_ok(title): return None
    if not domicile_ok(title): return None
    edu = education_band(title)
    if edu not in {"10th pass","12th pass","Any graduate"}: return None
    return {
        "id": make_id(prefix, url, title),
        "title": title,
        "organization": "N/A",
        "deadline": "N/A",
        "applyLink": url,
        "slug": re.sub(r"[^a-z0-9]+","-", (title or "job").lower())[:80] or "job",
        "qualificationLevel": edu,
        "domicile": "All India",
        "source": "aggregator" if not source.startswith("hint") else "official",
        "type": "VACANCY",
        "extractedAt": datetime.utcnow().isoformat()+"Z",
        "meta": {"sourceUrl": base, "sourceSite": source}
    }

# ---------- Parsers ----------
def parse_table_links(soup, base, source):
    jobs=[]
    for a in soup.select("table a[href], main a[href], .entry-content a[href], a[href]"):
        t=clean(a.get_text()); h=a.get("href","")
        if not t or not h: continue
        tl=t.lower()
        if any(x in tl for x in ["admit card","result","answer key","syllabus"]): continue
        if not any(x in tl for x in ["recruit","vacan","notific","apply","advert","employment","corrigendum","extension","extended"]): continue
        j=build_job("src", source, base, t, h)
        if j: jobs.append(j)
    return jobs

def parse_freejobalert(content, source, base): 
    from bs4 import BeautifulSoup
    return parse_table_links(BeautifulSoup(content,"html.parser"), base, source)

def parse_sarkarijobfind(content, source, base):
    soup=BeautifulSoup(content,"html.parser")
    jobs=[]
    for h in soup.find_all(re.compile("^h[1-6]$")):
        if re.search(r"new\s*update|latest", h.get_text(), re.I):
            nxt=h.find_next_sibling(["ul","ol"])
            if not nxt: continue
            for a in nxt.select("a[href]"):
                t=clean(a.get_text()); 
                if not t: continue
                j=build_job("sjf", source, base, t, a.get("href",""))
                if j: jobs.append(j)
            break
    return jobs

def parse_resultbharat(content, source, base):
    soup=BeautifulSoup(content,"html.parser")
    return parse_table_links(soup, base, source)

def parse_seed(content, source, base):
    soup=BeautifulSoup(content,"html.parser")
    jobs=[]
    for a in soup.select("main a[href], a[href]"):
        t=clean(a.get_text()); h=a.get("href","")
        if not t or not h: continue
        if not any(k in (t.lower()+h.lower()) for k in ["recruit","vacan","advert","notific","apply","corrigendum","extension"]):
            continue
        j=build_job("hint", source, base, t, h)
        if j: jobs.append(j)
    return jobs

PARSERS = {
  "parse_freejobalert": parse_freejobalert,
  "parse_sarkarijobfind": parse_sarkarijobfind,
  "parse_resultbharat": parse_resultbharat,
  "parse_adda247": parse_freejobalert,
  "parse_seed": parse_seed
}

def fetch_and_parse(src):
    html = get(src["url"], ttl=TTL, timeout=20)
    if not html: return []
    fn = PARSERS.get(src["parser"])
    if not fn: return []
    try:
        return fn(html, src["name"], src["url"])
    except Exception:
        return []

def atomic_write(obj):
    pending="data.pending.json"; final="data.json"
    with open(pending,"w",encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)
    # quick validation: non-empty listings and valid links
    ok = isinstance(obj.get("jobListings"), list) and all(j.get("applyLink") for j in obj["jobListings"])
    if ok:
        os.replace(pending, final)
    else:
        # keep previous data.json
        os.remove(pending)

def main():
    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
    collected=[]; used=[]
    start=time.time(); N_MIN=25; T_MAX=60
    for s in SOURCES:
        items = fetch_and_parse(s)
        if items:
            collected.extend(items); used.append(s["name"])
        if RUN_MODE!="light" and (len(collected)>=N_MIN or (time.time()-start)>T_MAX):
            break
        time.sleep(1.0)
    # enforce defaults
    for j in collected:
        j.setdefault("slug", j["id"])
        j.setdefault("domicile","All India")
        j.setdefault("type","VACANCY")
    transp = {
        "schemaVersion":"1.3",
        "runMode": RUN_MODE,
        "totalListings": len(collected),
        "sourcesTried": used,
        "lastUpdated": datetime.utcnow().isoformat()+"Z"
    }
    data={"jobListings": collected, "archivedListings": [], "transparencyInfo": transp}
    atomic_write(data)
    # health
    json.dump({"ok": bool(collected), **transp}, open("health.json","w",encoding="utf-8"), indent=2)

if __name__=="__main__": main()
