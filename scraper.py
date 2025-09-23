# scraper.py — light seeds + stable id + numberOfPosts extraction + detailLink
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
    except: return {"captureHints":[]}
RULES = load_rules()
HINTS = RULES.get("captureHints", [])

BASE = [
  {"name":"freejobalert","url":"https://www.freejobalert.com/","parser":"parse_generic"},
  {"name":"sarkarijobfind","url":"https://sarkarijobfind.com/","parser":"parse_generic"},
  {"name":"resultbharat","url":"https://www.resultbharat.com/","parser":"parse_generic"},
  {"name":"adda247","url":"https://www.adda247.com/jobs/","parser":"parse_generic"}
]
SEEDS = [{"name":f"hint{i+1}", "url":u, "parser":"dispatch_seed"} for i,u in enumerate(HINTS[:40])]
SOURCES = SEEDS if IS_LIGHT else (SEEDS + BASE)

TEACHER_TERMS = {"teacher","tgt","pgt","prt","faculty","lecturer","assistant professor","professor","b.ed","ctet","tet "}
TECH_TERMS = {"b.tech","btech","b.e","m.tech","m.e","mca","bca","engineer","developer","scientist","architect","analyst","devops","cloud","ml","ai","research"}
PG_TERMS = {"mba","pg ","post graduate","postgraduate","phd","m.phil","mcom","m.com","ma ","m.a","msc","m.sc"}
STATE_NAMES = ["andhra pradesh","arunachal pradesh","assam","bihar","chhattisgarh","goa","gujarat","haryana","himachal pradesh","jharkhand","karnataka","kerala","madhya pradesh","maharashtra","manipur","meghalaya","mizoram","nagaland","odisha","punjab","rajasthan","sikkim","tamil nadu","telangana","tripura","uttar pradesh","uttarakhand","west bengal","jammu","kashmir","ladakh","delhi","puducherry","chandigarh","andaman","nicobar","dadra","nagar haveli","daman","diu","lakshadweep"]
OPEN = ["all india","any state","open to all","pan india","indian citizens","across india","from any state","other state candidates"]
CLOSE = ["domicile","resident","locals only","local candidates","state quota","only for domicile"]

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
    if any(k in t for k in TECH_TERMS|PG_TERMS):
        if ("graduate" in t or "12th" in t or "10th") and "only" not in t:
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
    if any(k in t for k in OPEN): return True
    if "bihar" in t and not any(k in t for k in CLOSE): return True
    for st in STATE_NAMES:
        if st=="bihar": continue
        if st in t and any(k in t for k in CLOSE): return False
    return True

def stable_id(url, title):
    p=urlparse(url or "")
    key=f"{p.netloc}{p.path}".lower()
    return "src_" + hashlib.sha1((key+"|"+(title or "")).encode()).hexdigest()[:16]

POSTS_PAT = re.compile(r"(\d{1,6})\s*(posts?|vacanc(?:y|ies)|seats?)", re.I)
def posts_from_text(txt):
    if not txt: return None
    m = POSTS_PAT.search(txt)
    if m:
        try: return int(m.group(1))
        except: return None
    return None

def build_job(source, base, title, href):
    title=clean(title)
    if not title: return None
    url = href if href and href.startswith("http") else urljoin(base, href or "")
    if not url: return None
    if disallowed(title): return None
    if not skill_ok(title): return None
    if not domicile_ok(title): return None
    edu=education_band(title)
    if edu not in {"10th pass","12th pass","Any graduate"}: return None
    j = {
        "id": stable_id(url, title),
        "title": title,
        "deadline": "N/A",
        "applyLink": url,
        "slug": re.sub(r"[^a-z0-9]+","-", (title or "job").lower())[:80] or "job",
        "qualificationLevel": edu,
        "domicile": "All India",
        "source": "official" if source.startswith("hint") else "aggregator",
        "type": "VACANCY",
        "extractedAt": datetime.utcnow().isoformat()+"Z",
        "meta": {"sourceUrl": base, "sourceSite": source}
    }
    # Posts quick guess from title
    p = posts_from_text(title)
    if p: j["numberOfPosts"]=p
    # detailLink: official in light, aggregator otherwise
    j["detailLink"] = url if source.startswith("hint") else base
    return j

def parse_generic(content, source, base):
    soup=BeautifulSoup(content,"html.parser")
    out=[]
    for a in soup.select("table a[href], main a[href], .entry-content a[href], a[href]"):
        t=clean(a.get_text()); h=a.get("href","")
        if not t or not h: continue
        tl=t.lower()
        if any(x in tl for x in ["admit card","result","answer key","syllabus"]): continue
        if not any(x in (tl+h.lower()) for x in ["recruit","vacan","notific","apply","advert","employment","corrigendum","extension","extended"]): continue
        j=build_job(source, base, t, h)
        if j: out.append(j)
    return out

# Domain-specific seeds (unchanged logic)
def parse_dsssb(content, source, base):
    soup=BeautifulSoup(content,"html.parser"); out=[]
    for a in soup.select('a[href], .view-content a[href]'):
        t=clean(a.get_text()); h=a.get("href","")
        if not t or not h: continue
        if not re.search(r"(advert|advertisement|notice|recruit|vacanc|assistant|superintendent|prison|corrigendum|extension)", t, re.I): continue
        j=build_job(source, base, t, h)
        if j: out.append(j)
    return out

def parse_bssc(content, source, base):
    soup=BeautifulSoup(content,"html.parser"); out=[]
    for a in soup.select('#NoticeBoard a[href], a[href*="Advt"], a[href*="Notice"], a[href*="advert"]'):
        t=clean(a.get_text()); h=a.get("href","")
        if not t or not h: continue
        if not re.search(r"(advt|advertisement|notice|recruit|vacanc|graduate|office attendant|cgl|inter level)", t, re.I): continue
        j=build_job(source, base, t, h)
        if j: out.append(j)
    return out

def parse_ibps(content, source, base):
    soup=BeautifulSoup(content,"html.parser"); out=[]
    for a in soup.select('a[href]'):
        t=clean(a.get_text()); h=a.get("href","")
        if not t or not h: continue
        if not re.search(r"(crp|recruit|vacanc|rrb|po|so|clerk|officer|notification|advert)", t, re.I) and not re.search(r"(crp|recruit|vacanc)", h, re.I):
            continue
        j=build_job(source, base, t, h)
        if j: out.append(j)
    return out

def parse_rbi(content, source, base):
    soup=BeautifulSoup(content,"html.parser"); out=[]
    for tr in soup.select('table tr'):
        a=tr.find('a', href=True)
        if not a: continue
        t=clean(a.get_text()); h=a.get('href')
        if not re.search(r"(recruit|vacanc|officer|grade|advert|notification)", t, re.I): continue
        j=build_job(source, base, t, h)
        if j: out.append(j)
    return out

def dispatch_seed(content, source, base):
    host = urlparse(base).netloc.lower()
    if 'dsssb.delhi.gov.in' in host: return parse_dsssb(content, source, base)
    if 'bssc.bihar.gov.in' in host or 'onlinebssc.com' in host: return parse_bssc(content, source, base)
    if 'ibps.in' in host: return parse_ibps(content, source, base)
    if 'opportunities.rbi.org.in' in host: return parse_rbi(content, source, base)
    return parse_generic(content, source, base)

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
        time.sleep(1.0)
    for j in collected:
        j.setdefault("domicile","All India"); j.setdefault("type","VACANCY")
    transp={"schemaVersion":"1.5","runMode":RUN_MODE,"totalListings":len(collected),"sourcesTried":used,"lastUpdated":datetime.utcnow().isoformat()+"Z"}
    data={"jobListings":collected,"archivedListings":[],"transparencyInfo":transp}
    atomic_write(data)
    json.dump({"ok":bool(collected),**transp}, open("health.json","w",encoding="utf-8"), indent=2)

if __name__=="__main__": main()
