# scraper.py — v10.2 (Critical fixes + low-risk improvements; single file)
# - Publisher-first (FreeJobAlert) publishing
# - Verifiers: Adda247, SarkariExam, SarkariResult.com.cm
# - Official portals disabled
# - Fixes: remove walrus in validate_inline, add looks_official, strict SSL for Cloudscraper
# - Improvements: evidenceHTML snapshot (first 2KB), deadlineMismatch flag
# - 4-day corroboration, reports/submissions queues, daysLeft, userState, auto-archive

import os, re, json, time, random, hashlib, threading, warnings, xml.etree.ElementTree as ET, io
from datetime import datetime, timezone, timedelta, date
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests, certifi, dateparser
from bs4 import BeautifulSoup, UnicodeDammit
from requests.adapters import HTTPAdapter
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from urllib3.util import Retry
    from urllib3.exceptions import InsecureRequestWarning
except Exception:
    Retry = None
    InsecureRequestWarning = None
try:
    import cloudscraper
    CF = cloudscraper.create_scraper()
    try:
        CF.verify = certifi.where()
    except Exception:
        pass
except Exception:
    CF = None
try:
    import pdfplumber
except Exception:
    pdfplumber = None

# ---------------- Paths & Config ----------------
DATA_PATH = Path("data.json")
CACHE_PATH = Path(".cache_http.json")
AGG_CACHE_PATH = Path(".agg_seen.json")
REPORTS_PATH = Path("reports.jsonl")
SUBMISSIONS_PATH = Path("submissions.jsonl")
USER_STATE_PATH = Path("user_state.json")

def env_int(name, default):
    try: return int(os.getenv(name, "").strip() or default)
    except: return default
def env_float(name, default):
    try: return float(os.getenv(name, "").strip() or default)
    except: return default

IS_HARD_RECHECK = (os.getenv("IS_HARD_RECHECK","").lower() in ("1","true","yes"))
UTC_NOW = datetime.now(timezone.utc)

CONNECT_TO = env_int("CONNECT_TIMEOUT", 22 if not IS_HARD_RECHECK else 28)
READ_TO    = env_int("READ_TIMEOUT",    65 if not IS_HARD_RECHECK else 80)
LIST_TO, DETAIL_TO = (CONNECT_TO, READ_TO), (CONNECT_TO, READ_TO)

AGG_WINDOW_DAYS = env_int("CORROBORATION_DAYS", 4)

MAX_WORKERS     = env_int("MAX_WORKERS", 8 if not IS_HARD_RECHECK else 12)
PER_SOURCE_MAX  = env_int("PER_SOURCE_MAX", 160 if not IS_HARD_RECHECK else 220)

RETRY_TOTAL         = env_int("RETRY_TOTAL", 4)
RETRY_CONNECT       = env_int("RETRY_CONNECT", 3)
RETRY_READ          = env_int("RETRY_READ", 3)
BACKOFF_FACTOR      = env_float("BACKOFF_FACTOR", 1.3)
MAX_BACKOFF_SECONDS = env_int("MAX_BACKOFF_SECONDS", 75)

PER_HOST_RPM   = env_int("PER_HOST_RPM", 18 if not IS_HARD_RECHECK else 24)
BASELINE_SLEEP = env_float("BASELINE_SLEEP_S", 1.3 if not IS_HARD_RECHECK else 1.0)
JITTER_MIN     = env_float("JITTER_MIN", 0.7)
JITTER_MAX     = env_float("JITTER_MAX", 2.4)

UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]
if InsecureRequestWarning:
    warnings.simplefilter("ignore", InsecureRequestWarning)

PUBLISHER = {"name":"FreeJobAlert", "base":"https://www.freejobalert.com", "url":"https://www.freejobalert.com/latest-notifications/"}
VERIFIERS = [
  {"name":"Adda247", "base":"https://www.adda247.com", "url":"https://www.adda247.com/jobs/"},
  {"name":"SarkariExam", "base":"https://www.sarkariexam.com", "url":"https://www.sarkariexam.com"},
  {"name":"SarkariResult", "base":"https://sarkariresult.com.cm", "url":"https://sarkariresult.com.cm/"},
]

HOST_RPM = {
  "www.freejobalert.com": 18, "freejobalert.com": 18,
  "www.adda247.com": 10, "adda247.com": 10,
  "www.sarkariexam.com": 10, "sarkariexam.com": 10,
  "sarkariresult.com.cm": 12, "www.sarkariresult.com.cm": 12,
}

# ---------------- HTTP layer ----------------
def session_with_retries(pool=96):
  s = requests.Session()
  s.verify = certifi.where()
  if Retry:
    retry = Retry(
      total=RETRY_TOTAL, connect=RETRY_CONNECT, read=RETRY_READ,
      backoff_factor=BACKOFF_FACTOR,
      status_forcelist=[429,500,502,503,504],
      allowed_methods={"GET","HEAD"},
      respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=pool, pool_maxsize=pool)
  else:
    adapter = HTTPAdapter(pool_connections=pool, pool_maxsize=pool)
  s.mount("http://", adapter); s.mount("https://", adapter)
  return s

HTTP = session_with_retries()
_thread_local = threading.local()
def thread_session():
  if not hasattr(_thread_local, "s"): _thread_local.s = session_with_retries()
  return _thread_local.s

_host_tokens = {}; _host_lock = threading.Lock()
_host_cooldown = {}  # host -> resume_time

def _host(url): return urlparse(url or "").netloc.lower()
def _cooldown_host(host, seconds):
  with _host_lock: _host_cooldown[host] = time.monotonic() + max(0.0, seconds)

def _rate_limit_for(host):
  capacity = HOST_RPM.get(host, PER_HOST_RPM)
  refill = capacity / 60.0
  with _host_lock:
    now = time.monotonic()
    cd = _host_cooldown.get(host, 0.0)
    if now < cd: return cd - now
    b = _host_tokens.get(host, {"t": capacity, "ts": now})
    elapsed = now - b["ts"]
    b["t"] = min(capacity, b["t"] + elapsed * refill)
    if b["t"] < 1.0:
      wait = (1.0 - b["t"]) / refill
      _host_tokens[host] = {"t": b["t"], "ts": now}
      return max(0.0, wait)
    b["t"] -= 1.0; b["ts"] = now; _host_tokens[host] = b
  return 0.0

def _sleep_with_jitter(): time.sleep(BASELINE_SLEEP + random.uniform(JITTER_MIN, JITTER_MAX))

try: _cache=json.loads(CACHE_PATH.read_text(encoding="utf-8"))
except Exception: _cache={}
def _cache_headers_for(url):
  meta=_cache.get(url,{}); h={}
  if "etag" in meta: h["If-None-Match"]=meta["etag"]
  if "last_modified" in meta: h["If-Modified-Since"]=meta["last_modified"]
  return h
def _update_cache_from_response(url, resp):
  et = resp.headers.get("ETag"); lm = resp.headers.get("Last-Modified")
  if et or lm:
    _cache[url] = {}
    if et: _cache[url]["etag"]=et
    if lm: _cache[url]["last_modified"]=lm
def save_http_cache():
  try: CACHE_PATH.write_text(json.dumps(_cache, indent=2, ensure_ascii=False), encoding="utf-8")
  except Exception: pass

def get_dynamic_headers(referer=None):
  langs = [
    "en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
    "en-GB,en;q=0.9",
    "en-US,en;q=0.9",
  ]
  h = {
    "User-Agent": random.choice(UA_POOL),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": random.choice(langs),
    "Upgrade-Insecure-Requests": "1",
    "Connection": "keep-alive",
  }
  if referer: h["Referer"] = referer
  return h

def fetch(url, timeout, use_cache=False, referer=None):
  host = _host(url)
  wait = _rate_limit_for(host)
  if wait>0: time.sleep(wait)
  _sleep_with_jitter()
  headers = get_dynamic_headers(referer=referer)
  if use_cache: headers.update(_cache_headers_for(url))
  to = timeout
  try:
    r = thread_session().get(url, timeout=to, headers=headers, allow_redirects=True)
    if "sorry/index" in (getattr(r, "url", "") or "") and "google.com" in (r.url or ""):
      _cooldown_host(host, 30); raise requests.exceptions.RetryError("Google bot-check redirect")
    if r.status_code == 304:
      class _R: pass
      nr=_R(); nr.status_code=304; nr.headers=r.headers; nr.content=b""; nr.url=url
      return nr
    if r.status_code == 429:
      _cooldown_host(host, 45); time.sleep(2.0)
      alt = thread_session().get(url, timeout=to, headers=get_dynamic_headers(referer=referer), allow_redirects=True)
      if alt.status_code == 200:
        if use_cache: _update_cache_from_response(url, alt); return alt
      _cooldown_host(host, 90); alt.raise_for_status()
    if r.status_code == 403:
      attempts = 2 if host in ("www.adda247.com","adda247.com","www.sarkariexam.com","sarkariexam.com","sarkariresult.com.cm","www.sarkariresult.com.cm") else 1
      last = r
      for _ in range(attempts):
        time.sleep(2.5)
        last = thread_session().get(url, timeout=to, headers=get_dynamic_headers(referer=referer), allow_redirects=True)
        if last.status_code == 200:
          if use_cache: _update_cache_from_response(url, last); return last
      print(f"[BLOCK] 403 from {url}. Attempts={attempts+1}."); last.raise_for_status()
    r.raise_for_status()
    if use_cache: _update_cache_from_response(url, r)
    return r
  except requests.exceptions.SSLError:
    for _ in range(2):
      sess = session_with_retries()
      r2 = sess.get(url, timeout=to, headers=headers, allow_redirects=True)
      try:
        r2.raise_for_status()
        if use_cache: _update_cache_from_response(url, r2)
        return r2
      except Exception:
        continue
    raise

# ---------------- Helpers & parsing ----------------
def norm(s): return re.sub(r"\s+"," ",(s or "")).strip()
def absolute(base, href): return href if (href and href.startswith("http")) else (urljoin(base, href) if href else None)

def looks_official(url):
  try:
    dom = urlparse(url).netloc.lower()
    return dom in {"dsssb.delhi.gov.in","dsssbonline.nic.in","rrbcdg.gov.in","bpsc.bihar.gov.in","ssc.gov.in","ssc.nic.in"}
  except Exception:
    return False

def _decode_html_bytes(resp):
  try:
    content = resp.content
    if not content: return ""
    dammit = UnicodeDammit(content, is_html=True, known_definite_encodings=[resp.encoding or "", "utf-8", "utf-16", "cp1252", "windows-1252", "iso-8859-1"])
    txt = dammit.unicode_markup
    if txt is None:
      try: return content.decode("utf-8", errors="ignore")
      except Exception: return resp.text
    return txt
  except Exception:
    try: return resp.text
    except Exception: return ""

def soup_from_resp(resp):
  ct = (resp.headers.get("Content-Type") or "").lower()
  if ("html" not in ct) and ("text/" not in ct): return None
  html = _decode_html_bytes(resp)
  try: return BeautifulSoup(html, "lxml")
  except Exception: return BeautifulSoup(html, "html.parser")

RECRUITMENT_TERMS = [r"\brecruitment\b", r"\bvacanc(?:y|ies)\b", r"\badvertisement\b", r"\bnotification\b", r"\bonline\s*form\b", r"\bapply\s*online\b"]
EXCLUDE_NOISE = [r"\badmit\s*card\b", r"\banswer\s*key\b", r"\bresult\b", r"\bsyllabus\b", r"\blogin\b"]
UPDATE_TERMS = [r"\bcorrigendum\b", r"\baddendum\b", r"\bamendment\b", r"\brevised\b", r"\bdate\s*(?:extended|extension)\b"]
def contains_any(patterns, text): return any(re.search(p, (text or "").lower()) for p in patterns)
def is_update(text): return contains_any(UPDATE_TERMS, text)
def is_joblike(text): return contains_any(RECRUITMENT_TERMS, text) and not contains_any(EXCLUDE_NOISE, text)
def is_non_vacancy(text): return bool(re.search(r"\b(otr|one\s*time\s*registration)\b", (text or "").lower()))

ALLOWED_EDU = [r"\b10\s*th\b", r"\bmatric\b", r"\b12\s*th\b", r"\binter(?:mediate)?\b", r"\bany\s+graduate\b", r"\bgraduate\b", r"\bbachelor(?:'s)?\s+degree\b"]
EXCLUDE_EDU = [
  r"\bmaster'?s\b|\bm\.?\s?sc\b|\bm\.?\s?a\b|\bm\.?\s?com\b", r"\bmba\b|\bpgdm\b",
  r"\b(b\.?tech|be)\b|\bm\.?tech\b|\bengineering\b", r"\bmca\b|\bbca\b",
  r"\blaw\b|\bllb\b|\bllm\b", r"\bca\b|\bcfa\b|\bcs\b|\bicwa\b|\bcma\b",
  r"\bmbbs\b|\bbds\b|\bnursing\b|\bgnm\b|\banm\b|\bpharm\b", r"\bphd\b|\bdoctorate\b"
]
def education_allowed(text):
  t=(text or "").lower()
  if any(re.search(p, t) for p in EXCLUDE_EDU): return False
  return any(re.search(p, t) for p in ALLOWED_EDU)
def infer_qualification(text):
  t=(text or "").lower()
  if any(re.search(p, t) for p in [r"\b10\s*th\b", r"\bmatric\b"]): return "10th Pass"
  if any(re.search(p, t) for p in [r"\b12\s*th\b", r"\binter\b"]): return "12th Pass"
  if any(re.search(p, t) for p in [r"\bany\s+graduate\b", r"\bgraduate\b"]): return "Graduate"
  return "Graduate"

DATE_WORDS = {"jan":"january","feb":"february","mar":"march","apr":"april","jun":"june","jul":"july","aug":"august","sep":"september","oct":"october","nov":"november","dec":"december"}
def _month_word_fix(s):
  t=s.lower()
  for k,v in DATE_WORDS.items(): t=re.sub(rf"\b{k}\b",v,t)
  return re.sub(r"[–—−]", "-", t)
PAT_TILL = re.compile(r"(till|closes\s*on|last\s*date)\s*[:\-]?\s*(\d{1,2}\s+[a-z]+(?:\s+\d{4})?|\d{1,2}[-/]\d{1,2}[-/]\d{2,4})", re.I)
def parse_application_window(text):
  t=_month_word_fix(norm(text or ""))
  m = PAT_TILL.search(t)
  if m:
    dt=dateparser.parse(m.group(2), settings={"DATE_ORDER":"DMY","PREFER_DATES_FROM":"future"})
    return (None, dt.date().isoformat() if dt else None)
  return (None,None)

def probe_deadline_from_link(url):
  try:
    r = fetch(url, DETAIL_TO)
    ct = (r.headers.get("Content-Type","").lower())
    if "html" in ct:
      s = soup_from_resp(r)
      if s:
        _, e = parse_application_window(norm(s.get_text(" ")[:4000]))
        return e
    elif pdfplumber and ("pdf" in ct or "pdf" in url.lower()):
      with pdfplumber.open(io.BytesIO(r.content)) as pdf:
        for i in range(min(3, len(pdf.pages))):
          text = pdf.pages[i].extract_text() or ""
          _, e = parse_application_window(text)
          if e: return e
  except Exception:
    return None
  return None

def extract_deadline(text, fallback_url=None):
  _,e = parse_application_window(text or "")
  if e: return e
  if fallback_url:
    e2 = probe_deadline_from_link(fallback_url)
    if e2: return e2
  dt = dateparser.parse(text or "", settings={"PREFER_DATES_FROM":"future","DATE_ORDER":"DMY"})
  return dt.date().isoformat() if (dt and dt.date() >= datetime.now().date()) else None

def days_left(deadline_iso):
  try:
    d = datetime.fromisoformat(deadline_iso).date()
    return (d - date.today()).days
  except Exception:
    return None

def norm_key(title):
  t = norm(title).lower()
  t = re.sub(r"(recruitment|notification|advertisement|apply\s*online|online\s*form|\b20\d{2})"," ",t)
  t = re.sub(r"[^a-z0-9\s]", " ", t)
  tokens = [w for w in t.split() if len(w) > 2]
  return " ".join(tokens[:14])

# ---------------- Corroboration cache ----------------
def load_agg_seen():
  try:
    data = json.loads(AGG_CACHE_PATH.read_text(encoding="utf-8"))
    if AGG_WINDOW_DAYS <= 0: return {}
    cutoff = datetime.utcnow() - timedelta(days=AGG_WINDOW_DAYS)
    pruned = {}
    for k, info in data.items():
      kept=[]
      for sname, ts in info.get("seen", []):
        try:
          dt = datetime.fromisoformat(ts.replace("Z",""))
          if dt >= cutoff: kept.append((sname, ts))
        except: continue
      if kept: pruned[k] = {"seen": kept}
    return pruned
  except: return {}
def save_agg_seen(cache):
  try: AGG_CACHE_PATH.write_text(json.dumps(cache, indent=2, ensure_ascii=False), encoding="utf-8")
  except: pass

# ---------------- Queues & user state ----------------
def read_jsonl(path):
  items=[]
  if Path(path).exists():
    with open(path, "r", encoding="utf-8") as f:
      for line in f:
        line=line.strip()
        if not line: continue
        try: items.append(json.loads(line))
        except: continue
  return items

def read_user_state():
  try: return json.loads(USER_STATE_PATH.read_text(encoding="utf-8"))
  except: return {}

# ---------------- Scraping ----------------
def try_get_text(url, referer=None):
  try:
    r = fetch(url, DETAIL_TO, referer=referer)
    # Evidence snapshot: first 2KB raw HTML/text
    ev = r.content[:2048].decode("utf-8", errors="ignore") if getattr(r, "content", None) else ""
    text = ""
    if "html" in (r.headers.get("Content-Type","") or "").lower():
      s=soup_from_resp(r); text = norm(s.get_text(" ")) if s else ""
    return text, ev
  except Exception:
    return "", ""

def validate_inline(rec):
  if not (rec.get("title") and len(rec["title"])>6): return False
  if not (rec.get("detailText") and len(rec["detailText"])>40): return False
  if not (is_valid_url(rec.get("applyLink")) or is_valid_url(rec.get("pdfLink"))): return False
  return True

def is_valid_url(u):
  try:
    if not u: return False
    pr = urlparse(u); return pr.scheme in ("http","https") and pr.netloc
  except: return False

def scrape_list(src, treat_as, metrics):
  name = src["name"]; url = src["url"]; base = src["base"]
  started=time.time(); raw=0; hinted=0; kept=0; items=[]
  try:
    r = fetch(url, LIST_TO, use_cache=True)
    if r.status_code == 304:
      m=metrics[name]; m["not_modified"]=m.get("not_modified",0)+1; m["durations"].append(time.time()-started)
      return []
    soup = soup_from_resp(r)
    if soup is None:
      metrics[name]["fail"]=metrics[name].get("fail",0)+1; return []
    anchors=[]
    for a in soup.find_all("a", href=True):
      href = absolute(base, a["href"]); title = norm(a.get_text(" "))
      if not href or len(title)<6: continue
      raw += 1
      if (treat_as!="publisher" and treat_as!="verifier") or is_joblike(f"{title} {href}"):
        anchors.append((title, href)); hinted += 1
      if len(anchors)>=PER_SOURCE_MAX: break
    for title, href in anchors:
      detail_text, evidence = try_get_text(href, referer=url)
      if not detail_text: detail_text = title  # minimal fallback
      if is_non_vacancy(detail_text) or not education_allowed(detail_text): continue
      key_base = norm_key(title); upd=is_update(detail_text)

      deadline_pub = extract_deadline(detail_text, fallback_url=href)

      rec = {
        "key": key_base, "uniqueKey": f"{key_base}|upd" if upd else key_base,
        "title": title, "organization": name,
        "applyLink": href, "pdfLink": href,
        "deadline": deadline_pub,
        "domicile": "Bihar" if "bihar" in detail_text.lower() else "All India",
        "sourceType": treat_as, "source": name,
        "isUpdate": upd, "updateSummary": title if upd else None,
        "detailText": detail_text,
        "evidenceHTML": evidence
      }
      if validate_inline(rec):
        items.append(rec); kept+=1
    metrics[name]["ok"]=metrics[name].get("ok",0)+1; metrics[name]["durations"].append(time.time()-started)
    metrics[name]["raw"]=metrics[name].get("raw",0)+raw; metrics[name]["hinted"]=metrics[name].get("hinted",0)+hinted; metrics[name]["kept"]=metrics[name].get("kept",0)+kept
  except Exception as e:
    metrics[name]["fail"]=metrics[name].get("fail",0)+1
    metrics[name].setdefault("error_samples",[]).append(f"{type(e).__name__}:{str(e)[:160]}")
  return items

# ---------------- Merge & Publish (publisher-first) ----------------
def publish_from_publisher(pub_items, ver_items):
  agg_seen = load_agg_seen()
  published=[]; seen_ids=set()

  for it in pub_items + ver_items:
    if it.get("sourceType") in ("publisher","verifier"):
      rec = agg_seen.setdefault(it["key"], {"seen": []})
      if it["source"] not in [s for s,_ in rec["seen"]]:
        rec["seen"].append((it["source"], UTC_NOW.strftime("%Y-%m-%dT%H:%M:%SZ")))
  save_agg_seen(agg_seen)

  # Build quick lookup of verifier deadlines per key to flag mismatches
  ver_deadline_by_key = {}
  for v in ver_items:
    if v.get("deadline"):
      ver_deadline_by_key.setdefault(v["key"], set()).add(v["deadline"])

  buckets={}
  def add(it):
    b=buckets.setdefault(it["key"], {"pub":[], "ver":set(), "items":[]})
    if it["sourceType"]=="publisher": b["pub"].append(it)
    elif it["sourceType"]=="verifier": b["ver"].add(it["source"])
    b["items"].append(it)
  for it in pub_items: add(it)
  for it in ver_items: add(it)

  for key, b in buckets.items():
    rep = next((x for x in b["pub"] if not x.get("isUpdate")), None) or (b["pub"][0] if b["pub"] else None)
    if not rep: continue
    dl = rep.get("deadline"); ok_deadline=False
    if dl:
      try: ok_deadline = dateparser.parse(dl, settings={"DATE_ORDER":"DMY"}).date() >= datetime.now().date()
      except Exception: ok_deadline=False
    if not ok_deadline: continue

    verifiedBy = "publisher"
    if len(b["ver"]) >= 1: verifiedBy = "multi-aggregator"

    rid_base = hashlib.sha1(f"{rep['uniqueKey']}|{verifiedBy}".encode("utf-8")).hexdigest()[:12]
    rid = rid_base; n=1
    while rid in seen_ids:
      rid = hashlib.sha1(f"{rid_base}|{n}".encode("utf-8")).hexdigest()[:12]; n+=1
    seen_ids.add(rid)

    dleft = days_left(rep["deadline"])
    mismatch = False
    if key in ver_deadline_by_key:
      mismatch = (rep["deadline"] not in ver_deadline_by_key[key])

    rep_rec = {
      "id": rid, "slug": rid, "title": rep["title"],
      "organization": rep["organization"],
      "qualificationLevel": infer_qualification(rep["detailText"]),
      "domicile": rep["domicile"], "source": "aggregator",
      "verifiedBy": verifiedBy, "type": "VACANCY",
      "updateSummary": None, "relatedTo": None,
      "deadline": rep["deadline"], "daysLeft": dleft,
      "applyLink": rep.get("applyLink"), "pdfLink": rep.get("pdfLink"),
      "extractedAt": UTC_NOW.strftime("%Y-%m-%dT%H:%M:%SZ"),
      "flags": {"hidden": False, "reported": False, "reportReason": None, "evidenceUrl": None, "deadlineMismatch": mismatch},
      "userState": None,
      "evidenceHTML": rep.get("evidenceHTML","")
    }
    published.append(rep_rec)

    for u in (x for x in b["items"] if x.get("isUpdate")):
      uid_base = hashlib.sha1(f"{u['uniqueKey']}|upd".encode("utf-8")).hexdigest()[:12]
      uid=uid_base; m=1
      while uid in seen_ids:
        uid = hashlib.sha1(f"{uid_base}|{m}".encode("utf-8")).hexdigest()[:12]; m+=1
      seen_ids.add(uid)
      upd = {
        "id": uid, "slug": uid, "title": "[UPDATE] " + u["title"],
        "organization": rep_rec["organization"],
        "qualificationLevel": infer_qualification(u["detailText"]),
        "domicile": rep["domicile"], "source": "aggregator",
        "verifiedBy": verifiedBy, "type": "UPDATE",
        "updateSummary": u.get("updateSummary"), "relatedTo": rep_rec["slug"],
        "deadline": u.get("deadline") or rep_rec["deadline"],
        "daysLeft": days_left(u.get("deadline") or rep_rec["deadline"]),
        "applyLink": u.get("applyLink") or rep_rec["applyLink"], "pdfLink": u.get("pdfLink") or rep_rec["pdfLink"],
        "extractedAt": UTC_NOW.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "flags": {"hidden": False, "reported": False, "reportReason": None, "evidenceUrl": None, "deadlineMismatch": False},
        "userState": None,
        "evidenceHTML": u.get("evidenceHTML","")
      }
      published.append(upd)
  return published

# ---------------- Feedback & Learning ----------------
def apply_reports_and_learning(items, reports):
  reasons_count = {}
  for r in reports:
    lid = r.get("listingId"); reason = (r.get("reason") or "Other").strip()
    for it in items:
      if it["id"] == lid:
        it["flags"]["hidden"] = True; it["flags"]["reported"] = True
        it["flags"]["reportReason"] = reason
        it["flags"]["evidenceUrl"] = r.get("evidenceUrl") or None
        break
    reasons_count[reason] = reasons_count.get(reason, 0) + 1
  return reasons_count

def self_learn_adjustments(reasons_count, metrics):
  if reasons_count.get("Not a vacancy", 0) >= 3:
    if r"\bnotification\s*pdf\b" not in EXCLUDE_NOISE: EXCLUDE_NOISE.append(r"\bnotification\s*pdf\b")
  if reasons_count.get("Wrong eligibility", 0) >= 3:
    if r"\bpost\b" not in RECRUITMENT_TERMS: RECRUITMENT_TERMS.append(r"\bpost\b")
  metrics.setdefault("learning", {})["reportReasons"] = reasons_count

def attach_user_state(items, user_state_map):
  for it in items:
    if it["id"] in user_state_map:
      it["userState"] = user_state_map[it["id"]]

def archive_past_deadline(items):
  today = date.today()
  active=[]; archived=[]
  for it in items:
    try:
      if it.get("deadline"):
        d = datetime.fromisoformat(it["deadline"]).date()
        if d < today:
          archived.append(it); continue
        it["daysLeft"] = (d - today).days
    except Exception:
      pass
    active.append(it)
  return active, archived

# ---------------- Data IO ----------------
def load_data():
  base={"jobListings": [], "archivedListings": [], "transparencyInfo": {}}
  if DATA_PATH.exists():
    try: base=json.loads(DATA_PATH.read_text(encoding="utf-8"))
    except Exception: pass
  base.setdefault("jobListings",[]); base.setdefault("archivedListings",[]); base.setdefault("transparencyInfo",{})
  return base
def save_data(data): DATA_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

# ---------------- Main ----------------
def main():
  run_started = time.time()
  metrics = {PUBLISHER["name"]: {"ok":0,"fail":0,"skipped_due_cb":0,"not_modified":0,"raw":0,"hinted":0,"kept":0,"error_samples":[],"durations":[]}}
  for v in VERIFIERS:
    metrics[v["name"]]={"ok":0,"fail":0,"skipped_due_cb":0,"not_modified":0,"raw":0,"hinted":0,"kept":0,"error_samples":[],"durations":[]}

  publisher_items = scrape_list(PUBLISHER, "publisher", metrics)

  verifier_items=[]
  with ThreadPoolExecutor(max_workers=min(len(VERIFIERS), 4)) as ex:
    futs={ex.submit(scrape_list, v, "verifier", metrics): v["name"] for v in VERIFIERS}
    for f in as_completed(futs):
      try: verifier_items.extend(f.result())
      except Exception as e:
        nm=futs[f]; metrics[nm]["fail"]=metrics[nm].get("fail",0)+1; metrics[nm].setdefault("error_samples",[]).append(str(e)[:160])

  published = publish_from_publisher(publisher_items, verifier_items)

  reports = read_jsonl(REPORTS_PATH)
  submissions = read_jsonl(SUBMISSIONS_PATH)
  user_state = read_user_state()

  reasons_count = apply_reports_and_learning(published, reports)
  self_learn_adjustments(reasons_count, metrics)
  attach_user_state(published, user_state)
  active, archived_new = archive_past_deadline(published)

  data = load_data()
  prev_ids = {it["id"] for it in data["jobListings"]}
  active_ids = {it["id"] for it in active}
  moved_to_archive = [it for it in data["jobListings"] if it["id"] not in active_ids and it["id"] in prev_ids]
  data["archivedListings"].extend(archived_new + moved_to_archive)
  data["jobListings"] = active

  duration = time.time() - run_started
  ti = data.setdefault("transparencyInfo",{})
  ti["lastUpdated"] = UTC_NOW.strftime("%Y-%m-%dT%H:%M:%SZ")
  ti["totalActive"] = len(active)
  ti["totalArchived"] = len(data["archivedListings"])
  ti["publisherCounts"] = {PUBLISHER["name"]: len(publisher_items)}
  ti["verifierCounts"] = {v["name"]: sum(1 for x in verifier_items if x["organization"]==v["name"]) for v in VERIFIERS}
  ti["reportReasons"] = reasons_count
  ti["notes"] = "v10.2: FJA publisher-first; verifiers Adda247,SarkariExam,SarkariResult; strict SSL; evidenceHTML; deadlineMismatch; 4-day corroboration; daysLeft; userState."
  ti["runDurationSec"] = round(duration,2)
  ti["submissionsQueued"] = len(submissions)

  save_data(data); save_http_cache()
  print(f"[INFO] active={len(active)} archived+={len(archived_new)} duration={round(duration,2)}s")

if __name__ == "__main__":
  main()
