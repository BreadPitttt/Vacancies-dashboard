# scraper.py — Aggregators-first discovery with official-first verification
# Fix: remove illegal walrus assignment on subscript; safe metrics increments.
# Resilience: backoff+jitter, per-host rate limit, circuit breakers, caching.
# Strict open-window: only publish with valid, non-expired deadlines.

import os, re, json, time, random, hashlib, threading, xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import urljoin, urlparse
import requests, certifi, dateparser
from bs4 import BeautifulSoup, UnicodeDammit
from requests.adapters import HTTPAdapter
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from urllib3.util import Retry
except Exception:
    Retry = None
try:
    import cloudscraper
    CF = cloudscraper.create_scraper()
except Exception:
    CF = None

# ------------ Config ------------
def env_int(name, default): 
    try: return int(os.getenv(name, "").strip() or default)
    except: return default
def env_float(name, default):
    try: return float(os.getenv(name, "").strip() or default)
    except: return default

IS_HARD_RECHECK = (os.getenv("IS_HARD_RECHECK","").lower() in ("1","true","yes"))
DATA_PATH = Path("data.json")
CACHE_PATH = Path(".cache_http.json")
UTC_NOW = datetime.now(timezone.utc)

CONNECT_TO = env_int("CONNECT_TIMEOUT", 12 if not IS_HARD_RECHECK else 16)
READ_TO    = env_int("READ_TIMEOUT",    35 if not IS_HARD_RECHECK else 45)
LIST_TO, DETAIL_TO = (CONNECT_TO, READ_TO), (CONNECT_TO, READ_TO)

MAX_WORKERS     = env_int("MAX_WORKERS", 10 if not IS_HARD_RECHECK else 14)
PER_SOURCE_MAX  = env_int("PER_SOURCE_MAX", 140 if not IS_HARD_RECHECK else 200)

RETRY_TOTAL         = env_int("RETRY_TOTAL", 5)
RETRY_CONNECT       = env_int("RETRY_CONNECT", 4)
RETRY_READ          = env_int("RETRY_READ", 4)
BACKOFF_FACTOR      = env_float("BACKOFF_FACTOR", 0.9)
MAX_BACKOFF_SECONDS = env_int("MAX_BACKOFF_SECONDS", 45)

PER_HOST_RPM   = env_int("PER_HOST_RPM", 24 if not IS_HARD_RECHECK else 36)
BASELINE_SLEEP = env_float("BASELINE_SLEEP_S", 0.7 if not IS_HARD_RECHECK else 0.6)
JITTER_MIN     = env_float("JITTER_MIN", 0.6)
JITTER_MAX     = env_float("JITTER_MAX", 1.8)

CB_FAILURE_THRESHOLD  = env_int("CB_FAILURE_THRESHOLD", 6)
CB_OPEN_SECONDS       = env_int("CB_OPEN_SECONDS", 600)
CB_HALF_OPEN_PROBE    = env_int("CB_HALF_OPEN_PROBE", 1)

UA_POOL = [
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
]
HEADERS_BASE = { "Accept-Language": "en-IN,en;q=0.9", "Cache-Control": "no-cache", "Pragma": "no-cache", "Connection": "keep-alive" }

# ------------ Sources ------------
PRIMARY_AGG = [
  {"name":"Adda247", "base":"https://www.adda247.com", "url":"https://www.adda247.com/jobs/government-jobs/"},
  {"name":"SarkariResultAlt", "base":"https://sarkariresult.com.cm", "url":"https://sarkariresult.com.cm/latestjob.php"},
]
BACKUP_AGG = [
  {"name":"SarkariExam",   "base":"https://www.sarkariexam.com",  "url":"https://www.sarkariexam.com"},
  {"name":"RojgarResult",  "base":"https://www.rojgarresult.com", "url":"https://www.rojgarresult.com/recruitments/"},
  {"name":"ResultBharat",  "base":"https://www.resultbharat.com", "url":"https://www.resultbharat.com"},
]
OFFICIAL_SOURCES = [
  {"name":"DSSSB_Notice",   "base":"https://dsssb.delhi.gov.in", "url":"https://dsssb.delhi.gov.in/notice-of-exam"},
  {"name":"RRB_Chandigarh", "base":"https://www.rrbcdg.gov.in",  "url":"https://www.rrbcdg.gov.in"},
  {"name":"BPSC",           "base":"https://bpsc.bihar.gov.in",  "url":"https://bpsc.bihar.gov.in"},
  {"name":"SSC",            "base":"https://ssc.gov.in",         "url":"https://ssc.gov.in"},
]
def env_channels():
  raw=os.getenv("TELEGRAM_CHANNELS","").strip()
  return [x for x in raw.split(",") if x] or []
TELEGRAM_CHANNELS = env_channels()
TELEGRAM_RSS_BASES = [
  os.getenv("TELEGRAM_RSS_BASE") or "https://rsshub.app",
  "https://rsshub.netlify.app",
  "https://rsshub.rssforever.com",
]
OFFICIAL_DOMAINS = {
  "dsssb.delhi.gov.in","dsssbonline.nic.in",
  "www.rrbcdg.gov.in","rrbcdg.gov.in",
  "bpsc.bihar.gov.in","www.bpsc.bihar.gov.in",
  "ssc.gov.in","www.ssc.gov.in","www.ssc.nic.in","ssc.nic.in",
}

# ------------ HTTP ------------
def session_with_retries(pool=96):
  s = requests.Session()
  s.headers.update({**HEADERS_BASE, "User-Agent": random.choice(UA_POOL)})
  s.verify = certifi.where()
  if Retry:
    retry = Retry(
      total=RETRY_TOTAL, connect=RETRY_CONNECT, read=RETRY_READ,
      backoff_factor=BACKOFF_FACTOR,
      status_forcelist=[403,429,500,502,503,504],
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
  if not hasattr(_thread_local, "s"):
    _thread_local.s = session_with_retries()
  return _thread_local.s

_host_tokens = {}; _host_lock = threading.Lock()
def _rate_limit_for(host):
  capacity = PER_HOST_RPM
  refill = PER_HOST_RPM / 60.0
  with _host_lock:
    b = _host_tokens.get(host, {"t": capacity, "ts": time.monotonic()})
    now = time.monotonic()
    elapsed = now - b["ts"]
    b["t"] = min(capacity, b["t"] + elapsed * refill)
    if b["t"] < 1.0:
      wait = (1.0 - b["t"]) / refill
      _host_tokens[host] = {"t": b["t"], "ts": now}
      return max(0.0, wait)
    b["t"] -= 1.0; b["ts"] = now; _host_tokens[host] = b
  return 0.0
def _sleep_with_jitter(): time.sleep(BASELINE_SLEEP + random.uniform(JITTER_MIN, JITTER_MAX))

_cb = {}
def cb_before(name):
  st = _cb.get(name, {"fail":0,"state":"closed","opened":0.0,"probe":0})
  if st["state"] == "open":
    if (time.time() - st["opened"]) >= CB_OPEN_SECONDS:
      st["state"]="half-open"; st["probe"]=0; _cb[name]=st
    else: return False
  if st["state"] == "half-open":
    if st["probe"] >= CB_HALF_OPEN_PROBE: return False
    st["probe"] += 1; _cb[name]=st
  return True
def cb_succ(name): _cb[name] = {"fail":0,"state":"closed","opened":0.0,"probe":0}
def cb_fail(name):
  st = _cb.get(name, {"fail":0,"state":"closed","opened":0.0,"probe":0})
  st["fail"] += 1
  if st["state"] == "half-open": st["state"]="open"; st["opened"]=time.time(); st["probe"]=0
  elif st["fail"] >= CB_FAILURE_THRESHOLD: st["state"]="open"; st["opened"]=time.time()
  _cb[name]=st

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
    _cache[url] = {}; 
    if et: _cache[url]["etag"]=et
    if lm: _cache[url]["last_modified"]=lm
def save_http_cache():
  try: CACHE_PATH.write_text(json.dumps(_cache, indent=2, ensure_ascii=False), encoding="utf-8")
  except Exception: pass

OFFICIAL_SSL_FALLBACK = {"bpsc.bihar.gov.in"}
def looks_official(url):
  try: return urlparse(url or "").netloc.lower() in OFFICIAL_DOMAINS
  except Exception: return False
def _host(url): return urlparse(url or "").netloc.lower()

def fetch(url, timeout, use_cache=False):
  host = _host(url)
  w = _rate_limit_for(host)
  if w>0: time.sleep(w)
  _sleep_with_jitter()
  extra={}
  if use_cache: extra.update(_cache_headers_for(url))
  attempt=0
  while True:
    attempt+=1
    try:
      r = HTTP.get(url, timeout=timeout, headers=extra)
      if r.status_code == 304:
        class _R: pass
        nr=_R(); nr.status_code=304; nr.headers=r.headers; nr.content=b""; nr.url=url
        return nr
      r.raise_for_status()
      if use_cache: _update_cache_from_response(url, r)
      return r
    except requests.exceptions.SSLError:
      if any(h in host for h in OFFICIAL_SSL_FALLBACK):
        r2 = requests.get(url, headers={**HTTP.headers, **extra}, timeout=sum(timeout), verify=False)
        r2.raise_for_status()
        if use_cache: _update_cache_from_response(url, r2)
        return r2
      raise
    except requests.exceptions.RequestException as e:
      code = getattr(e.response, "status_code", 0) if hasattr(e, "response") else 0
      if code == 403 and CF:
        try:
          r2 = CF.get(url, timeout=sum(timeout))
          if getattr(r2, "status_code", 0) == 200:
            if use_cache: _update_cache_from_response(url, r2)
            return r2
        except Exception:
          pass
      ra = 0
      if hasattr(e,"response") and e.response is not None:
        v = e.response.headers.get("Retry-After")
        if v:
          try: ra = int(v)
          except: ra = min(60, MAX_BACKOFF_SECONDS)
      backoff = min(MAX_BACKOFF_SECONDS, (2 ** min(attempt, 6)) * BACKOFF_FACTOR) + random.uniform(0.2,0.9)
      sleep_s = max(backoff, ra)
      if attempt <= RETRY_TOTAL:
        time.sleep(sleep_s); continue
      raise

# ------------ Parsing & filters ------------
def norm(s): return re.sub(r"\s+"," ",(s or "")).strip()
def absolute(base, href): return href if (href and href.startswith("http")) else (urljoin(base, href) if href else None)

def _decode_html_bytes(resp):
  enc = resp.encoding or getattr(resp, "apparent_encoding", None) or "utf-8"
  dammit = UnicodeDammit(resp.content, is_html=True, known_definite_encodings=[enc,"utf-8","windows-1252","iso-8859-1"])
  return dammit.unicode_markup
def soup_from_resp(resp):
  ct = (resp.headers.get("Content-Type") or "").lower()
  if ("html" not in ct) and ("text/" not in ct): return None
  html = _decode_html_bytes(resp)
  try: return BeautifulSoup(html, "lxml")
  except Exception: return BeautifulSoup(html, "html.parser")

RECRUITMENT_TERMS = [r"\brecruitment\b", r"\bvacanc(?:y|ies)\b", r"\badvertisement\b", r"\bnotification\b", r"\bonline\s*form\b", r"\bapply\s*online\b"]
EXCLUDE_NOISE = [r"\badmit\s*card\b", r"\banswer\s*key\b", r"\bresult\b", r"\bsyllabus\b", r"\bcalendar\b", r"\bwebinar\b", r"\bwellness\b"]
UPDATE_TERMS = [r"\bcorrigendum\b", r"\baddendum\b", r"\bamendment\b", r"\brevised\b", r"\bdate\s*(?:extended|extension)\b", r"\bpostponed\b", r"\brescheduled\b", r"\bedit\s*window\b"]

def contains_any(patterns, text): return any(re.search(p, (text or "").lower()) for p in patterns)
def is_update(text): return contains_any(UPDATE_TERMS, text)
def is_joblike(text): return contains_any(RECRUITMENT_TERMS, text) and not contains_any(EXCLUDE_NOISE, text)
def is_non_vacancy(text): return bool(re.search(r"\b(otr|one\s*time\s*registration|registration\s*process)\b", (text or "").lower()))

INDIAN_STATES = ["andhra pradesh","arunachal pradesh","assam","bihar","chhattisgarh","goa","gujarat","haryana","himachal pradesh","jharkhand","karnataka","kerala","madhya pradesh","maharashtra","manipur","meghalaya","mizoram","nagaland","odisha","punjab","rajasthan","sikkim","tamil nadu","telangana","tripura","uttar pradesh","uttarakhand","west bengal"]
def other_state_only(text):
  t=(text or "").lower()
  if not re.search(r"\b(domici(?:le|liary)|resident)\b", t): return False
  for st in INDIAN_STATES:
    if st=="bihar": continue
    if re.search(rf"\b{re.escape(st)}\b.*\bonly\b", t) or re.search(rf"\bonly\b.*\b{re.escape(st)}\b", t): return True
  return False

ALLOWED_EDU = [r"\b10\s*th\b", r"\bmatric\b", r"\b12\s*th\b", r"\binter(?:mediate)?\b", r"\bany\s+graduate\b", r"\bgraduate\b", r"\bbachelor(?:'s)?\s+degree\b"]
EXCLUDE_EDU = [
  r"\bmaster'?s\b|\bm\.?\s?sc\b|\bm\.?\s?a\b|\bm\.?\s?com\b", r"\bmba\b|\bpgdm\b",
  r"\b(b\.?tech|be)\b|\bm\.?tech\b|\bengineering\b", r"\bmca\b|\bbca\b",
  r"\blaw\b|\bllb\b|\bllm\b", r"\bca\b|\bcfa\b|\bcs\b|\bicwa\b|\bcma\b",
  r"\bmbbs\b|\bbds\b|\bnursing\b|\bgnm\b|\banm\b|\bpharm\b", r"\bphd\b|\bdoctorate\b"
]
ALLOW_SKILLS = [r"\btyping\b", r"\bsteno\b", r"\bcomputer\s+(?:certificate|knowledge|literacy|proficiency)\b", r"\bccc\b|\bnielit\b|\bdoeacc\b|\b'o?\s*level\b", r"\bpet\b|\bpst\b|\bphysical\b"]
EXCLUDE_TECH = [r"\bpython\b|\bjava\b|\bjavascript\b|\bc\+\+\b|\bnode\.?js\b|\breact\b|\bangular\b|\b\.net\b", r"\bautocad\b|\bmatlab\b|\bsolidworks\b|\bcatia\b|\bsap\b"]

def education_allowed(text):
  t=(text or "").lower()
  if any(re.search(p, t) for p in EXCLUDE_EDU): return False
  if any(re.search(p, t) for p in EXCLUDE_TECH) and not any(re.search(p, t) for p in ALLOW_SKILLS): return False
  return any(re.search(p, t) for p in ALLOWED_EDU)

def norm_key(title):
  t = norm(title).lower()
  t = re.sub(r"(recruitment|notification|advertisement|apply\s*online|online\s*form|\b20\d{2})"," ",t)
  t = re.sub(r"[^a-z0-9\s]", " ", t)
  tokens = [w for w in t.split() if len(w) > 2]
  return " ".join(tokens[:14])

DATE_WORDS = {"jan":"january","feb":"february","mar":"march","apr":"april","may":"may","jun":"june","jul":"july","aug":"august","sep":"september","sept":"september","oct":"october","nov":"november","dec":"december"}
def _month_word_fix(s):
  t=s.lower()
  for k,v in DATE_WORDS.items(): t=re.sub(rf"\b{k}\b",v,t)
  return re.sub(r"[–—−]", "-", t)
PAT_TILL = re.compile(r"(apply\s*(?:online)?\s*till|closes\s*on|last\s*date)\s*[:\-]?\s*(\d{1,2}\s+[a-z]+(?:\s+\d{4})?|\d{1,2}[-/]\d{1,2}[-/]\d{2,4})", re.I)
PAT_LASTDATE_COLON = re.compile(r"(?:last\s*date|closing\s*date|last\s*day)\s*[:\-]?\s*(\d{1,2}[./-]\d{1,2}[./-](?:\d{2,4}))", re.I)
PAT_NUMERIC_DMY     = re.compile(r"\b(\d{1,2})[./-](\d{1,2})[./-](\d{2,4})\b")
PAT_YYYYMMDD        = re.compile(r"\b(20\d{2})[./-]?(\d{2})[./-]?(\d{2})\b")

def _parse_dmy_token(s):
  try:
    dt = dateparser.parse(s, settings={"DATE_ORDER":"DMY"})
    return dt.date() if dt else None
  except: return None

def parse_application_window(text):
  t=_month_word_fix(norm(text or ""))
  pat_range=re.compile(r"(application|online\s*registration|apply\s*online|registration)[^\.:\n]{0,30}(\d{1,2}\s+[a-z]+(?:\s+\d{4})?)\s*[-to]+\s*(\d{1,2}\s+[a-z]+(?:\s+\d{4})?)", re.I)
  m=pat_range.search(t)
  if m:
    s_str,e_str=m.group(2),m.group(3)
    end_year_match = re.search(r"\d{4}", e_str)
    if end_year_match and not re.search(r"\d{4}", s_str): s_str = s_str + " " + end_year_match.group(0)
    s_dt=dateparser.parse(s_str, settings={"DATE_ORDER":"DMY","PREFER_DAY_OF_MONTH":"first"})
    e_dt=dateparser.parse(e_str, settings={"DATE_ORDER":"DMY","PREFER_DATES_FROM":"future"})
    return (s_dt.date().isoformat() if s_dt else None, e_dt.date().isoformat() if e_dt else None)
  m2 = PAT_TILL.search(t)
  if m2:
    e_dt=dateparser.parse(m2.group(2), settings={"DATE_ORDER":"DMY","PREFER_DATES_FROM":"future"})
    return (None, e_dt.date().isoformat() if e_dt else None)
  m3 = PAT_LASTDATE_COLON.search(t)
  if m3:
    d = _parse_dmy_token(m3.group(1))
    if d: return (None, d.isoformat())
  dates_found=[]
  for a,b,c in PAT_NUMERIC_DMY.findall(t):
    d = _parse_dmy_token(f"{a}-{b}-{c}")
    if d: dates_found.append(d)
  for y,mm,dd in PAT_YYYYMMDD.findall(t):
    d = _parse_dmy_token(f"{dd}-{mm}-{y}")
    if d: dates_found.append(d)
  if dates_found:
    dates_found.sort()
    latest = dates_found[-1]
    if latest >= datetime.now().date():
      return (None, latest.isoformat())
  return (None,None)

def probe_deadline_from_link(primary_url):
  try:
    if not primary_url: return None
    r = fetch(primary_url, DETAIL_TO)
    ct = (r.headers.get("Content-Type","").lower())
    if ct.startswith("application/pdf") or "pdf" in (primary_url.lower()):
      sample = r.content[:2000].decode("latin-1", errors="ignore")
      _, e = parse_application_window(sample)
      return e
    if "html" in ct:
      s = soup_from_resp(r)
      if s:
        txt = norm(s.get_text(" ")[:1500])
        _, e = parse_application_window(txt)
        return e
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

def infer_qualification(text):
  t=(text or "").lower()
  if any(re.search(p, t) for p in [r"\b10\s*th\b", r"\bmatric\b"]): return "10th Pass"
  if any(re.search(p, t) for p in [r"\b12\s*th\b", r"\binter(?:mediate)?\b"]): return "12th Pass"
  if any(re.search(p, t) for p in [r"\bany\s+graduate\b", r"\bgraduate\b", r"\bbachelor(?:'s)?\s+degree\b"]): return "Graduate"
  return "Graduate"

def is_valid_url(u):
  try:
    if not u: return False
    pr = urlparse(u)
    return pr.scheme in ("http","https") and pr.netloc
  except: return False

def validate_inline(rec):
  if not rec.get("title") or len(rec.get("title","")) < 6: return False
  if not rec.get("detailText") or len(rec.get("detailText","")) < 40: return False
  al = rec.get("applyLink"); pl = rec.get("pdfLink")
  if not (is_valid_url(al) or is_valid_url(pl)): return False
  return True

def soup_text(url):
  try:
    r = thread_session().get(url, timeout=DETAIL_TO, verify=certifi.where(), headers={"User-Agent": random.choice(UA_POOL), **HEADERS_BASE})
    if "html" in (r.headers.get("Content-Type","")).lower():
      s = soup_from_resp(r); return norm(s.get_text(" ")) if s else ""
  except Exception:
    pass
  return ""

def scrape_generic_list(src, treat_as="aggregator", metrics=None):
  name = src["name"]; url = src["url"]; base = src["base"]
  if not cb_before(name):
    if metrics is not None:
      metrics[name]["skipped_due_cb"] = metrics[name]["skipped_due_cb"] + 1
    return []
  started=time.time()
  items=[]; raw=0; hinted=0; kept=0
  try:
    r = fetch(url, LIST_TO, use_cache=True)
    if r.status_code == 304:
      if metrics is not None:
        m = metrics[name]
        m["not_modified"] = m["not_modified"] + 1
        m["durations"].append(time.time()-started)
      cb_succ(name); return []
    soup = soup_from_resp(r)
    if soup is None:
      cb_fail(name)
      if metrics is not None:
        metrics[name]["fail"] = metrics[name]["fail"] + 1
      return []
    anchors=[]
    for a in soup.find_all("a", href=True):
      href = absolute(base, a["href"]); title = norm(a.get_text(" "))
      if not href or len(title) < 6: continue
      raw += 1
      if treat_as=="official" or is_joblike(f"{title} {href}"):
        anchors.append((title, href)); hinted += 1
      if len(anchors) >= PER_SOURCE_MAX: break
    for title, href in anchors:
      detail_text = title + " — " + soup_text(href)
      if is_non_vacancy(detail_text) or other_state_only(detail_text) or not education_allowed(detail_text): continue
      key_base = norm_key(title); upd = is_update(detail_text); unique_key = f"{key_base}|upd" if upd else key_base
      rec = {
        "key": key_base, "uniqueKey": unique_key, "title": title,
        "organization": src["name"], "applyLink": href, "pdfLink": href,
        "deadline": extract_deadline(detail_text, fallback_url=href),
        "domicile": "Bihar" if "bihar" in detail_text.lower() else "All India",
        "sourceType": treat_as, "source": src["name"],
        "isUpdate": upd, "updateSummary": title if upd else None,
        "detailText": detail_text
      }
      if validate_inline(rec):
        items.append(rec); kept += 1
    cb_succ(name)
    if metrics is not None:
      m = metrics[name]
      m["ok"] = m["ok"] + 1
      m["durations"].append(time.time()-started)
      m["raw"] += raw; m["hinted"] += hinted; m["kept"] += kept
    print(f"[{treat_as.upper()}] {name}: raw={raw} hinted={hinted} kept={kept}")
  except Exception as e:
    cb_fail(name)
    if metrics is not None:
      metrics[name]["fail"] = metrics[name]["fail"] + 1
      metrics[name]["error_samples"].append(str(e))
    print(f"[WARN] {treat_as} {name} error: {e}")
  return items

def scrape_aggregator_page(src, metrics): return scrape_generic_list(src, "aggregator", metrics)
def scrape_official_page(src, metrics):   return scrape_generic_list(src, "official", metrics)

def telegram_feed_urls(username):
  for base in TELEGRAM_RSS_BASES:
    if base: yield f"{base.rstrip('/')}/telegram/channel/{username}"

def scrape_telegram_channel(username, metrics):
  key = f"TG:{username}"
  if not cb_before(key):
    metrics[key]["skipped_due_cb"] = metrics[key]["skipped_due_cb"] + 1
    return []
  started=time.time()
  items=[]; kept=0
  try:
    for url in telegram_feed_urls(username):
      try:
        root = ET.fromstring(fetch(url, LIST_TO, use_cache=True).content)
        def tx(node, tag):
          el = node.find(tag); return norm(el.text) if (el is not None and el.text) else ""
        for it in root.findall(".//item"):
          title = tx(it,"title"); link = tx(it,"link"); desc = tx(it,"description")
          combo = norm(f"{title} {desc}")
          if len(title) < 6 or not is_joblike(combo): continue
          if is_non_vacancy(combo) or other_state_only(combo) or not education_allowed(combo): continue
          key_base = norm_key(title); upd = is_update(combo); unique_key = f"{key_base}|upd" if upd else key_base
          rec = {
            "key": key_base, "uniqueKey": unique_key, "title": title,
            "organization": f"Telegram:{username}", "applyLink": link or None, "pdfLink": link or None,
            "deadline": extract_deadline(combo, fallback_url=link or None),
            "domicile": "Bihar" if "bihar" in combo.lower() else "All India",
            "sourceType": "telegram", "source": f"Telegram:{username}",
            "isUpdate": upd, "updateSummary": title if upd else None,
            "detailText": combo
          }
          if validate_inline(rec): items.append(rec); kept += 1
        print(f"[TG ] {username}: kept={kept}")
        cb_succ(key); metrics[key]["ok"] = metrics[key]["ok"] + 1
        metrics[key]["durations"].append(time.time()-started); metrics[key]["kept"]+=kept
        return items
      except Exception as e:
        print(f"[WARN] telegram {username}@{url}: {e}; trying next")
        metrics[key]["error_samples"].append(str(e))
        continue
  except Exception:
    cb_fail(key); metrics[key]["fail"] = metrics[key]["fail"] + 1; metrics[key]["durations"].append(time.time()-started)
  print(f"[TG ] {username}: all RSS endpoints failed; continuing"); return items

# ------------ Aggregators-first merge & verify ------------
def merge_and_verify(primary_items, backup_items, official_items):
  buckets={}
  def add(it):
    b = buckets.setdefault(it["key"], {"aggs": set(), "items": [], "hasOfficial": False, "offNames": set()})
    if it["sourceType"] == "aggregator": b["aggs"].add(it["source"])
    if it["sourceType"] == "official":
      b["hasOfficial"] = True; b["offNames"].add(it["source"])
    b["items"].append(it)

  for lst in (primary_items, backup_items, official_items):
    for it in lst: add(it)

  published=[]; pending=set(); seen_ids=set()
  for key, b in buckets.items():
    rep = None
    if b["hasOfficial"]:
      rep = next((x for x in b["items"] if x.get("sourceType")=="official" and not x.get("isUpdate")), None) or \
            next((x for x in b["items"] if x.get("sourceType")=="official"), None)
      verifiedBy = "official"
    else:
      if len(b["aggs"]) >= 2:
        rep = next((x for x in b["items"] if x.get("sourceType")=="aggregator" and not x.get("isUpdate")), None) or \
              next((x for x in b["items"] if x.get("sourceType")=="aggregator"), None)
        verifiedBy = "multi-aggregator"
      else:
        pending.add(key); continue
    if not rep: continue

    dl = rep.get("deadline")
    ok_deadline = False
    if dl:
      try:
        ok_deadline = dateparser.parse(dl, settings={"DATE_ORDER":"DMY"}).date() >= datetime.now().date()
      except Exception:
        ok_deadline = False
    if not ok_deadline:
      continue

    sources = sorted({x["source"] for x in b["items"] if x.get("sourceType") in ("official","aggregator")})
    schema_source = "official" if verifiedBy == "official" else "aggregator"
    base_id = hashlib.sha1(f"{'|'.join(sources)}|{rep['uniqueKey']}".encode("utf-8")).hexdigest()[:12]
    rid = base_id; n=1
    while rid in seen_ids:
      rid = hashlib.sha1(f"{base_id}|{n}".encode("utf-8")).hexdigest()[:12]; n+=1
    seen_ids.add(rid)
    rep_rec = {
      "id": rid, "slug": rid, "title": rep["title"], "organization": "/".join(sources) if sources else rep["organization"],
      "qualificationLevel": infer_qualification(rep["detailText"]),
      "domicile": rep["domicile"], "source": schema_source, "verifiedBy": verifiedBy,
      "type": "VACANCY", "updateSummary": None, "relatedTo": None, "deadline": rep.get("deadline"),
      "applyLink": rep.get("applyLink"), "pdfLink": rep.get("pdfLink"), "extractedAt": UTC_NOW.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    published.append(rep_rec)

    for u in (x for x in b["items"] if x.get("isUpdate")):
      up_base = hashlib.sha1(f"{'|'.join(sources)}|{u['uniqueKey']}".encode("utf-8")).hexdigest()[:12]
      uid = up_base; m=1
      while uid in seen_ids:
        uid = hashlib.sha1(f"{up_base}|{m}".encode("utf-8")).hexdigest()[:12]; m+=1
      seen_ids.add(uid)
      upd = {
        "id": uid, "slug": uid, "title": "[UPDATE] " + u["title"], "organization": rep_rec["organization"],
        "qualificationLevel": infer_qualification(u["detailText"]),
        "domicile": rep["domicile"], "source": schema_source, "verifiedBy": verifiedBy,
        "type": "UPDATE", "updateSummary": u.get("updateSummary"), "relatedTo": rep_rec["slug"],
        "deadline": u.get("deadline") or rep_rec["deadline"],
        "applyLink": u.get("applyLink") or rep_rec["applyLink"], "pdfLink": u.get("pdfLink") or rep_rec["pdfLink"],
        "extractedAt": UTC_NOW.strftime("%Y-%m-%dT%H:%M:%SZ"),
      }
      published.append(upd)
  return published, pending

# ------------ Repo IO ------------
def load_data():
  base={"jobListings": [], "transparencyInfo": {}}
  if DATA_PATH.exists():
    try: base=json.loads(DATA_PATH.read_text(encoding="utf-8"))
    except Exception: pass
  base.setdefault("jobListings",[]); base.setdefault("transparencyInfo",{})
  return base
def save_data(data): DATA_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

# ------------ Main ------------
def main():
  run_started = time.time()
  per_source_metrics = {}
  for s in PRIMARY_AGG+BACKUP_AGG: per_source_metrics[s["name"]]={"ok":0,"fail":0,"skipped_due_cb":0,"not_modified":0,"raw":0,"hinted":0,"kept":0,"error_samples":[],"durations":[]}
  for s in OFFICIAL_SOURCES: per_source_metrics[s["name"]]={"ok":0,"fail":0,"skipped_due_cb":0,"not_modified":0,"raw":0,"hinted":0,"kept":0,"error_samples":[],"durations":[]}
  for ch in TELEGRAM_CHANNELS: per_source_metrics[f"TG:{ch}"]={"ok":0,"fail":0,"skipped_due_cb":0,"not_modified":0,"raw":0,"hinted":0,"kept":0,"error_samples":[],"durations":[]}

  primary_items=[]; backup_items=[]; official_items=[]; tg_hints=[]
  agg_counts={}; off_counts={}; tg_counts={}

  with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
    futs = {}
    for src in PRIMARY_AGG: futs[ex.submit(scrape_aggregator_page, src, per_source_metrics)] = ("p", src["name"])
    for src in BACKUP_AGG:  futs[ex.submit(scrape_aggregator_page, src, per_source_metrics)] = ("b", src["name"])
    for src in OFFICIAL_SOURCES: futs[ex.submit(scrape_official_page, src, per_source_metrics)] = ("o", src["name"])
    for ch in TELEGRAM_CHANNELS: futs[ex.submit(scrape_telegram_channel, ch, per_source_metrics)] = ("t", ch)
    for f in as_completed(futs):
      tag, name = futs[f]
      try:
        its = f.result()
        if tag=="p": primary_items.extend(its); agg_counts[name]=len(its)
        elif tag=="b": backup_items.extend(its); agg_counts[name]=len(its)
        elif tag=="o": official_items.extend(its); off_counts[name]=len(its)
        else: tg_hints.extend(its); tg_counts[name]=len(its)
      except Exception as e:
        print(f"[ERROR] {tag}:{name} failed: {e}")
        if tag in ("p","b"): agg_counts[name]=0
        elif tag=="o": off_counts[name]=0
        else: tg_counts[name]=0

  published, pending = merge_and_verify(primary_items, backup_items, official_items)

  prev = load_data()
  data = prev
  data["jobListings"] = published

  duration = time.time() - run_started
  ti = data.setdefault("transparencyInfo",{})
  ti["lastUpdated"] = UTC_NOW.strftime("%Y-%m-%dT%H:%M:%SZ")
  ti["totalListings"] = len(published)
  ti["aggCounts"] = agg_counts
  ti["officialCounts"] = off_counts
  ti["telegramCounts"] = tg_counts
  ti["pendingFromAggregators"] = sorted(list(pending))
  ti["notes"] = "Aggregators-first discovery; official-first verification; strict open window; resilient networking."
  ti["runDurationSec"] = round(duration,2)
  ti["perSourceMetrics"] = per_source_metrics

  save_data(data); save_http_cache()
  print(f"[INFO] published={len(published)} pending={len(pending)} duration={round(duration,2)}s")

if __name__ == "__main__":
  main()
