# scraper.py — Aggregator-first publishing with:
# - unique IDs for updates (no duplicate ids),
# - schema-compliant 'source' enum,
# - verifiedBy confidence field,
# - open-window-only publishing (parse application ranges / last-date),
# - exclude non-vacancies like SSC OTR,
# - Bihar-only or All-India eligibility kept; other state-only excluded,
# - Telegram(RSS) multi-endpoint fallbacks,
# - resilient requests Session + HTTPAdapter + Retry with short timeouts.

import json, re, hashlib, threading, os, xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse

import requests
from requests.adapters import HTTPAdapter
try:
    from urllib3.util import Retry
except Exception:
    Retry = None

from bs4 import BeautifulSoup, UnicodeDammit
import dateparser
from concurrent.futures import ThreadPoolExecutor, as_completed

DATA_PATH = Path("data.json")
UTC_NOW = datetime.now(timezone.utc)

# Networking
CONNECT_TO, READ_TO = 5, 9
LIST_TO, DETAIL_TO = (CONNECT_TO, READ_TO), (CONNECT_TO, READ_TO)
HEAD_TO = 6
MAX_WORKERS = 10
PER_SOURCE_MAX = 140

HEADERS = {
  "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
  "Accept-Language": "en-IN,en;q=0.9",
  "Cache-Control": "no-cache"
}

def session_with_retries(pool=64):
  s = requests.Session(); s.headers.update(HEADERS)
  if Retry:
    retry = Retry(total=2, connect=2, read=2, backoff_factor=0.4,
                  status_forcelist=[429,500,502,503,504], allowed_methods={"GET","HEAD"})
    adapter = HTTPAdapter(max_retries=retry, pool_connections=pool, pool_maxsize=pool)
  else:
    adapter = HTTPAdapter(pool_connections=pool, pool_maxsize=pool)
  s.mount("http://", adapter); s.mount("https://", adapter)
  return s  # HTTPAdapter + Retry is the recommended resilient pattern. [13][10][19]

HTTP = session_with_retries()
_thread_local = threading.local()
def thread_session():
  if not hasattr(_thread_local, "s"):
    _thread_local.s = session_with_retries()
  return _thread_local.s

def norm(s): return re.sub(r"\s+"," ",(s or "")).strip()
def absolute(base, href): return href if (href and href.startswith("http")) else (urljoin(base, href) if href else None)

def fetch(url, timeout):
  r = HTTP.get(url, timeout=timeout); r.raise_for_status(); return r

def decode_html_bytes(resp):
  enc = resp.encoding or getattr(resp, "apparent_encoding", None) or "utf-8"
  dammit = UnicodeDammit(resp.content, is_html=True, known_definite_encodings=[enc,"utf-8","windows-1252","iso-8859-1"])
  return dammit.unicode_markup

def soup_from_resp(resp):
  ct = (resp.headers.get("Content-Type") or "").lower()
  if ("html" not in ct) and ("text/" not in ct): return None
  html = decode_html_bytes(resp)
  try: return BeautifulSoup(html, "lxml")
  except Exception: return BeautifulSoup(html, "html.parser")

# Trusted aggregators (discovery)
AGG_SOURCES = [
  {"name":"Adda247",       "base":"https://www.adda247.com",      "url":"https://www.adda247.com/jobs/government-jobs/"},
  {"name":"SarkariExam",   "base":"https://www.sarkariexam.com",  "url":"https://www.sarkariexam.com"},
  {"name":"RojgarResult",  "base":"https://www.rojgarresult.com", "url":"https://www.rojgarresult.com/recruitments/"},
  {"name":"SarkariExamCM", "base":"https://sarkariexam.com.cm",   "url":"https://sarkariexam.com.cm"},
]  # Aggregator coverage for live recruitments and notices. [21]

# Telegram hints via RSSHub (no login)
TELEGRAM_CHANNELS = ["ezgovtjob", "sarkariresulinfo"]  # hints only
TELEGRAM_RSS_BASES = [
  os.getenv("TELEGRAM_RSS_BASE") or "https://rsshub.app",
  "https://rsshub.netlify.app",
  "https://rsshub.rssforever.com",
]  # /telegram/channel/:username per docs; multiple instances for fallback. [6][12][18]

# Official verification domains (includes SSC portal)
OFFICIAL_DOMAINS = {
  "ssc.gov.in","www.ssc.gov.in",
  "bssc.bihar.gov.in","onlinebssc.com",
  "dsssb.delhi.gov.in","dsssbonline.nic.in",
  "www.ibps.in",
  "bpsc.bihar.gov.in","bpsconline.bihar.gov.in",
  "www.rbi.org.in","opportunities.rbi.org.in",
  "ccras.nic.in",
  "www.rrbcdg.gov.in","www.rrbpatna.gov.in",
}

# Filters
RECRUITMENT_TERMS = [r"\brecruitment\b", r"\bvacanc(?:y|ies)\b", r"\badvertisement\b", r"\bnotification\b", r"\bonline\s*form\b", r"\bapply\s*online\b"]
EXCLUDE_NOISE = [r"\badmit\s*card\b", r"\banswer\s*key\b", r"\bresult\b", r"\bsyllabus\b", r"\bcalendar\b", r"\bwebinar\b", r"\bwellness\b"]

UPDATE_TERMS = [r"\bcorrigendum\b", r"\baddendum\b", r"\bamendment\b", r"\brevised\b", r"\bdate\s*(?:extended|extension)\b", r"\bpostponed\b", r"\brescheduled\b", r"\bedit\s*window\b"]
def contains_any(patterns, text): return any(re.search(p, (text or "").lower()) for p in patterns)
def is_update(text): return contains_any(UPDATE_TERMS, text)
def is_joblike(text): return contains_any(RECRUITMENT_TERMS, text) and not contains_any(EXCLUDE_NOISE, text)  # broad but precise. [21]

# Exclude non-vacancies like SSC OTR
def is_non_vacancy(text):
  t = (text or "").lower()
  if re.search(r"\b(otr|one\s*time\s*registration|registration\s*process)\b", t): 
    return True  # OTR is a profile step, not a recruitment. [22][23]
  return False

# Domicile guard: show All-India or Bihar-only; exclude other state-only
INDIAN_STATES = ["andhra pradesh","arunachal pradesh","assam","bihar","chhattisgarh","goa","gujarat","haryana","himachal pradesh",
 "jharkhand","karnataka","kerala","madhya pradesh","maharashtra","manipur","meghalaya","mizoram","nagaland","odisha",
 "punjab","rajasthan","sikkim","tamil nadu","telangana","tripura","uttar pradesh","uttarakhand","west bengal"]
RESTRICT_PAT = r"\b(domici(?:le|liary)|resident)\b.*?\b(only|required)\b"
def other_state_only(text):
  t=(text or "").lower()
  for st in INDIAN_STATES:
    if st=="bihar": continue
    if re.search(rf"\b{re.escape(st)}\b.*{RESTRICT_PAT}", t) or re.search(rf"{RESTRICT_PAT}.*\b{re.escape(st)}\b", t):
      return True
  return False

# Key normalization
def norm_key(title):
  t = norm(title).lower()
  t = re.sub(r"(recruitment|notification|advertisement|apply\s*online|online\s*form|\b20\d{2})"," ",t)
  t = re.sub(r"[^a-z0-9\s]", " ", t)
  tokens = [w for w in t.split() if len(w) > 2]
  return " ".join(tokens[:14])

# Application window parsing (tables/lines like Adda247)
DATE_WORDS = {"jan":"january","feb":"february","mar":"march","apr":"april","may":"may","jun":"june",
              "jul":"july","aug":"august","sep":"september","sept":"september","oct":"october","nov":"november","dec":"december"}
def _month_word_fix(s):
  t=s.lower()
  for k,v in DATE_WORDS.items(): t=re.sub(rf"\b{k}\b",v,t)
  return re.sub(r"[–—−]", "-", t)

def parse_application_window(text):
  t=_month_word_fix(norm(text or ""))
  pat_range=re.compile(r"(application|online\s*registration|apply\s*online|registration)[^\.:\n]{0,30}"
                       r"(\d{1,2}\s+[a-z]+(?:\s+\d{4})?)\s*[-to]+\s*(\d{1,2}\s+[a-z]+(?:\s+\d{4})?)", re.I)
  m=pat_range.search(t)
  if m:
    s_str,e_str=m.group(2),m.group(3)
    if re.search(r"\d{4}",e_str) and not re.search(r"\d{4}",s_str):
      s_str=f"{s_str} {re.search(r'\\d{4}',e_str).group(0)}"
    s_dt=dateparser.parse(s_str, settings={"DATE_ORDER":"DMY"})
    e_dt=dateparser.parse(e_str, settings={"DATE_ORDER":"DMY"})
    return (s_dt.date().isoformat() if s_dt else None, e_dt.date().isoformat() if e_dt else None)
  m2=re.compile(r"(last\s*date|closing\s*date)[^\.:\n]{0,30}(\d{1,2}\s+[a-z]+(?:\s+\d{4})?)", re.I).search(t)
  if m2:
    e_dt=dateparser.parse(m2.group(2), settings={"DATE_ORDER":"DMY"})
    return (None, e_dt.date().isoformat() if e_dt else None)
  return (None,None)

def extract_deadline(text):
  _,e = parse_application_window(text or "")
  if e: return e
  dt = dateparser.parse(text or "", settings={"PREFER_DATES_FROM":"future","DATE_ORDER":"DMY"})
  return dt.date().isoformat() if (dt and dt.date() >= datetime.now().date()) else None  # open-window enforcement is applied later. [24]

# Official-domain check
def looks_official(url):
  try: return urlparse(url or "").netloc.lower() in OFFICIAL_DOMAINS
  except Exception: return False

def scrape_aggregator_page(src):
  items=[]; raw=0; hinted=0; kept=0
  try:
    try:
      soup = soup_from_resp(fetch(src["url"], LIST_TO))
    except requests.HTTPError as e:
      code = getattr(e.response, "status_code", 0)
      if code in (403, 429):
        print(f"[AGG] {src['name']}: {code} blocked, skipping quickly"); return items
      raise
    if soup is None:
      print(f"[AGG] {src['name']}: non-HTML"); return items

    anchors=[]
    for a in soup.find_all("a", href=True):
      href = absolute(src["base"], a["href"]); title = norm(a.get_text(" "))
      if not href or len(title) < 6: continue
      raw += 1
      if is_joblike(f"{title} {href}"): anchors.append((title, href)); hinted += 1
      if len(anchors) >= PER_SOURCE_MAX: break

    def enrich(pair):
      title, href = pair
      detail_text = title
      try:
        ds = soup_from_resp(thread_session().get(href, timeout=DETAIL_TO))
        if ds is not None: detail_text = norm(ds.get_text(" "))
      except Exception: pass
      combo = f"{title} — {detail_text}"
      if is_non_vacancy(combo): return None  # drop OTR/registration pages. [22][23]
      if other_state_only(combo): return None  # exclude other state-only.
      key_base = norm_key(title)
      upd = is_update(combo)
      unique_key = f"{key_base}|upd" if upd else key_base  # distinct keys for updates (no id collisions).
      return {
        "key": key_base,
        "uniqueKey": unique_key,
        "title": title,
        "organization": src["name"],
        "applyLink": href,
        "pdfLink": href,
        "deadline": extract_deadline(combo),
        "domicile": "Bihar" if "bihar" in combo.lower() else "All India",
        "sourceType": "aggregator",
        "source": src["name"],
        "isUpdate": upd,
        "updateSummary": title if upd else None,
        "detailText": combo
      }

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
      for fut in as_completed([ex.submit(enrich, p) for p in anchors]):
        it = fut.result()
        if it: items.append(it); kept += 1
    print(f"[AGG] {src['name']}: raw={raw} hinted={hinted} kept={kept}")
  except Exception as e:
    print(f"[WARN] aggregator {src['name']} error: {e}")
  return items

def telegram_feed_urls(username):
  for base in TELEGRAM_RSS_BASES:
    if base: yield f"{base.rstrip('/')}/telegram/channel/{username}"  # documented route. [6][9]

def scrape_telegram_channel(username):
  items=[]; kept=0
  for url in telegram_feed_urls(username):
    try:
      root = ET.fromstring(fetch(url, LIST_TO).content)
      def tx(node, tag):
        el = node.find(tag); return norm(el.text) if (el is not None and el.text) else ""
      for it in root.findall(".//item"):
        title = tx(it,"title"); link = tx(it,"link"); desc = tx(it,"description")
        combo = norm(f"{title} {desc}")
        if len(title) < 6 or not is_joblike(combo): continue
        if is_non_vacancy(combo): continue
        if other_state_only(combo): continue
        key_base = norm_key(title)
        upd = is_update(combo)
        unique_key = f"{key_base}|upd" if upd else key_base
        items.append({
          "key": key_base,
          "uniqueKey": unique_key,
          "title": title,
          "organization": f"Telegram:{username}",
          "applyLink": link or None,
          "pdfLink": link or None,
          "deadline": extract_deadline(combo),
          "domicile": "Bihar" if "bihar" in combo.lower() else "All India",
          "sourceType": "telegram",
          "source": f"Telegram:{username}",
          "isUpdate": upd,
          "updateSummary": title if upd else None,
          "detailText": combo
        }); kept += 1
      print(f"[TG ] {username}@{url}: kept={kept}"); return items
    except requests.HTTPError as e:
      print(f"[WARN] telegram {username}@{url} HTTP {getattr(e.response,'status_code',0)}; trying next")
    except requests.RequestException as e:
      print(f"[WARN] telegram {username}@{url} network: {e}; trying next")
    except Exception as e:
      print(f"[WARN] telegram {username}@{url} parse: {e}; trying next")
  print(f"[TG ] {username}: all RSS endpoints failed; continuing"); return items  # Fall back behavior. [15]

def merge_and_mark(collected, prev_pending):
  # group by base key for cross-source verification
  buckets={}
  for it in collected:
    b = buckets.setdefault(it["key"], {"aggs": set(), "hasOfficial": False, "tele": False, "items": []})
    if it["sourceType"] == "aggregator": b["aggs"].add(it["source"])
    if it["sourceType"] == "telegram": b["tele"] = True
    if looks_official(it.get("applyLink")) or looks_official(it.get("pdfLink")): b["hasOfficial"] = True
    b["items"].append(it)

  published=[]; pending=set(); seen_ids=set()
  for key, b in buckets.items():
    # confidence
    if b["hasOfficial"]:
      verifiedBy = "official"
    elif len(b["aggs"]) >= 2:
      verifiedBy = "multi-aggregator"
    elif len(b["aggs"]) == 1:
      verifiedBy = "single-aggregator"
    else:
      verifiedBy = None

    if verifiedBy is None:
      if b["tele"]: pending.add(key)
      continue

    # representative aggregator record
    rep = next((x for x in b["items"] if x.get("sourceType")=="aggregator" and not x.get("isUpdate")), None)
    if rep is None: rep = next((x for x in b["items"] if x.get("sourceType")=="aggregator"), None)
    if rep is None: rep = next((x for x in b["items"] if isinstance(x, dict)), None)
    if not isinstance(rep, dict): 
      continue

    sources = sorted({x["source"] for x in b["items"] if x.get("sourceType")=="aggregator"})
    schema_source = "official" if verifiedBy == "official" else "aggregator"  # enum-safe. [1][3][5]

    # compute unique id from sources + uniqueKey
    base_id = hashlib.sha1(f"{'|'.join(sources)}|{rep['uniqueKey']}".encode("utf-8")).hexdigest()[:12]
    rid = base_id
    n=1
    while rid in seen_ids:
      rid = hashlib.sha1(f"{base_id}|{n}".encode("utf-8")).hexdigest()[:12]; n+=1
    seen_ids.add(rid)

    rep_rec = {
      "id": rid,
      "slug": rid,
      "title": rep["title"],
      "organization": "/".join(sources) if sources else rep["organization"],
      "qualificationLevel": "Graduate",
      "domicile": rep["domicile"],
      "source": schema_source,
      "verifiedBy": verifiedBy,
      "type": "VACANCY",
      "updateSummary": None,
      "relatedTo": None,
      "deadline": rep.get("deadline"),
      "applyLink": rep.get("applyLink"),
      "pdfLink": rep.get("pdfLink"),
      "extractedAt": UTC_NOW.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    published.append(rep_rec)

    # attach updates with unique ids
    for u in (x for x in b["items"] if x.get("isUpdate")):
      up_base = hashlib.sha1(f"{'|'.join(sources)}|{u['uniqueKey']}".encode("utf-8")).hexdigest()[:12]
      uid = up_base; m=1
      while uid in seen_ids:
        uid = hashlib.sha1(f"{up_base}|{m}".encode("utf-8")).hexdigest()[:12]; m+=1
      seen_ids.add(uid)

      upd = {
        "id": uid,
        "slug": uid,
        "title": "[UPDATE] " + u["title"],
        "organization": rep_rec["organization"],
        "qualificationLevel": "Graduate",
        "domicile": rep["domicile"],
        "source": schema_source,
        "verifiedBy": verifiedBy,
        "type": "UPDATE",
        "updateSummary": u.get("updateSummary"),
        "relatedTo": rep_rec["slug"],
        "deadline": u.get("deadline") or rep_rec["deadline"],
        "applyLink": u.get("applyLink") or rep_rec["applyLink"],
        "pdfLink": u.get("pdfLink") or rep_rec["pdfLink"],
        "extractedAt": UTC_NOW.strftime("%Y-%m-%dT%H:%M:%SZ"),
      }
      published.append(upd)

  return published, pending

def load_data():
  base={"jobListings": [], "transparencyInfo": {}}
  if DATA_PATH.exists():
    try: base=json.loads(DATA_PATH.read_text(encoding="utf-8"))
    except Exception: pass
  base.setdefault("jobListings",[]); base.setdefault("transparencyInfo",{})
  return base

def save_data(data): DATA_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

def drop_expired(listings):
  out=[]; today = datetime.now().date()
  for j in listings:
    dl=j.get("deadline")
    if dl:
      try:
        d = dateparser.parse(dl).date()
        if d < today: continue
      except Exception: pass
    out.append(j)
  return out

def filter_open_only(listings):
  today=datetime.now().date(); out=[]
  for rec in listings:
    dl = rec.get("deadline")
    if not dl: 
      continue  # conservative: no explicit deadline => don't publish
    try:
      d = dateparser.parse(dl).date()
    except Exception:
      continue
    if d >= today:
      out.append(rec)
  return out  # ensures only open vacancies appear (e.g., suppress expired SBI PO). [25][26]

def main():
  collected=[]; agg_counts={}; tg_counts={}
  for src in AGG_SOURCES:
    its = scrape_aggregator_page(src); collected.extend(its); agg_counts[src["name"]] = len(its)
  for ch in TELEGRAM_CHANNELS:
    its = scrape_telegram_channel(ch); collected.extend(its); tg_counts[ch] = len(its)

  prev = load_data()
  prev_pending = set(prev.get("transparencyInfo",{}).get("pendingFromTelegram",[]))
  published, pending = merge_and_mark(collected, prev_pending)

  open_only = filter_open_only(published)      # open-window only
  cleaned   = drop_expired(open_only)          # secondary safeguard

  data = prev
  data["jobListings"] = cleaned
  ti = data.setdefault("transparencyInfo",{})
  ti["lastUpdated"] = UTC_NOW.strftime("%Y-%m-%dT%H:%M:%SZ")
  ti["totalListings"] = len(cleaned)
  ti["aggCounts"] = agg_counts
  ti["telegramCounts"] = tg_counts
  ti["pendingFromTelegram"] = sorted(pending)
  ti["notes"] = "Open-window only; unique ids for updates; schema-safe source; verifiedBy confidence; OTR excluded; All-India or Bihar-only allowed."
  save_data(data)
  print(f"[INFO] agg_total={sum(agg_counts.values())} tg_total={sum(tg_counts.values())} published={len(cleaned)} pending={len(pending)}")

if __name__=="__main__":
  main()
