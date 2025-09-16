# scraper.py — Aggregators + Telegram(RSS) hints with strict verification
# Fixes: standard Chrome UA (better HTML), education optional (recruitment wording still required)

import json, re, hashlib, threading, os, xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin, urlparse

import requests
from requests.adapters import HTTPAdapter
try:
    from urllib3.util.retry import Retry
except Exception:
    try:
        from urllib3.util import Retry
    except Exception:
        Retry = None

from bs4 import BeautifulSoup, UnicodeDammit
import dateparser
from concurrent.futures import ThreadPoolExecutor, as_completed

DATA_PATH = Path("data.json")
UTC_NOW = datetime.now(timezone.utc)

CONNECT_TO, READ_TO = 6, 12
LIST_TO, DETAIL_TO = (CONNECT_TO, READ_TO), (CONNECT_TO, READ_TO)
HEAD_TO = 8
MAX_WORKERS = 8
PER_SOURCE_MAX = 200

# Use a standard Chrome UA for better compatibility with aggregator sites
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
  return s

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

def head_ok(url):
  try:
    r = HTTP.head(url, timeout=HEAD_TO, allow_redirects=True)
    if r.status_code in (405,501): r = HTTP.get(url, timeout=HEAD_TO, stream=True)
    return 200 <= r.status_code < 300
  except Exception:
    return False

AGG_SOURCES = [
  {"name":"Adda247",       "base":"https://www.adda247.com",      "url":"https://www.adda247.com/jobs/government-jobs/"},
  {"name":"SarkariExam",   "base":"https://www.sarkariexam.com",  "url":"https://www.sarkariexam.com"},
  {"name":"RojgarResult",  "base":"https://www.rojgarresult.com", "url":"https://www.rojgarresult.com/recruitments/"},
  {"name":"SarkariExamCM", "base":"https://sarkariexam.com.cm",   "url":"https://sarkariexam.com.cm"},
]  # Aggregators are the primary discovery sources. [5][6]

TELEGRAM_CHANNELS = ["ezgovtjob", "sarkariresulinfo"]  # hints only
TELEGRAM_RSS_BASES = [
  os.getenv("TELEGRAM_RSS_BASE") or "https://rsshub.app",
  "https://rsshub.netlify.app",
]  # RSSHub exposes public Telegram channels via RSS. [5][6]

OFFICIAL_DOMAINS = {
  "ssc.gov.in","www.ssc.gov.in",
  "bssc.bihar.gov.in","onlinebssc.com",
  "dsssb.delhi.gov.in","dsssbonline.nic.in",
  "www.ibps.in",
  "bpsc.bihar.gov.in","bpsconline.bihar.gov.in",
  "www.rbi.org.in","opportunities.rbi.org.in",
  "ccras.nic.in",
  "www.rrbcdg.gov.in","www.rrbpatna.gov.in",
}  # Official domains pass verification immediately. [4]

RECRUITMENT_TERMS = [r"\brecruitment\b", r"\bvacanc(?:y|ies)\b", r"\badvertisement\b", r"\bnotification\b", r"\bonline\s*form\b", r"\bapply\s*online\b"]
# Education now optional; keep as a quality signal but not a hard requirement
EDU_HINTS = [r"\b10\s*(?:th|matric)\b", r"\b12\s*(?:th|inter(?:mediate)?)\b", r"\bgraduate\b", r"\bany\s+graduate\b"]
EXCLUDE_DEGREE = [r"\b(b\.?tech|be\b|m\.?tech|engineering)\b", r"\bmba|pgdm|management\b",
                  r"\bmbbs|bds|ayush|gnm|anm|nursing\b", r"\bca\b|\bcs\b|\bcma\b|\bicwa\b", r"\blaw\b|\bllb\b|\ballm\b"]
EXCLUDE_NON_RECRUITMENT = [r"\bcharter\b", r"\bcalendar\b", r"\bcommittee\b", r"\bwebinar\b", r"\bwellness\b",
                           r"\bcancellation\b", r"\bcancelled\b", r"\bdebarred\b", r"\bwalk\s*through\b", r"\btraining\b", r"\bvideo\b"]
UPDATE_TERMS = [r"\bcorrigendum\b", r"\baddendum\b", r"\bamendment\b", r"\brevised\b", r"\bcorrection\b",
                r"\bdate\s*(?:extended|extension)\b", r"\bextension\s*notice\b", r"\bpostponed\b", r"\brescheduled\b", r"\bedit\s*window\b"]

INDIAN_STATES = ["andhra pradesh","arunachal pradesh","assam","bihar","chhattisgarh","goa","gujarat","haryana","himachal pradesh",
 "jharkhand","karnataka","kerala","madhya pradesh","maharashtra","manipur","meghalaya","mizoram","nagaland","odisha",
 "punjab","rajasthan","sikkim","tamil nadu","telangana","tripura","uttar pradesh","uttarakhand","west bengal"]
RESTRICT_PAT = r"\b(domici(?:le|liary)|resident)\b.*?\b(only|required)\b"

def contains_any(patterns, text):
  t=(text or "").lower()
  return any(re.search(p, t) for p in patterns)

def is_update(text): return contains_any(UPDATE_TERMS, text or "")

def excluded_notice(text):
  t=(text or "").lower()
  return contains_any(EXCLUDE_NON_RECRUITMENT, t) or contains_any(EXCLUDE_DEGREE, t)

def has_minimum_signals(text):
  # Require recruitment wording; education is optional
  return contains_any(RECRUITMENT_TERMS, (text or "").lower())

def other_state_only(text):
  t=(text or "").lower()
  for st in INDIAN_STATES:
    if st=="bihar": continue
    if re.search(rf"\b{re.escape(st)}\b.*{RESTRICT_PAT}", t) or re.search(rf"{RESTRICT_PAT}.*\b{re.escape(st)}\b", t):
      return True
  return False

def eligible_by_domicile(detail_text): return not other_state_only(detail_text or "")

def classify_level(text):
  t=(text or "").lower()
  if re.search(r"\b10(th| matric)\b",t): return "10th"
  if re.search(r"\b12(th| intermediate| senior\s+secondary)\b",t): return "12th"
  return "Graduate"

def clean_title_for_id(title):
  t = norm(title).lower()
  t = re.sub(r"[\(\)\[\]\-:|,]"," ",t)
  t = re.sub(r"\b(recruitment|notification|advertisement|online\s*form|apply\s*online)\b","",t)
  return re.sub(r"[^a-z0-9\s]","",t).strip()

def base_slug_without_update(text):
  t = norm(text or "").lower()
  t = re.sub("|".join(UPDATE_TERMS), " ", t)
  return re.sub(r"[^a-z0-9\s]","",t).strip()

def make_id(org, slug): return hashlib.sha1(f"{org.lower()}|{slug}".encode("utf-8")).hexdigest()[:12]

def extract_deadline(text):
  m = re.search(r"(last\s*date|closing\s*date)[^\n]{0,20}(\d{1,2}[-/\.]\d{1,2}[-/\.]\d{2,4})", (text or ""), flags=re.I)
  if m:
    dt = dateparser.parse(m.group(2), settings={"DATE_ORDER":"DMY"})
    if dt: return dt.date().isoformat()
  dt = dateparser.parse(text or "", settings={"PREFER_DATES_FROM":"future","DATE_ORDER":"DMY"})
  return dt.date().isoformat() if dt and dt.date()>=UTC_NOW.date() else None

def get_domain(u):
  try: return urlparse(u).netloc.lower()
  except Exception: return ""

def looks_official(url): return get_domain(url) in OFFICIAL_DOMAINS

def scrape_aggregator_page(src):
  items=[]
  try:
    resp = fetch(src["url"], LIST_TO)
    soup = soup_from_resp(resp)
    if soup is None: return items

    anchors=[]
    for a in soup.find_all("a", href=True):
      href = absolute(src["base"], a["href"])
      if not href: continue
      title = norm(a.get_text(" "))
      if len(title) < 8: continue
      if re.search(r"\b(admit\s*card|answer\s*key|result|syllabus)\b", title, flags=re.I): continue
      anchors.append((title, href))
      if len(anchors) >= PER_SOURCE_MAX: break

    def enrich(pair):
      title, href = pair
      s = thread_session()
      detail_text = title
      try:
        r = s.get(href, timeout=DETAIL_TO); r.raise_for_status()
        ds = soup_from_resp(r)
        if ds is not None: detail_text = norm(ds.get_text(" "))
      except Exception: pass
      combo = f"{title} — {detail_text}"
      if excluded_notice(combo): return None
      if is_update(combo):
        if not contains_any(RECRUITMENT_TERMS, combo): return None
      else:
        if not has_minimum_signals(combo): return None
      if not eligible_by_domicile(combo): return None

      base = base_slug_without_update(title)
      slug = clean_title_for_id(title) if not is_update(title) else clean_title_for_id(f"{base}-update")
      return {
        "slug": slug, "parentSlug": clean_title_for_id(base) if base else None,
        "title": title, "organization": src["name"],
        "applyLink": href, "pdfLink": href,
        "deadline": extract_deadline(combo),
        "qualificationLevel": classify_level(combo),
        "domicile": "Bihar" if "bihar" in combo.lower() else "All India",
        "sourceType": "aggregator", "source": src["name"],
        "isUpdate": is_update(combo), "updateSummary": title if is_update(combo) else None,
        "detailText": combo
      }

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
      for fut in as_completed([ex.submit(enrich, pair) for pair in anchors]):
        it = fut.result()
        if it: items.append(it)
    print(f"[AGG] {src['name']}: raw={len(anchors)} kept={len(items)}")
  except Exception as e:
    print(f"[WARN] aggregator {src['name']} error: {e}")
  return items

def telegram_feed_url(username):
  for base in TELEGRAM_RSS_BASES:
    if base:
      return f"{base.rstrip('/')}/telegram/channel/{username}"
  return None  # RSSHub exposes public channel feeds. [5][6]

def scrape_telegram_channel(username):
  items=[]
  try:
    url = telegram_feed_url(username)
    if not url: return items
    resp = fetch(url, LIST_TO)
    root = ET.fromstring(resp.content)
    def tx(node, tag): 
      el = node.find(tag); return norm(el.text) if (el is not None and el.text) else ""
    for it in root.findall(".//item"):
      title = tx(it,"title"); link = tx(it,"link"); desc = tx(it,"description")
      combo = norm(f"{title} — {desc}")
      if len(title) < 8: continue
      if excluded_notice(combo): continue
      if is_update(combo):
        if not contains_any(RECRUITMENT_TERMS, combo): continue
      else:
        if not has_minimum_signals(combo): continue
      if not eligible_by_domicile(combo): continue
      base = base_slug_without_update(title)
      slug = clean_title_for_id(title) if not is_update(title) else clean_title_for_id(f"{base}-update")
      items.append({
        "slug": slug, "parentSlug": clean_title_for_id(base) if base else None,
        "title": title, "organization": f"Telegram:{username}",
        "applyLink": link or None, "pdfLink": link or None,
        "deadline": extract_deadline(combo),
        "qualificationLevel": classify_level(combo),
        "domicile": "Bihar" if "bihar" in combo.lower() else "All India",
        "sourceType": "telegram", "source": f"Telegram:{username}",
        "isUpdate": is_update(combo), "updateSummary": title if is_update(combo) else None,
        "detailText": combo
      })
    print(f"[TG ] {username}: kept={len(items)} via RSS")
  except Exception as e:
    print(f"[WARN] telegram {username} error: {e}")
  return items

def cross_verify_and_pending(collected, pending_prev):
  buckets={}
  for it in collected:
    key = it["parentSlug"] or it["slug"]
    b = buckets.setdefault(key, {"aggs": set(), "hasOfficial": False, "teleSeen": False, "items": []})
    if it["sourceType"] == "aggregator": b["aggs"].add(it["source"])
    if it["sourceType"] == "telegram": b["teleSeen"] = True
    if looks_official(it.get("applyLink") or "") or looks_official(it.get("pdfLink") or ""): b["hasOfficial"] = True
    b["items"].append(it)

  published=[]; pending=set()
  for key, b in buckets.items():
    can_publish = b["hasOfficial"] or (len(b["aggs"]) >= 2)
    if can_publish:
      base = None; updates=[]
      for it in b["items"]:
        if it["sourceType"] == "aggregator" and not it["isUpdate"] and base is None:
          base = it
        if it["isUpdate"]: updates.append(it)
      chosen=[]
      if base: chosen.append(base)
      chosen.extend(updates)
      for it in chosen:
        sources = sorted({x["source"] for x in b["items"] if x["sourceType"]=="aggregator"})
        it["organization"] = "/".join(sources) if sources else it["organization"]
        it["source"] = "verified"
        published.append(it)
    else:
      if b["teleSeen"]: pending.add(key)

  pending_now = (pending_prev or set()) | pending
  for it in published:
    key = it["parentSlug"] or it["slug"]
    if key in pending_now: pending_now.remove(key)
  return published, pending_now

def drop_expired(listings):
  out=[]
  for j in listings:
    dl=j.get("deadline")
    if dl:
      try: d=datetime.fromisoformat(dl)
      except Exception:
        dt=dateparser.parse(dl); d=dt if isinstance(dt, datetime) else None
      if d and d.date()<UTC_NOW.date(): continue
    out.append(j)
  return out

def load_data():
  base={"jobListings": [], "transparencyInfo": {}}
  if DATA_PATH.exists():
    try: base=json.loads(DATA_PATH.read_text(encoding="utf-8"))
    except Exception: pass
  base.setdefault("jobListings",[]); base.setdefault("transparencyInfo",{})
  return base

def save_data(data): DATA_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

def main():
  collected=[]; agg_counts={}
  for src in AGG_SOURCES:
    its = scrape_aggregator_page(src)
    collected.extend(its); agg_counts[src["name"]] = len(its)

  tg_counts={}
  for ch in TELEGRAM_CHANNELS:
    its = scrape_telegram_channel(ch)
    collected.extend(its); tg_counts[ch] = len(its)

  data_prev = load_data()
  prev_pending = set(data_prev.get("transparencyInfo",{}).get("pendingFromTelegram",[]))
  verified, pending = cross_verify_and_pending(collected, prev_pending)

  cleaned = drop_expired(verified)
  data = data_prev
  data["jobListings"] = []
  for it in cleaned:
    rec = {
      "id": make_id(it["organization"], it["slug"]),
      "slug": it["slug"],
      "title": ("[UPDATE] " + it["title"]) if it.get("isUpdate") else it["title"],
      "organization": it["organization"],
      "qualificationLevel": it["qualificationLevel"],
      "domicile": it["domicile"],
      "source": it["source"],
      "type": "UPDATE" if it.get("isUpdate") else "VACANCY",
      "updateSummary": it.get("updateSummary"),
      "relatedTo": it.get("parentSlug"),
      "deadline": it.get("deadline"),
      "applyLink": it.get("applyLink"),
      "pdfLink": it.get("pdfLink"),
      "extractedAt": UTC_NOW.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    data["jobListings"].append(rec)

  ti = data.setdefault("transparencyInfo",{})
  ti["lastUpdated"] = UTC_NOW.strftime("%Y-%m-%dT%H:%M:%SZ")
  ti["totalListings"] = len(data["jobListings"])
  ti["aggCounts"] = agg_counts
  ti["telegramCounts"] = tg_counts
  ti["pendingFromTelegram"] = sorted(pending)
  ti["notes"] = "Telegram used as hints; publish only after official-domain or >=2 aggregators; pending hints auto-publish once confirmed."

  save_data(data)
  print(f"[INFO] agg={sum(agg_counts.values())} tg={sum(tg_counts.values())} verified={len(cleaned)} pending={len(pending)}")

if __name__=="__main__":
  main()
