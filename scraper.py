# scraper.py — production-ready: stable IDs, explicit decoding, shorter timeouts,
# retry-hardening, non-HTML skip, per-source caps, and 8-thread concurrent fetch.
# All try blocks have matching except blocks to avoid SyntaxError.

import json, re, hashlib, threading
from pathlib import Path
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin, urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from bs4 import BeautifulSoup, UnicodeDammit
import dateparser
import urllib.robotparser as robotparser
from concurrent.futures import ThreadPoolExecutor, as_completed

DATA_PATH = Path("data.json")
UTC_NOW = datetime.now(timezone.utc)

# Tunables
CONNECT_TO = 6          # seconds (connect)
READ_TO    = 12         # seconds (read)
LIST_TO    = (CONNECT_TO, READ_TO)
DETAIL_TO  = (CONNECT_TO, READ_TO)
HEAD_TO    = 8
MAX_WORKERS = 8
PER_SOURCE_MAX = 120     # bound total work per source

SOURCES_OFFICIAL = [
    {"name": "SSC", "base": "https://ssc.gov.in", "url": "https://ssc.gov.in"},
    {"name": "SSC-ER", "base": "https://sscer.org", "url": "https://sscer.org"},
    {"name": "IBPS", "base": "https://www.ibps.in", "url": "https://www.ibps.in"},
    {"name": "RRB-CDG", "base": "https://www.rrbcdg.gov.in", "url": "https://www.rrbcdg.gov.in"},
    {"name": "RRB-Patna", "base": "https://www.rrbpatna.gov.in", "url": "https://www.rrbpatna.gov.in"},
    {"name": "BPSC", "base": "https://bpsc.bihar.gov.in", "url": "https://bpsc.bihar.gov.in/whats-new/"},
    {"name": "LIC", "base": "https://licindia.in", "url": "https://licindia.in/careers"},
    {"name": "NIACL", "base": "https://www.newindia.co.in", "url": "https://www.newindia.co.in/recruitment"},
    {"name": "UIIC", "base": "https://www.uiic.co.in", "url": "https://www.uiic.co.in/web/careers/recruitment"},
]

SOURCES_AGG = [
    {"name": "CareerPower", "base": "https://www.adda247.com", "url": "https://www.adda247.com/jobs/government-jobs/"},
    {"name": "SarkariExam", "base": "https://www.sarkariexam.com", "url": "https://www.sarkariexam.com"},
    {"name": "RojgarResult", "base": "https://www.rojgarresult.com", "url": "https://www.rojgarresult.com"},
    {"name": "SarkariResult", "base": "https://sarkariresult.com.cm", "url": "https://sarkariresult.com.cm/latest-jobs/"},
]

INCLUDE_EDU = [
    r"\bany\s+graduate\b",
    r"\bgraduate\s+in\s+any\s+(discipline|stream)\b",
    r"\bany\s+degree\b",
    r"\b12(?:th|th\s*pass| intermediate| senior\s+secondary)\b",
    r"\b10(?:th|th\s*pass| matric)\b",
]

ALLOWED_SKILLS = [
    r"\btyping\b", r"\bcomputer(?!\s*science)\b", r"\bpet\b", r"\bpst\b",
    r"\bphysical\b", r"\bms\s*office\b", r"\bccc\b", r"\bdca\b"
]

DISALLOWED_SKILLS = [
    r"\b(programming|coding|java|python|autocad|cad|sap|oracle|network|hardware|software|tally|marketing|sales|management)\b",
    r"\bcertificate|licen[cs]e|diploma\b"
]

EXCLUDE_DEGREE = [
    r"\b(b\.?tech|be\b|m\.?tech|engineering)\b", r"\bmba|pgdm|management\b",
    r"\blaw\b|\bllb\b|\ballm\b", r"\bnursing\b|\bgnm\b|\banm\b|\bpharma|bpharm|mpharm\b",
    r"\bmca\b|\bbca\b|\bcomputer\s+science\b|\bit\b", r"\b(b\.?ed|bed)\b",
    r"\b(diploma|iti)\b", r"\bca\b|\bcs\b|\bcma\b|\bicwa\b", r"\bmedical|mbbs|bds|ayush|veterinary\b",
]

EXCLUDE_NON_RECRUITMENT = [
    r"\badmit\s*card\b", r"\bresult\b", r"\banswer\s*key\b", r"\bexam\s*date\b", r"\bsyllabus\b"
]

INDIAN_STATES = [
    "andhra pradesh","arunachal pradesh","assam","bihar","chhattisgarh","goa","gujarat","haryana","himachal pradesh",
    "jharkhand","karnataka","kerala","madhya pradesh","maharashtra","manipur","meghalaya","mizoram","nagaland","odisha",
    "punjab","rajasthan","sikkim","tamil nadu","telangana","tripura","uttar pradesh","uttarakhand","west bengal",
]

ALLOW_OUTSIDE_PATTERNS = [r"\bany\s+state\b", r"\ball\s+india\b", r"\bfrom\s+any\s+state\b", r"\bdomicile\s+not\s+required\b", r"\bopen\s+to\s+all\b"]
RESTRICT_PAT = r"\b(domici(?:le|liary)|resident)\b.*?\b(only|required)\b"

HEADERS = {"User-Agent":"Mozilla/5.0 (VacancyBot)","Accept-Language":"en-IN,en;q=0.9","Cache-Control":"no-cache"}

def session_with_retries(pool=64):
  s = requests.Session()
  s.headers.update(HEADERS)
  retry = Retry(
      total=2, connect=2, read=2, backoff_factor=0.4,
      status_forcelist=[429,500,502,503,504],
      allowed_methods={"GET","HEAD"}
  )
  adapter = HTTPAdapter(max_retries=retry, pool_connections=pool, pool_maxsize=pool)
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
  r = HTTP.get(url, timeout=timeout)     # tuple timeouts: (connect, read)
  r.raise_for_status()
  return r

def decode_html_bytes(resp):
  enc = resp.encoding or getattr(resp, "apparent_encoding", None) or "utf-8"
  dammit = UnicodeDammit(resp.content, is_html=True, known_definite_encodings=[enc, "utf-8", "windows-1252", "iso-8859-1"])
  return dammit.unicode_markup

def head_ok(url):
  try:
    r = HTTP.head(url, timeout=HEAD_TO, allow_redirects=True)
    if r.status_code in (405, 501):
      r = HTTP.get(url, timeout=HEAD_TO, stream=True)
    return 200 <= r.status_code < 300
  except Exception:
    return False

def clean_title_for_id(title):
  t = norm(title).lower()
  t = re.sub(r"\b(corrigendum|addendum|notice|update|extension|date\s*extended)\b","",t,flags=re.I)
  t = re.sub(r"[\(\)\[\]\-:|,]"," ",t)
  t = re.sub(r"\b(recruitment|notification|advertisement|online\s*form|apply\s*online)\b","",t)
  return re.sub(r"[^a-z0-9\s]","",t).strip()

def make_id(org, slug):
  h = hashlib.sha1(f"{org.lower()}|{slug}".encode("utf-8")).hexdigest()[:12]
  return f"{org}:{h}"

def passes_education(text): return any(re.search(p,(text or "").lower()) for p in INCLUDE_EDU)
def passes_skill_rule(text):
  t=(text or "").lower()
  if not re.search(r"\b(skill|certificate|course|experience|typing|computer|pet|pst|physical)\b",t): return True
  if any(re.search(p,t) for p in DISALLOWED_SKILLS): return False
  return any(re.search(p,t) for p in ALLOWED_SKILLS)
def passes_degree_exclusions(text): return not any(re.search(p,(text or "").lower()) for p in EXCLUDE_DEGREE)
def is_recruitment(text): return not any(re.search(p,(text or "").lower()) for p in EXCLUDE_NON_RECRUITMENT)

def is_all_india(text): return any(re.search(p,(text or "").lower()) for p in ALLOW_OUTSIDE_PATTERNS)
def is_bihar_only(text):
  t=(text or "").lower()
  return ("bihar" in t) and (re.search(RESTRICT_PAT,t) is not None)
def other_state_only(text):
  t=(text or "").lower()
  for st in INDIAN_STATES:
    if st=="bihar": continue
    if re.search(rf"\b{re.escape(st)}\b.*{RESTRICT_PAT}",t) or re.search(rf"{RESTRICT_PAT}.*\b{re.escape(st)}\b",t): return True
  return False
def eligible_by_domicile(org, detail_text):
  if is_all_india(detail_text): return True
  if ("bpsc" in (org or "").lower()) or is_bihar_only(detail_text): return True
  if other_state_only(detail_text): return False
  return False

def classify_level(text):
  t=(text or "").lower()
  if re.search(r"\b10(th| matric)\b",t): return "10th"
  if re.search(r"\b12(th| intermediate| senior\s+secondary)\b",t): return "12th"
  return "Graduate"

DATE_HINTS = [
  r"last\s*date[:\-\s]*([^\n<]{6,30})", r"closing\s*date[:\-\s]*([^\n<]{6,30})",
  r"apply\s*online\s*last\s*date[:\-\s]*([^\n<]{6,30})", r"last\s*date\s*to\s*apply[:\-\s]*([^\n<]{6,30})",
]
EXTENSION_HINTS = [r"\bextension\b", r"\bextended\s+to\b", r"\blast\s*date\s*(?:extended|revised)\s*to\b", r"\bdate\s*extended\b"]

def extract_deadline(text):
  for patt in DATE_HINTS:
    m=re.search(patt,text,flags=re.I)
    if m:
      cand=norm(m.group(1))
      dt=dateparser.parse(cand, settings={"DATE_ORDER":"DMY"})
      if dt: return dt.date().isoformat()
  dt=dateparser.parse(text, settings={"PREFER_DATES_FROM":"future","DATE_ORDER":"DMY"})
  return dt.date().isoformat() if dt and dt.date()>=UTC_NOW.date() else None

def find_extension_date(text):
  if any(re.search(p,text,flags=re.I) for p in EXTENSION_HINTS):
    dt=dateparser.parse(text, settings={"PREFER_DATES_FROM":"future","DATE_ORDER":"DMY"})
    if dt: return dt.date().isoformat()
  return None

_ROBOTS = {}
def robots_allowed(url, ua=HEADERS.get("User-Agent","*")):
  try:
    p = urlparse(url); root = f"{p.scheme}://{p.netloc}"
    rp = _ROBOTS.get(root)
    if not rp:
      rp = robotparser.RobotFileParser()
      rp.set_url(urljoin(root, "/robots.txt"))
      try:
        rp.read()
      except Exception:
        pass
      _ROBOTS[root] = rp
    return rp.can_fetch(ua, url) if rp else True
  except Exception:
    return True

def soup_from_resp(resp):
  ct = (resp.headers.get("Content-Type") or "").lower()
  if ("html" not in ct) and ("text/" not in ct): return None
  html = decode_html_bytes(resp)
  try:
    return BeautifulSoup(html, "lxml")
  except Exception:
    return BeautifulSoup(html, "html.parser")

def scrape_html_list(list_url, base, org):
  items=[]
  try:
    if not robots_allowed(list_url):
      print(f"[INFO] robots disallow list: {list_url}")
      return items

    list_resp = fetch(list_url, LIST_TO)
    soup = soup_from_resp(list_resp)
    if soup is None: return items

    anchors=[]
    for a in soup.find_all("a", href=True):
      href = absolute(base, a["href"])
      if not href or href.startswith("mailto:") or href.startswith("tel:"):
        continue
      if any(href.lower().endswith(ext) for ext in (".jpg",".png",".gif",".zip",".rar",".7z",".xlsx",".xls",".doc",".docx")):
        continue
      title = norm(a.get_text(" "))
      if len(title) < 8:
        continue
      anchors.append((title, href))
      if len(anchors) >= PER_SOURCE_MAX:
        break

    def fetch_detail(pair):
      title, href = pair
      s = thread_session()
      if not robots_allowed(href):
        return None
      try:
        resp = s.get(href, timeout=DETAIL_TO)
        resp.raise_for_status()
        dsoup = soup_from_resp(resp)
        detail_text = norm(dsoup.get_text(" ")) if dsoup is not None else title
      except Exception:
        detail_text = title

      combo = f"{title} — {detail_text}"
      if not (passes_education(combo) and passes_skill_rule(combo) and
              passes_degree_exclusions(combo) and is_recruitment(combo) and
              eligible_by_domicile(org, detail_text)):
        return None

      slug=clean_title_for_id(title)
      domicile_label="Bihar" if (("bpsc" in org.lower()) or ("bihar" in detail_text.lower())) else "All India"
      deadline=extract_deadline(detail_text)
      ext=find_extension_date(detail_text)
      deadline = ext or deadline

      it={
        "id": make_id(org, slug),
        "slug": slug,
        "title": title,
        "organization": org,
        "qualificationLevel": classify_level(f"{title} {detail_text}"),
        "domicile": domicile_label,
        "source": "official",
        "deadline": deadline,
        "applyLink": href,
        "pdfLink": href,
        "extractedAt": UTC_NOW.strftime("%Y-%m-%dT%H:%M:%SZ"),
      }
      it["_link_ok"] = head_ok(it["applyLink"] or it["pdfLink"])
      return it

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
      futures = [ex.submit(fetch_detail, pair) for pair in anchors]
      for fut in as_completed(futures):
        it = fut.result()
        if it:
          items.append(it)

  except Exception as e:
    print(f"[WARN] {org} scrape error: {e}")
  return items

def scrape_official_all():
  out=[]
  for s in SOURCES_OFFICIAL:
    print("Scraping official:", s["name"])
    out.extend(scrape_html_list(s["url"], s["base"], s["name"]))
  return out

def scrape_aggregators_all():
  out=[]
  for s in SOURCES_AGG:
    print("Scraping aggregator:", s["name"])
    items=scrape_html_list(s["url"], s["base"], s["name"])
    for it in items:
      it["source"]="aggregator"
      if it["domicile"]!="Bihar":
        it["domicile"]="All India"
    out.extend(items)
  return out

def merge_with_fallback(official_items, aggregator_items):
  by_slug={}
  for it in official_items:
    by_slug.setdefault(it["slug"], it)
  for agg in aggregator_items:
    slug=agg["slug"]
    if slug not in by_slug:
      by_slug[slug]=agg
      continue
    off=by_slug[slug]
    if agg.get("deadline") and (not off.get("deadline")):
      off["deadline"]=agg["deadline"]
    if not off.get("_link_ok"):
      off["applyLink"]=agg.get("applyLink") or off.get("applyLink")
      off["pdfLink"]=agg.get("pdfLink") or off.get("pdfLink")
      off["source"]="aggregator"
  merged=[]
  for it in by_slug.values():
    it.pop("_link_ok", None)
    merged.append(it)
  return merged

def drop_expired(listings):
  out=[]
  for j in listings:
    dl=j.get("deadline")
    if dl:
      try:
        d=datetime.fromisoformat(dl)
      except Exception:
        dt=dateparser.parse(dl)
        d=dt if isinstance(dt, datetime) else None
      if d and d.date()<UTC_NOW.date():
        continue
    if j.get("extractedAt"):
      try:
        ext=datetime.fromisoformat(j["extractedAt"].replace("Z",""))
        if ext < UTC_NOW - timedelta(days=120):
          continue
      except Exception:
        pass
    out.append(j)
  return out

def load_data():
  base={"jobListings": [], "transparencyInfo": {}}
  if DATA_PATH.exists():
    try:
      base=json.loads(DATA_PATH.read_text(encoding="utf-8"))
    except Exception:
      pass
  base.setdefault("jobListings",[])
  base.setdefault("transparencyInfo",{})
  return base

def save_data(data):
  DATA_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

def main():
  official=scrape_official_all()
  aggs=scrape_aggregators_all()
  merged=merge_with_fallback(official, aggs)
  cleaned=drop_expired(merged)

  data=load_data()
  data["jobListings"]=cleaned
  data["transparencyInfo"]["lastUpdated"]=UTC_NOW.strftime("%Y-%m-%dT%H:%M:%SZ")
  data["transparencyInfo"]["totalListings"]=len(cleaned)
  save_data(data)
  print(f"Saved {len(cleaned)} listings")

if __name__=="__main__":
  main()
