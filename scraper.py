# scraper.py — Robust networking (longer timeouts, retries, CA bundle, CF + scoped SSL fallback) + resilient discovery.

import os, re, json, hashlib, threading, xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests, certifi, dateparser
from bs4 import BeautifulSoup, UnicodeDammit
from requests.adapters import HTTPAdapter
try:
    from urllib3.util import Retry
except Exception:
    Retry = None

# Optional Cloudflare-aware fallback
try:
    import cloudscraper
    CF = cloudscraper.create_scraper()
except Exception:
    CF = None

DATA_PATH = Path("data.json")
UTC_NOW = datetime.now(timezone.utc)

# Timeouts tuned for public portals (connect 12s, read 35s)
CONNECT_TO, READ_TO = 12, 35
LIST_TO, DETAIL_TO = (CONNECT_TO, READ_TO), (CONNECT_TO, READ_TO)
MAX_WORKERS = 10
PER_SOURCE_MAX = 160

HEADERS = {
  "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
  "Accept-Language": "en-IN,en;q=0.9",
  "Cache-Control": "no-cache"
}

def session_with_retries(pool=64):
  s = requests.Session()
  s.headers.update(HEADERS)
  s.verify = certifi.where()  # modern CA bundle for SSL verification [6][7]
  if Retry:
    retry = Retry(
      total=5, connect=4, read=4,
      backoff_factor=0.9,
      status_forcelist=[403,429,500,502,503,504],
      allowed_methods={"GET","HEAD"}
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=pool, pool_maxsize=pool)
  else:
    adapter = HTTPAdapter(pool_connections=pool, pool_maxsize=pool)
  s.mount("http://", adapter); s.mount("https://", adapter)
  return s  # Requests + HTTPAdapter + Retry per best practice. [3][5]

HTTP = session_with_retries()
_thread_local = threading.local()
def thread_session():
  if not hasattr(_thread_local, "s"):
    _thread_local.s = session_with_retries()
  return _thread_local.s

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

OFFICIAL_SSL_FALLBACK = {"bpsc.bihar.gov.in"}  # domain-scoped verify=False retry (last resort) [6][7]

def fetch(url, timeout):
  host = urlparse(url or "").netloc.lower()
  try:
    r = HTTP.get(url, timeout=timeout)
    r.raise_for_status()
    return r
  except requests.SSLError as e:
    if any(h in host for h in OFFICIAL_SSL_FALLBACK):
      # Scoped: retry once without verification for BPSC-like chains that fail on CI; log and continue. [6][7]
      r2 = requests.get(url, headers=HEADERS, timeout=sum(timeout), verify=False)
      r2.raise_for_status()
      return r2
    raise
  except requests.HTTPError as e:
    code = getattr(e.response, "status_code", 0)
    if code == 403 and CF:
      r2 = CF.get(url, timeout=sum(timeout))
      if getattr(r2, "status_code", 0) == 200:
        return r2
    raise

# Sources (aggregators + official)
AGG_SOURCES = [
  {"name":"Adda247",       "base":"https://www.adda247.com",      "url":"https://www.adda247.com/jobs/government-jobs/"},
  {"name":"SarkariExam",   "base":"https://www.sarkariexam.com",  "url":"https://www.sarkariexam.com"},
  {"name":"RojgarResult",  "base":"https://www.rojgarresult.com", "url":"https://www.rojgarresult.com/recruitments/"},
  {"name":"ResultBharat",  "base":"https://www.resultbharat.com", "url":"https://www.resultbharat.com"},
]  # CF fallback handles 403 where possible. [5]

OFFICIAL_SOURCES = [
  {"name":"DSSSB_Notice",   "base":"https://dsssb.delhi.gov.in", "url":"https://dsssb.delhi.gov.in/notice-of-exam"},
  {"name":"RRB_Chandigarh", "base":"https://www.rrbcdg.gov.in",  "url":"https://www.rrbcdg.gov.in"},
  {"name":"BPSC",           "base":"https://bpsc.bihar.gov.in",  "url":"https://bpsc.bihar.gov.in"},
]  # Direct discovery reduces dependence on blogs. [12][13][14]

# Telegram hints via RSSHub (multi-instance fallback)
def env_channels():
  raw=os.getenv("TELEGRAM_CHANNELS","").strip()
  return [x for x in raw.split(",") if x] or ["ezgovtjob"]
TELEGRAM_CHANNELS = env_channels()
TELEGRAM_RSS_BASES = [
  os.getenv("TELEGRAM_RSS_BASE") or "https://rsshub.app",
  "https://rsshub.netlify.app",
  "https://rsshub.rssforever.com",
]  # /telegram/channel/:username route per docs. [10][11]

# Official domains (verification)
OFFICIAL_DOMAINS = {
  "dsssb.delhi.gov.in","dsssbonline.nic.in",
  "www.rrbcdg.gov.in","rrbcdg.gov.in",
  "bpsc.bihar.gov.in","www.bpsc.bihar.gov.in",
  "ssc.gov.in","www.ssc.gov.in",
}

# Vacancy logic (same as previous resilient build) ...

# — for brevity in this reply, keep the same filter functions, education rules,
#   date parsing, and merge/publish code from your last working version. —

# IMPORTANT: retain the previously provided functions for:
# - contains_any, is_update, is_joblike
# - is_non_vacancy, other_state_only, education_allowed
# - norm_key, DATE_WORDS/_month_word_fix, PAT_TILL, parse_application_window, extract_deadline
# - looks_official
# - scrape_generic_list (uses thread_session().get with verify=certifi.where()), scrape_aggregator_page, scrape_official_page
# - telegram_feed_urls, scrape_telegram_channel
# - merge_and_mark (enum-safe source; verifiedBy), load_data/save_data
# - drop_expired, filter_open_only, main()

# Paste those unchanged from the immediately previous working version,
# and ONLY replace the networking, fetch, sources, and TELEGRAM sections with this updated block.
