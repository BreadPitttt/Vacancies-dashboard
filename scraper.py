import requests
from bs4 import BeautifulSoup
import json
import logging
from datetime import datetime, date
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
REQUEST_TIMEOUT = 18 if IS_LIGHT else 20
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
