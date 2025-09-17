import requests
from bs4 import BeautifulSoup
import json
import logging
from datetime import datetime
import re
import time
from urllib.parse import urljoin, urlparse

# ---------------- Configuration ----------------

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

REQUEST_TIMEOUT = 20
REQUEST_SLEEP_SECONDS = 1.2
FIRST_SUCCESS_MODE = True  # stop at first source that returns jobs

SOURCES = [
    {"name": "freejobalert",   "url": "https://www.freejobalert.com/", "parser": "parse_freejobalert"},
    {"name": "sarkarijobfind", "url": "https://sarkarijobfind.com/",   "parser": "parse_sarkarijobfind"},
    {"name": "resultbharat",   "url": "https://www.resultbharat.com/", "parser": "parse_resultbharat"},
    {"name": "adda247",        "url": "https://www.adda247.com/jobs/", "parser": "parse_adda247"},
]

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36'
}

# ------------- Helpers: policy and schema fields -------------

def now_iso():
    return datetime.now().isoformat()

def clean_text(x):
    return re.sub(r'\s+', ' ', x or '').strip()

def normalize_url(base, href):
    if not href:
        return None
    return urljoin(base, href)

def slugify(text):
    t = (text or "").lower()
    t = re.sub(r'[^a-z0-9]+', '-', t).strip('-')
    return t[:80] or 'job'

def derive_slug(title, link):
    s = slugify(title)
    if s == 'job' and link:
        path = urlparse(link).path
        s = slugify(path.split('/')[-1])
    return s or 'job'

def make_id(prefix, link):
    if not link:
        return f"{prefix}_no_link"
    parsed = urlparse(link)
    key = parsed.path + ("?" + parsed.query if parsed.query else "")
    return f"{prefix}_{key.strip('/') or 'root'}"

# -------- General-vacancy policy (strict) --------
# Allowed education bands only: 10th pass, 12th pass, Any graduate
# No technical/management/postgraduate. Allowed skills only: Typing, Computer operations, Physical.
# Teacher roles and variants must be excluded entirely.

TEACHER_TERMS = {
    "teacher","tgt","pgt","prt","school teacher","faculty","lecturer",
    "assistant professor","professor","b.ed","bed ","d.el.ed","deled","ctet","tet "
}

def education_band_from_text(text):
    t = (text or "").lower()
    if any(k in t for k in ["10th", "matric", "ssc "]): return "10th pass"
    if any(k in t for k in ["12th", "intermediate", "hsc"]): return "12th pass"
    if "graduate" in t or "bachelor" in t: return "Any graduate"
    return "N/A"

def violates_general_policy(text):
    t = (text or "").lower()

    # Hard exclusion for teacher roles and related qualifications/tests
    if any(k in t for k in TEACHER_TERMS):
        return True

    # Explicit higher/technical/management keywords
    disallow = [
        "b.tech", "btech", "b.e", "be ", "m.tech", "mtech", "m.e", "mca", "bca",
        "mba", "cma", "cfa", "ca ", "pg ", "post graduate", "postgraduate",
        "phd", "m.sc", "msc", "m.a", "ma ", "m.com", "mcom", "mphil",
        "engineer", "developer", "scientist", "research associate", "architect",
        "data scientist", "ml engineer", "cloud engineer", "sde", "devops"
    ]
    return any(k in t for k in disallow)

def allowed_basic_skill(text):
    t = (text or "").lower()
    return any(k in t for k in ["typing", "computer", "physical", "field duty"])

def derive_post(text):
    t = (text or "").lower()
    # Intentionally exclude teacher variants from post detection
    for key in [
        "constable", "clerk", "apprentice", "technician", "je", "ae",
        "officer", "assistant", "mts", "multi tasking",
        "data entry", "operator", "guard", "peon", "si", "jlo", "nursing officer"
    ]:
        if key in t:
            return key.title()
    return "N/A"

def derive_org(text):
    t = (text or "").upper()
    for key in ["SSC","UPSC","RRB","RPF","IBPS","SBI","PNB","RPSC","UPPSC","BPSC","HPSC",
                "DSSSB","NVS","AAI","IOCL","NTPC","DRDO","ISRO","NHAI","NHPC","PGCIL","NCL"]:
        if key in t:
            return key
    return "N/A"

def build_job(prefix, source_name, base_url, title, href, deadline="N/A"):
    title = clean_text(title)
    href_abs = normalize_url(base_url, href)

    # Apply strict general-vacancy filter and teacher exclusion
    if violates_general_policy(title):
        return None

    education = education_band_from_text(title)

    job = {
        "id": make_id(prefix, href_abs),
        "title": title or "N/A",
        "organization": derive_org(title),
        "deadline": deadline or "N/A",
        "applyLink": href_abs or base_url,

        # Required by schema
        "slug": derive_slug(title, href_abs),
        "qualificationLevel": education if education in ["10th pass", "12th pass", "Any graduate"] else "N/A",
        "domicile": "All India",
        "source": source_name,
        "type": "General",
        "extractedAt": now_iso(),

        # Optional meta
        "meta": {
            "post": derive_post(title),
            "allowedSkills": [k for k in ["Typing","Computer operations","Physical"] if allowed_basic_skill(title)],
            "sourceUrl": base_url
        }
    }
    return job

# ---------------- Parsers ----------------

def parse_freejobalert(content, source_name, base_url):
    soup = BeautifulSoup(content, 'html.parser')
    jobs = []
    # Generic anchors inside tables
    for tbl in soup.select('table'):
        for a in tbl.select('a[href]'):
            title = clean_text(a.get_text())
            href = a.get('href')
            if not title or not href:
                continue
            job = build_job('fja', source_name, base_url, title, href)
            if job:
                jobs.append(job)
    # Fallback: anchors with common recruitment cues
    if not jobs:
        for a in soup.select('a[href]'):
            title = clean_text(a.get_text())
            href = a.get('href')
            if not title or not href:
                continue
            if any(k in title.lower() for k in ["recruit", "vacancy", "notification"]):
                job = build_job('fja', source_name, base_url, title, href)
                if job:
                    jobs.append(job)
    return jobs

def parse_sarkarijobfind(content, source_name, base_url):
    soup = BeautifulSoup(content, 'html.parser')
    jobs = []
    # Look for headings with 'New Update' and list under it
    for h in soup.find_all(re.compile('^h[1-6]$')):
        if re.search(r'new\\s*update', h.get_text(), re.I):
            ul = h.find_next_sibling(['ul','ol'])
            if not ul:
                continue
            for li in ul.select('li'):
                a = li.find('a', href=True)
                if not a:
                    continue
                title = clean_text(a.get_text())
                href = a['href']
                job = build_job('sjf', source_name, base_url, title, href)
                if job:
                    jobs.append(job)
            break
    return jobs

def parse_resultbharat(content, source_name, base_url):
    soup = BeautifulSoup(content, 'html.parser')
    jobs = []
    for table in soup.select('table'):
        headers = [clean_text(th.get_text()) for th in table.select('th')]
        if not headers:
            continue
        col_idx = -1
        for i, h in enumerate(headers):
            if re.search(r'latest\\s*jobs', h, re.I):
                col_idx = i
                break
        if col_idx == -1:
            continue
        for tr in table.select('tr'):
            tds = tr.select('td')
            if col_idx < len(tds):
                a = tds[col_idx].find('a', href=True)
                if not a:
                    continue
                title = clean_text(a.get_text())
                href = a['href']
                job = build_job('rb', source_name, base_url, title, href)
                if job:
                    jobs.append(job)
    return jobs

def parse_adda247(content, source_name, base_url):
    soup = BeautifulSoup(content, 'html.parser')
    jobs = []
    # Try common card/article anchors
    for a in soup.select('article a[href], div.post-card a[href], div.card a[href]'):
        title = clean_text(a.get_text())
        href = a.get('href')
        if not title or not href:
            continue
        job = build_job('adda', source_name, base_url, title, href)
        if job:
            jobs.append(job)
    return jobs

# --------------- Fetch / Save / Main ----------------

def fetch_and_parse(source):
    parser_func = globals().get(source["parser"])
    if not parser_func:
        logging.error(f"Missing parser: {source['parser']}")
        return []
    try:
        logging.info(f"Fetching {source['name']} -> {source['url']}")
        r = requests.get(source["url"], headers=HEADERS, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        jobs = parser_func(r.content, source["name"], source["url"])
        logging.info(f"{source['name']}: {len(jobs)} jobs parsed.")
        return jobs
    except requests.RequestException as e:
        logging.error(f"{source['name']} request failed: {e}")
        return []
    except Exception as e:
        logging.error(f"{source['name']} parsing error: {e}")
        return []

def enforce_schema_defaults(jobs):
    # Ensure required keys exist for every item
    for j in jobs:
        j.setdefault("slug", derive_slug(j.get("title"), j.get("applyLink")))
        j.setdefault("qualificationLevel", "N/A")
        j.setdefault("domicile", "All India")
        j.setdefault("source", "N/A")
        j.setdefault("type", "General")
        j.setdefault("extractedAt", now_iso())
    return jobs

def save_outputs(jobs, sources_used):
    jobs = enforce_schema_defaults(jobs)
    output_data = {
        "lastUpdated": now_iso(),
        "totalJobs": len(jobs),
        "jobListings": jobs,
        # Required by your schema
        "transparencyInfo": {
            "notes": "General vacancies only (10th/12th/Any graduate). Excludes teacher roles and technical/management/postgraduate requirements. Allowed skills: typing/computer operations/physical.",
            "sourcesTried": sources_used if isinstance(sources_used, list) else [sources_used],
            "schemaVersion": "1.0"
        }
    }
    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=4, ensure_ascii=False)
    logging.info(f"Saved {len(jobs)} jobs to data.json")

    health = {
        "ok": bool(jobs),
        "lastChecked": now_iso(),
        "totalActive": len(jobs),
        "sourceUsed": sources_used if not isinstance(sources_used, list) else (sources_used[0] if sources_used else "None")
    }
    with open("health.json", "w", encoding="utf-8") as f:
        json.dump(health, f, indent=4)

if __name__ == "__main__":
    collected = []
    used = []
    if FIRST_SUCCESS_MODE:
        for src in SOURCES:
            jobs = fetch_and_parse(src)
            if jobs:
                collected = jobs
                used = [src["name"]]
                break
            time.sleep(REQUEST_SLEEP_SECONDS)
    else:
        for src in SOURCES:
            jobs = fetch_and_parse(src)
            if jobs:
                collected.extend(jobs)
                used.append(src["name"])
            time.sleep(REQUEST_SLEEP_SECONDS)

    if not collected:
        logging.error("No data collected from any source.")
    save_outputs(collected, used or ["None"])
