import requests
from bs4 import BeautifulSoup
import json
import logging
from datetime import datetime
import re
import time
import os
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

RULES_PATH = "rules.json"
REPORTS_PATH = "reports.jsonl"
PRUNE_DAYS = 14
MAX_RULES = 200

# ---------------- Small utilities ----------------
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

# ---------------- Strict eligibility policy ----------------
TEACHER_TERMS = {
    "teacher","tgt","pgt","prt","school teacher","faculty","lecturer",
    "assistant professor","professor","b.ed","bed ","d.el.ed","deled","ctet","tet "
}

def education_band_from_text(text):
    t = (text or "").lower()
    if any(k in t for k in ["10th", "matric", "ssc "]): return "10th pass"
    if any(k in t for k in ["12th", "intermediate", "hsc"]): return "12th pass"
    if "graduate" in t or "bachelor" in t or "any degree" in t: return "Any graduate"
    return "N/A"

def violates_general_policy(text):
    t = (text or "").lower()
    if any(k in t for k in TEACHER_TERMS):
        return True
    disallow = [
        "b.tech","btech","b.e"," be ","m.tech","mtech","m.e","mca","bca",
        "b.sc (engg)","bsc (engg)","engineering degree","m.sc","msc","m.a"," m a ",
        "m.com","mcom","mba","cma","cfa"," ca ","cs ","company secretary",
        "pg ","post graduate","postgraduate","phd","m.phil","mphil",
        "engineer","developer","scientist","specialist","analyst",
        "technical manager","architect","research","research associate",
        "data scientist","ml engineer","cloud engineer","sde","devops"
    ]
    return any(k in t for k in disallow)

def allowed_basic_skill(text):
    t = (text or "").lower()
    return any(k in t for k in ["typing", "computer", "physical", "field duty"])

def derive_post(text):
    t = (text or "").lower()
    for key in [
        "constable","clerk","apprentice","technician","je","ae",
        "officer","assistant","mts","multi tasking","data entry",
        "operator","guard","peon","si","jlo","nursing officer"
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

# ---------------- Domicile policy (precise) ----------------
STATE_NAMES = [
    "andhra pradesh","arunachal pradesh","assam","bihar","chhattisgarh","goa","gujarat","haryana",
    "himachal pradesh","jharkhand","karnataka","kerala","madhya pradesh","maharashtra","manipur",
    "meghalaya","mizoram","nagaland","odisha","punjab","rajasthan","sikkim","tamil nadu",
    "telangana","tripura","uttar pradesh","uttarakhand","west bengal","jammu","kashmir","ladakh",
    "delhi","puducherry","chandigarh","andaman","nicobar","dadra","nagar haveli","daman","diu",
    "lakshadweep"
]
OPEN_SIGNALS = ["all india","any state","open to all","pan india","indian nationals","across india","from any state"]
CLOSE_SIGNALS = ["domicile","resident","locals only","local candidates","state quota","only"]

def domicile_allow(title):
    t = (title or "").lower()
    if any(k in t for k in OPEN_SIGNALS):
        return True
    if "bihar" in t and any(k in t for k in CLOSE_SIGNALS):
        return True
    for st in STATE_NAMES:
        if st == "bihar":
            continue
        if st in t and any(k in t for k in CLOSE_SIGNALS):
            return False
    return True

# ---------------- Rules + Feedback adapter ----------------
def load_rules():
    seed = {
        "exclusions": {
            "titleKeywords": ["teacher","tgt","pgt","prt","b.ed","ctet","tet"],
            "skillKeywords": ["advanced analytics","cloud architecture","machine learning","cad"],
            "scoredTitle": [
                {"token":"engineer","score":0.9,"hits":0,"falsePos":0,"lastSeen":""},
                {"token":"developer","score":0.9,"hits":0,"falsePos":0,"lastSeen":""},
                {"token":"scientist","score":0.9,"hits":0,"falsePos":0,"lastSeen":""},
                {"token":"analyst","score":0.85,"hits":0,"falsePos":0,"lastSeen":""},
                {"token":"mba","score":0.95,"hits":0,"falsePos":0,"lastSeen":""},
                {"token":"m.tech","score":0.95,"hits":0,"falsePos":0,"lastSeen":""},
                {"token":"mca","score":0.95,"hits":0,"falsePos":0,"lastSeen":""},
                {"token":"b.tech","score":0.95,"hits":0,"falsePos":0,"lastSeen":""},
                {"token":"b.e","score":0.95,"hits":0,"falsePos":0,"lastSeen":""},
                {"token":"phd","score":0.98,"hits":0,"falsePos":0,"lastSeen":""},
                {"token":"postgraduate","score":0.9,"hits":0,"falsePos":0,"lastSeen":""},
                {"token":"research","score":0.8,"hits":0,"falsePos":0,"lastSeen":""}
            ],
            "domicileCloseSignals": CLOSE_SIGNALS
        },
        "inclusions": {
            "educationKeywords": ["10th","matric","ssc","12th","intermediate","hsc","graduate","any degree","bachelor"],
            "skillKeywords": ["typing","computer","physical","field duty"],
            "domicileOpenSignals": OPEN_SIGNALS
        },
        "siteHints": {
            "www.adda247.com": {"excludeSections": ["sarkari result","admit card","answer key"]},
            "www.resultbharat.com": {"excludeSections": ["result","admit card","answer key"]},
            "sarkarijobfind.com": {"excludeSections": ["result","admit card","answer key"]},
            "www.freejobalert.com": {"excludeSections": ["admit card","result","answer key","syllabus"]}
        },
        "domicile": {
            "states": STATE_NAMES,
            "biharAllowed": True
        },
        "metadata": {
            "updatedAt": now_iso(),
            "changelog": [],
            "prune": {"days": 14, "maxRules": 220, "dailyDecay": 0.98, "promoteAt": 0.70, "demoteBelow": 0.30}
        }
    }
    if not os.path.exists(RULES_PATH):
        return seed
    try:
        with open(RULES_PATH, "r", encoding="utf-8") as f:
            data = json.load(f) or {}
            for k in seed:
                if k not in data:
                    data[k] = seed[k]
            return data
    except Exception:
        return seed

def save_rules(rules):
    rules["metadata"]["updatedAt"] = now_iso()
    for bucket in ["exclusions","inclusions"]:
        for key in ["titleKeywords","skillKeywords"]:
            if key in rules.get(bucket, {}):
                rules[bucket][key] = rules[bucket][key][:MAX_RULES]
    with open(RULES_PATH, "w", encoding="utf-8") as f:
        json.dump(rules, f, indent=2, ensure_ascii=False)

def feedback_adapter(rules):
    if not os.path.exists(REPORTS_PATH):
        return rules
    cutoff_ts = datetime.now().timestamp() - PRUNE_DAYS*86400
    added = 0
    try:
        with open(REPORTS_PATH, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except Exception:
                    continue
                flag = (rec.get("flag") or rec.get("type") or "").lower()
                if flag not in ["report","not general vacancy","wrong eligibility","right"]:
                    continue
                ts = rec.get("ts") or rec.get("timestamp") or ""
                try:
                    tsv = datetime.fromisoformat(ts).timestamp() if isinstance(ts, str) else float(ts or 0)
                except Exception:
                    tsv = 0
                if tsv and tsv < cutoff_ts:
                    continue
                title = (rec.get("title") or rec.get("note") or rec.get("url") or "").lower()
                if not title:
                    continue
                tokens = [it.get("token") for it in rules["exclusions"]["scoredTitle"] if it.get("token")]
                for tok in tokens:
                    if tok in title:
                        update_scored_rules_from_feedback(tok, positive=(flag in ["report","not general vacancy","wrong eligibility"]))
                        added += 1
    except Exception:
        pass
    if added:
        save_rules(rules)
    return rules

RULES = load_rules()

def scored_match_excluded(title):
    t = (title or "").lower()
    scored = RULES.get("exclusions", {}).get("scoredTitle", [])
    total_strength = 0.0
    for it in scored:
        tok = it.get("token","")
        sc = float(it.get("score",0))
        if tok and tok in t:
            total_strength += sc
            if sc >= 0.9:
                return True, "high_conf_rule"
    if total_strength >= 1.2:
        return True, "combined_rules"
    return False, ""

def update_scored_rules_from_feedback(token, positive=True):
    scored = RULES.get("exclusions", {}).get("scoredTitle", [])
    for it in scored:
        if it.get("token") == token:
            if positive:
                it["score"] = min(0.99, round(float(it.get("score",0)) * 1.02 + 0.02, 4))
                it["hits"] = int(it.get("hits",0)) + 1
            else:
                it["score"] = max(0.01, round(float(it.get("score",0)) * 0.97 - 0.02, 4))
                it["falsePos"] = int(it.get("falsePos",0)) + 1
            it["lastSeen"] = now_iso()
            break
    decay = float(RULES.get("metadata",{}).get("prune",{}).get("dailyDecay", 0.98))
    for it in scored:
        it["score"] = max(0.01, round(float(it.get("score",0))*decay, 4))
    promote = float(RULES.get("metadata",{}).get("prune",{}).get("promoteAt", 0.70))
    RULES["exclusions"]["titleKeywords"] = list({*RULES["exclusions"]["titleKeywords"]})
    for it in scored:
        if it["score"] >= promote and it["token"] not in RULES["exclusions"]["titleKeywords"]:
            RULES["exclusions"]["titleKeywords"].append(it["token"])

RULES = feedback_adapter(RULES)

def title_hits_excluded(title):
    t = (title or "").lower()
    for k in RULES["exclusions"]["titleKeywords"]:
        if k in t:
            return True, f"Excluded by rule: {k}"
    return False, ""

def site_section_excluded(host, title):
    hint = RULES.get("siteHints", {}).get(host, {})
    if not hint:
        return False
    t = (title or "").lower()
    for w in hint.get("excludeSections", []):
        if w in t:
            return True
    return False

# ---------------- Job object builder ----------------
def build_job(prefix, source_name, base_url, title, href, deadline="N/A"):
    title = clean_text(title)
    href_abs = normalize_url(base_url, href)

    hit, _ = title_hits_excluded(title)
    if hit:
        return None
    hit_scored, _w = scored_match_excluded(title)
    if hit_scored:
        return None
    if violates_general_policy(title):
        return None
    if not domicile_allow(title):
        return None

    education = education_band_from_text(title)
    job = {
        "id": make_id(prefix, href_abs),
        "title": title or "N/A",
        "organization": derive_org(title),
        "deadline": deadline or "N/A",
        "applyLink": href_abs or base_url,
        "slug": derive_slug(title, href_abs),
        "qualificationLevel": education if education in ["10th pass", "12th pass", "Any graduate"] else "N/A",
        "domicile": "All India",
        "source": "aggregator",          # schema enum
        "type": "VACANCY",               # schema enum
        "extractedAt": now_iso(),
        "meta": {
            "post": derive_post(title),
            "allowedSkills": [k for k in ["Typing","Computer operations","Physical"] if allowed_basic_skill(title)],
            "sourceUrl": base_url,
            "sourceSite": source_name     # keep original site here
        }
    }
    return job

# ---------------- Parsers ----------------
def parse_freejobalert(content, source_name, base_url):
    soup = BeautifulSoup(content, 'html.parser')
    jobs = []
    host = urlparse(base_url).netloc

    def looks_job(t):
        tl = (t or "").lower()
        if any(x in tl for x in ["admit card","result","answer key","syllabus"]):
            return False
        return any(x in tl for x in ["recruit","vacancy","notification","apply online"])

    for tbl in soup.select('table'):
        for a in tbl.select('a[href]'):
            title = clean_text(a.get_text())
            if not looks_job(title):
                continue
            href = a.get('href')
            job = build_job('fja', source_name, base_url, title, href)
            if job and not site_section_excluded(host, title):
                jobs.append(job)

    if not jobs:
        for a in soup.select('main a[href], .entry-content a[href], a[href]'):
            title = clean_text(a.get_text())
            if not looks_job(title):
                continue
            href = a.get('href')
            job = build_job('fja', source_name, base_url, title, href)
            if job and not site_section_excluded(host, title):
                jobs.append(job)
    return jobs

def parse_sarkarijobfind(content, source_name, base_url):
    soup = BeautifulSoup(content, 'html.parser')
    jobs = []
    host = urlparse(base_url).netloc

    for h in soup.find_all(re.compile('^h[1-6]$')):
        if re.search(r'new\s*update', h.get_text(), re.I):
            ul = h.find_next_sibling(['ul','ol'])
            if not ul:
                continue
            for li in ul.select('li'):
                a = li.find('a', href=True)
                if not a:
                    continue
                title = clean_text(a.get_text())
                if site_section_excluded(host, title):
                    continue
                href = a['href']
                job = build_job('sjf', source_name, base_url, title, href)
                if job:
                    jobs.append(job)
            break
    return jobs

def parse_resultbharat(content, source_name, base_url):
    soup = BeautifulSoup(content, 'html.parser')
    jobs = []
    host = urlparse(base_url).netloc

    for table in soup.select('table'):
        headers = [clean_text(th.get_text()) for th in table.select('th')]
        if not headers:
            continue
        col_idx = -1
        for i, h in enumerate(headers):
            if re.search(r'latest\s*jobs', h, re.I):
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
                if site_section_excluded(host, title):
                    continue
                href = a['href']
                job = build_job('rb', source_name, base_url, title, href)
                if job:
                    jobs.append(job)
    return jobs

def parse_adda247(content, source_name, base_url):
    soup = BeautifulSoup(content, 'html.parser')
    jobs = []
    host = urlparse(base_url).netloc

    for a in soup.select('article a[href], .post-card a[href], .card a[href]'):
        title = clean_text(a.get_text())
        href = a.get('href')
        if not title or not href:
            continue
        if site_section_excluded(host, title):
            continue
        job = build_job('adda', source_name, base_url, title, href)
        if job:
            jobs.append(job)
    return jobs

# ---------------- Fetch / Save / Main ----------------
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
    for j in jobs:
        j.setdefault("slug", derive_slug(j.get("title"), j.get("applyLink")))
        j.setdefault("qualificationLevel", "N/A")
        j.setdefault("domicile", "All India")
        j.setdefault("source", "aggregator")
        j.setdefault("type", "VACANCY")
        j.setdefault("extractedAt", now_iso())
    return jobs

def save_outputs(jobs, sources_used):
    jobs = enforce_schema_defaults(jobs)
    output_data = {
        "lastUpdated": now_iso(),
        "totalJobs": len(jobs),
        "jobListings": jobs,
        "transparencyInfo": {
            "notes": "General vacancies (10th/12th/Any graduate). Excludes teacher and technical/management/PG roles. Domicile: All‑India and Bihar‑only allowed; other states allowed only if title signals open to any state.",
            "sourcesTried": sources_used if isinstance(sources_used, list) else [sources_used],
            "schemaVersion": "1.2",
            "totalListings": len(jobs),          # required by schema
            "lastUpdated": now_iso()             # required by schema
        }
    }
    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=4, ensure_ascii=False)

    health = {
        "ok": bool(jobs),
        "lastChecked": now_iso(),
        "totalActive": len(jobs),
        "sourceUsed": sources_used if not isinstance(sources_used, list) else (sources_used[0] if sources_used else "None"),
        "rulesUpdatedAt": RULES.get("metadata", {}).get("updatedAt", "N/A")
    }
    with open("health.json", "w", encoding="utf-8") as f:
        json.dump(health, f, indent=4)

# ---------------- Entrypoint ----------------
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
        logging.warning("No data collected from any source. Check selectors for the last changed site or relax that parser only.")

    save_outputs(collected, used or ["None"])
