# scraper.py — Official-first with aggregator fallback, corrigendum/extension handling, de-dup, expiry
# Filters: Any Graduate / 12th / 10th only; allow no skill or only typing/computer/PET-PST/physical;
# Domicile: All India and Bihar (Bihar-only OK); other states only if outsiders explicitly allowed.

import json, re
from pathlib import Path
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
import feedparser
import dateparser

DATA_PATH = Path("data.json")
UTC_NOW = datetime.now(timezone.utc)

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
ALLOWED_SKILLS = [r"\btyping\b", r"\bcomputer(?!\s*science)\b", r"\bpet\b", r"\bpst\b", r"\bphysical\b", r"\bms\s*office\b", r"\bccc\b", r"\bdca\b"]
DISALLOWED_SKILLS = [r"\b(programming|coding|java|python|autocad|cad|sap|oracle|network|hardware|software|tally|marketing|sales|management)\b", r"\bcertificate|licen[cs]e|diploma\b"]
EXCLUDE_DEGREE = [
    r"\b(b\.?tech|be\b|m\.?tech|engineering)\b", r"\bmba|pgdm|management\b",
    r"\blaw\b|\bllb\b|\ballm\b", r"\bnursing\b|\bgnm\b|\banm\b|\bpharma|bpharm|mpharm\b",
    r"\bmca\b|\bbca\b|\bcomputer\s+science\b|\bit\b", r"\b(b\.?ed|bed)\b", r"\b(diploma|iti)\b",
    r"\bca\b|\bcs\b|\bcma\b|\bicwa\b", r"\bmedical|mbbs|bds|ayush|veterinary\b",
]
EXCLUDE_NON_RECRUITMENT = [r"\badmit\s*card\b", r"\bresult\b", r"\banswer\s*key\b", r"\bexam\s*date\b", r"\bsyllabus\b"]

INDIAN_STATES = [
    "andhra pradesh","arunachal pradesh","assam","bihar","chhattisgarh","goa","gujarat","haryana","himachal pradesh",
    "jharkhand","karnataka","kerala","madhya pradesh","maharashtra","manipur","meghalaya","mizoram","nagaland","odisha",
    "punjab","rajasthan","sikkim","tamil nadu","telangana","tripura","uttar pradesh","uttarakhand","west bengal",
]
ALLOW_OUTSIDE_PATTERNS = [r"\bany\s+state\b", r"\ball\s+india\b", r"\bfrom\s+any\s+state\b", r"\bdomicile\s+not\s+required\b", r"\bopen\s+to\s+all\b"]
RESTRICT_PAT = r"\b(domici(?:le|liary)|resident)\b.*?\b(only|required)\b"

DATE_HINTS = [
    r"last\s*date[:\-\s]*([^\n<]{6,30})",
    r"closing\s*date[:\-\s]*([^\n<]{6,30})",
    r"apply\s*online\s*last\s*date[:\-\s]*([^\n<]{6,30})",
    r"last\s*date\s*to\s*apply[:\-\s]*([^\n<]{6,30})",
]
EXTENSION_HINTS = [
    r"\bextension\b", r"\bextended\s+to\b", r"\blast\s*date\s*(?:extended|revised)\s*to\b", r"\bdate\s*extended\b"
]

HEADERS = {"User-Agent": "Mozilla/5.0 (VacancyBot)", "Accept-Language": "en-IN,en;q=0.9", "Cache-Control": "no-cache"}

def norm(s): return re.sub(r"\s+", " ", (s or "")).strip()
def absolute(base, href): return href if (href and href.startswith("http")) else (urljoin(base, href) if href else None)

def fetch_html(url):
    r = requests.get(url, headers=HEADERS, timeout=40)
    r.raise_for_status()
    return r.content

def head_ok(url):
    try:
        r = requests.head(url, headers=HEADERS, timeout=20, allow_redirects=True)
        if r.status_code in (405, 501):
            r = requests.get(url, headers=HEADERS, timeout=20, stream=True)
        return 200 <= r.status_code < 300
    except Exception:
        return False

def clean_title_for_id(title):
    t = norm(title).lower()
    t = re.sub(r"\b(corrigendum|addendum|notice|update|extension|date\s*extended)\b", "", t, flags=re.I)
    t = re.sub(r"[\(\)\[\]\-:|,]", " ", t)
    t = re.sub(r"\b(recruitment|notification|advertisement|online\s*form|apply\s*online)\b", "", t)
    return re.sub(r"[^a-z0-9\s]", "", t).strip()

def make_id(org, slug): return f"{org}:{abs(hash(org.lower()+'|'+slug))%100000000}"
def passes_education(text): return any(re.search(p, (text or "").lower()) for p in INCLUDE_EDU)
def passes_skill_rule(text):
    t = (text or "").lower()
    if not re.search(r"\b(skill|certificate|course|experience|typing|computer|pet|pst|physical)\b", t): return True
    if any(re.search(p, t) for p in DISALLOWED_SKILLS): return False
    return any(re.search(p, t) for p in ALLOWED_SKILLS)
def passes_degree_exclusions(text): return not any(re.search(p, (text or "").lower()) for p in EXCLUDE_DEGREE)
def is_recruitment(text): return not any(re.search(p, (text or "").lower()) for p in EXCLUDE_NON_RECRUITMENT)
def is_all_india(text): return any(re.search(p, (text or "").lower()) for p in ALLOW_OUTSIDE_PATTERNS)
def is_bihar_only(text): t=(text or "").lower(); return ("bihar" in t) and (re.search(RESTRICT_PAT, t) is not None)
def other_state_only(text):
    t = (text or "").lower()
    for st in INDIAN_STATES:
        if st == "bihar": continue
        if re.search(rf"\b{re.escape(st)}\b.*{RESTRICT_PAT}", t) or re.search(rf"{RESTRICT_PAT}.*\b{re.escape(st)}\b", t): return True
    return False
def eligible_by_domicile(org, detail_text):
    if is_all_india(detail_text): return True
    if ("bpsc" in (org or "").lower()) or is_bihar_only(detail_text): return True
    if other_state_only(detail_text): return False
    return False

def classify_level(text):
    t = (text or "").lower()
    if re.search(r"\b10(th| matric)\b", t): return "10th"
    if re.search(r"\b12(th| intermediate| senior\s+secondary)\b", t): return "12th"
    return "Graduate"

def extract_deadline(text):
    for patt in DATE_HINTS:
        m = re.search(patt, text, flags=re.I)
        if m:
            cand = norm(m.group(1))
            dt = dateparser.parse(cand, settings={"DATE_ORDER": "DMY"})
            if dt: return dt.date().isoformat()
    dt = dateparser.parse(text, settings={"PREFER_DATES_FROM": "future", "DATE_ORDER": "DMY"})
    return dt.date().isoformat() if dt and dt.date() >= UTC_NOW.date() else None

def find_extension_date(text):
    if any(re.search(p, text, flags=re.I) for p in EXTENSION_HINTS):
        dt = dateparser.parse(text, settings={"PREFER_DATES_FROM": "future", "DATE_ORDER": "DMY"})
        if dt: return dt.date().isoformat()
    return None

def should_keep(org, title_text, detail_text):
    combo = f"{title_text} — {detail_text}"
    return passes_education(combo) and passes_skill_rule(combo) and passes_degree_exclusions(combo) and is_recruitment(combo) and eligible_by_domicile(org, detail_text)

def scrape_html_list(list_url, base, org):
    items = []
    try:
        soup = BeautifulSoup(fetch_html(list_url), "html.parser")
        for a in soup.find_all("a", href=True):
            title = norm(a.get_text(" "))
            if len(title) < 8: continue
            href = absolute(base, a["href"])
            if not href: continue

            try:
                detail = fetch_html(href)
                detail_text = norm(BeautifulSoup(detail, "html.parser").get_text(" "))
            except Exception:
                detail_text = title

            if not should_keep(org, title, detail_text): continue

            slug = clean_title_for_id(title)
            domicile_label = "Bihar" if (("bpsc" in org.lower()) or ("bihar" in detail_text.lower())) else "All India"
            deadline = extract_deadline(detail_text)
            extension = find_extension_date(detail_text)
            if extension: deadline = extension

            item = {
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
            item["_link_ok"] = head_ok(item["applyLink"] or item["pdfLink"])
            items.append(item)
    except Exception as e:
        print(f"[WARN] {org} scrape error: {e}")
    return items

def scrape_official_all():
    out = []
    for s in SOURCES_OFFICIAL:
        print("Scraping official:", s["name"])
        out.extend(scrape_html_list(s["url"], s["base"], s["name"]))
    return out

def scrape_aggregators_all():
    out = []
    for s in SOURCES_AGG:
        print("Scraping aggregator:", s["name"])
        items = scrape_html_list(s["url"], s["base"], s["name"])
        for it in items:
            it["source"] = "aggregator"
            if it["domicile"] != "Bihar":
                it["domicile"] = "All India"
        out.extend(items)
    return out

def merge_with_fallback(official_items, aggregator_items):
    by_slug = {}
    for it in official_items:
        by_slug.setdefault(it["slug"], it)

    for agg in aggregator_items:
        slug = agg["slug"]
        if slug not in by_slug:
            by_slug[slug] = agg
            continue
        off = by_slug[slug]
        # Corrigendum/extension wins for deadline updates
        if agg.get("deadline") and (not off.get("deadline")):
            off["deadline"] = agg["deadline"]
        # If official link is unhealthy, swap to aggregator link
        if not off.get("_link_ok"):
            off["applyLink"] = agg.get("applyLink") or off.get("applyLink")
            off["pdfLink"] = agg.get("pdfLink") or off.get("pdfLink")
            off["source"] = "aggregator"

    merged = []
    for it in by_slug.values():
        it.pop("_link_ok", None)
        merged.append(it)
    return merged

def drop_expired(listings):
    out = []
    for j in listings:
        dl = j.get("deadline")
        if dl:
            try: d = datetime.fromisoformat(dl)
            except Exception:
                dt = dateparser.parse(dl); d = dt if isinstance(dt, datetime) else None
            if d and d.date() < UTC_NOW.date():  # expired -> drop
                continue
        if j.get("extractedAt"):
            try:
                ext = datetime
