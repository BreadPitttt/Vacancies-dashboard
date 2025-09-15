# scraper.py — General-competition vacancies with SSC/Bank/Railway/Insurance + domicile and skill rules

import json, re
from pathlib import Path
from datetime import datetime, timedelta
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
import feedparser
import dateparser

DATA_PATH = Path("data.json")
UTC_NOW = datetime.utcnow()

SOURCES = [
    # SSC (new portal); include ER region as an example
    {"name": "SSC", "type": "html", "base": "https://ssc.gov.in", "url": "https://ssc.gov.in", "domicile": "All India", "source": "official"},
    {"name": "SSC-ER", "type": "html", "base": "https://sscer.org", "url": "https://sscer.org", "domicile": "All India", "source": "official"},

    # Banks via IBPS
    {"name": "IBPS", "type": "html", "base": "https://www.ibps.in", "url": "https://www.ibps.in", "domicile": "All India", "source": "official"},

    # Railways via RRB portals
    {"name": "RRB-CDG", "type": "html", "base": "https://www.rrbcdg.gov.in", "url": "https://www.rrbcdg.gov.in", "domicile": "All India", "source": "official"},
    {"name": "RRB-Patna", "type": "html", "base": "https://www.rrbpatna.gov.in", "url": "https://www.rrbpatna.gov.in", "domicile": "All India", "source": "official"},

    # Bihar PSC (Bihar-only acceptable)
    {"name": "BPSC", "type": "html", "base": "https://bpsc.bihar.gov.in", "url": "https://bpsc.bihar.gov.in/whats-new/", "domicile": "Bihar", "source": "official"},

    # Insurance careers
    {"name": "LIC", "type": "html", "base": "https://licindia.in", "url": "https://licindia.in/careers", "domicile": "All India", "source": "official"},
    {"name": "NIACL", "type": "html", "base": "https://www.newindia.co.in", "url": "https://www.newindia.co.in/recruitment", "domicile": "All India", "source": "official"},
    {"name": "UIIC", "type": "html", "base": "https://www.uiic.co.in", "url": "https://www.uiic.co.in/web/careers/recruitment", "domicile": "All India", "source": "official"},

    # Aggregator fallbacks
    {"name": "CareerPower", "type": "html", "base": "https://www.adda247.com", "url": "https://www.adda247.com/jobs/government-jobs/", "domicile": "All India", "source": "aggregator"},
    {"name": "SarkariExam", "type": "html", "base": "https://www.sarkariexam.com", "url": "https://www.sarkariexam.com", "domicile": "All India", "source": "aggregator"},
    {"name": "RojgarResult", "type": "html", "base": "https://www.rojgarresult.com", "url": "https://www.rojgarresult.com", "domicile": "All India", "source": "aggregator"},
    {"name": "SarkariResult", "type": "html", "base": "https://sarkariresult.com.cm", "url": "https://sarkariresult.com.cm/latest-jobs/", "domicile": "All India", "source": "aggregator"},
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
    r"\b(b\.?tech|be\b|m\.?tech|engineering)\b", r"\bmba|pgdm|management\b", r"\blaw\b|\bllb\b|\ballm\b",
    r"\bnursing\b|\bgnm\b|\banm\b|\bpharma|bpharm|mpharm\b", r"\bmca\b|\bbca\b|\bcomputer\s+science\b|\bit\b",
    r"\b(b\.?ed|bed)\b", r"\b(diploma|iti)\b", r"\bca\b|\bcs\b|\bcma\b|\bicwa\b", r"\bmedical|mbbs|bds|ayush|veterinary\b",
]
EXCLUDE_NON_RECRUITMENT = [r"\badmit\s*card\b", r"\bresult\b", r"\banswer\s*key\b", r"\bexam\s*date\b", r"\bsyllabus\b"]
CORRIGENDUM_TAGS = [r"\bcorrigendum\b", r"\baddendum\b", r"\bnotice\b", r"\bupdate\b"]
DATE_HINTS = [
    r"last\s*date[:\-\s]*([^\n<]{6,30})",
    r"closing\s*date[:\-\s]*([^\n<]{6,30})",
    r"apply\s*online\s*last\s*date[:\-\s]*([^\n<]{6,30})",
    r"last\s*date\s*to\s*apply[:\-\s]*([^\n<]{6,30})",
]
HEADERS = {"User-Agent": "Mozilla/5.0 (VacancyBot)", "Accept-Language": "en-IN,en;q=0.9", "Cache-Control": "no-cache"}

INDIAN_STATES = [
    "andhra pradesh","arunachal pradesh","assam","bihar","chhattisgarh","goa","gujarat","haryana","himachal pradesh",
    "jharkhand","karnataka","kerala","madhya pradesh","maharashtra","manipur","meghalaya","mizoram","nagaland","odisha",
    "punjab","rajasthan","sikkim","tamil nadu","telangana","tripura","uttar pradesh","uttarakhand","west bengal",
]
ALLOW_OUTSIDE_PATTERNS = [r"\bany\s+state\b", r"\ball\s+india\b", r"\bfrom\s+any\s+state\b", r"\bdomicile\s+not\s+required\b", r"\bopen\s+to\s+all\b"]
RESTRICT_PAT = r"\b(domici(?:le|liary)|resident)\b.*?\b(only|required)\b"

def norm(s): return re.sub(r"\s+", " ", (s or "")).strip()
def fetch_html(url): r = requests.get(url, headers=HEADERS, timeout=40); r.raise_for_status(); return r.content
def absolute(base, href): return href if (href and href.startswith("http")) else (urljoin(base, href) if href else None)

def clean_title_for_id(title):
    t = norm(title).lower()
    for pat in CORRIGENDUM_TAGS: t = re.sub(pat, "", t, flags=re.I)
    return re.sub(r"[^a-z0-9\s]", "", t)

def make_id(org, title, href): return f"{org}:{abs(hash(clean_title_for_id(title)+'|'+(href or '')))%100000000}"

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
    return False  # silent non‑Bihar ⇒ exclude

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

def should_keep(org, title_text, detail_text):
    combo = f"{title_text} — {detail_text}"
    return passes_education(combo) and passes_skill_rule(combo) and passes_degree_exclusions(combo) and is_recruitment(combo) and eligible_by_domicile(org, detail_text)

def add_or_update(listings, item):
    for i, x in enumerate(listings):
        if x["id"] == item["id"]:
            if (item.get("deadline") and not x.get("deadline")) or (item.get("extractedAt","") > x.get("extractedAt","")):
                listings[i] = item
            return
    listings.append(item)

def scrape_html_list(list_url, base, org, domicile):
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
            domicile_label = "Bihar" if (("bpsc" in org.lower()) or ("bihar" in detail_text.lower())) else "All India"
            item = {
                "id": make_id(org, title, href),
                "title": title,
                "organization": org,
                "qualificationLevel": classify_level(f"{title} {detail_text}"),
                "domicile": domicile_label,
                "source": "official" if org not in {"CareerPower","SarkariExam","RojgarResult","SarkariResult"} else "aggregator",
                "deadline": extract_deadline(detail_text),
                "applyLink": href,
                "pdfLink": href,
                "extractedAt": UTC_NOW.strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
            add_or_update(items, item)
    except Exception as e:
        print(f"[WARN] {org} scrape error: {e}")
    return items

def scrape_rss(url, org, domicile):
    items = []
    try:
        feed = feedparser.parse(url)
        for e in feed.entries[:30]:
            title = norm(e.get("title","")); link = e.get("link","")
            if not title or not link: continue
            try:
                detail = fetch_html(link)
                detail_text = norm(BeautifulSoup(detail, "html.parser").get_text(" "))
            except Exception:
                detail_text = title
            if not should_keep(org, title, detail_text): continue
            domicile_label = "Bihar" if "bpsc" in org.lower() else "All India"
            item = {
                "id": make_id(org, title, link),
                "title": title,
                "organization": org,
                "qualificationLevel": classify_level(f"{title} {detail_text}"),
                "domicile": domicile_label,
                "source": "official",
                "deadline": extract_deadline(detail_text),
                "applyLink": link,
                "pdfLink": link,
                "extractedAt": UTC_NOW.strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
            add_or_update(items, item)
    except Exception as e:
        print(f"[WARN] {org} RSS error: {e}")
    return items

def scrape_all():
    collected = []
    for s in SOURCES:
        print(f"Scraping: {s['name']}")
        if s["type"] == "html":
            collected.extend(scrape_html_list(s["url"], s["base"], s["name"], s["domicile"]))
        elif s["type"] == "rss":
            collected.extend(scrape_rss(s["url"], s["name"], s["domicile"]))
    return collected

def drop_expired(listings):
    out = []
    for j in listings:
        dl = j.get("deadline")
        if dl:
            try: d = datetime.fromisoformat(dl)
            except Exception:
                dt = dateparser.parse(dl); d = dt if isinstance(dt, datetime) else None
            if d and d.date() < UTC_NOW.date(): continue
        if j.get("extractedAt"):
            try:
                ext = datetime.fromisoformat(j["extractedAt"].replace("Z",""))
                if ext < UTC_NOW - timedelta(days=120): continue
            except Exception: pass
        out.append(j)
    return out

def load_data():
    base = {"jobListings": [], "transparencyInfo": {}}
    if DATA_PATH.exists():
        try: base = json.loads(DATA_PATH.read_text(encoding="utf-8"))
        except Exception: pass
    base.setdefault("jobListings", []); base.setdefault("transparencyInfo", {})
    return base

def save_data(data): DATA_PATH.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")

def main():
    data = load_data()
    fresh = scrape_all()
    by_id = {j["id"]: j for j in data["jobListings"]}
    for x in fresh: by_id[x["id"]] = x
    merged = drop_expired(list(by_id.values()))
    data["jobListings"] = merged
    data["transparencyInfo"]["lastUpdated"] = UTC_NOW.strftime("%Y-%m-%dT%H:%M:%SZ")
    data["transparencyInfo"]["totalListings"] = len(merged)
    save_data(data)
    print(f"Saved {len(merged)} listings")

if __name__ == "__main__":
    main()
