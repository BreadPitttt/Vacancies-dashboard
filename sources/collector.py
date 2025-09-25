#!/usr/bin/env python3
# Hybrid collector v3.1 — posts extraction from title + nearby text + PDF filenames (official-first)
import requests, json, sys, re, time, os, hashlib, pathlib
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

UA = {"User-Agent":"Mozilla/5.0"}
RULES = {}
try:
    RULES = json.loads(pathlib.Path("rules.json").read_text(encoding="utf-8"))
except Exception:
    RULES = {}
AGG_SCORES = RULES.get("aggregatorScores", {})

OFFICIAL_SITES = [
    ("https://ssc.gov.in/", "a[href]", "SSC", "All India"),
    ("https://bssc.bihar.gov.in/", "#NoticeBoard a[href], a[href*='Advt'], a[href*='advert']", "BSSC", "Bihar"),
    ("https://bpsc.bihar.gov.in/", "a[href]", "BPSC", "Bihar"),
    ("https://www.ibps.in/", "a[href]", "IBPS", "All India"),
    ("https://opportunities.rbi.org.in/Scripts/Vacancies.aspx", "a[href]", "RBI", "All India"),
    ("https://www.rrbapply.gov.in/#/auth/landing/", "a[href]", "RRB", "All India"),
    ("https://nests.tribal.gov.in/show_content.php?lang=1&level=1&ls_id=949&lid=550", "a[href]", "EMRS/NESTS", "All India"),
    ("https://examinationservices.nic.in/recSys2025/root/Home.aspx?enc=Ei4cajBkK1gZSfgr53ImFZ5JDNNIP7I8JbNwGOl976uPeIvr9X7G7iVESmo7y1L6", "a[href]", "EMRS/NESTS", "All India"),
]

AGGREGATORS = [
    ("https://www.freejobalert.com/", "a[href]"),
    ("https://sarkarijobfind.com/", "a[href]"),
    ("https://www.resultbharat.com/", "a[href]"),
    ("https://www.rojgarresult.com/", "a[href]"),
    ("https://www.adda247.com/jobs/", "a[href]"),
]

NEG_TOK = re.compile(r"\b(result|cutoff|exam\s*date|admit\s*card|syllabus|answer\s*key)\b", re.I)
ALLOW_UPDATE = re.compile(r"\b(corrigendum|extension|extended|addendum|amendment|revised|rectified|last\s*date)\b", re.I)

ALLOW_EDU = re.compile(r"(10th|matric|ssc\b|12th|intermediate|hsc|any\s+graduate|graduate\b)", re.I)
DISALLOW_STREAM = re.compile(r"(teacher|tgt|pgt|prt|b\.?ed|ctet|tet|b\.?tech|m\.?tech|b\.e|m\.e|mca|bca|engineer|developer|scientist|architect|analyst|nursing|pharma|iti|polytechnic|diploma|mba|msc|m\.sc|phd|post\s*graduate)", re.I)
SKILL_BAD = re.compile(r"(steno|shorthand|trade\s+test|cad|sap|oracle|aws|azure|docker|kubernetes|tally\s*erp)", re.I)

DATE_PAT = re.compile(r"(\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b)", re.I)
LAST_DATE_PAT = re.compile(r"(last\s*date|apply\s*by|closing\s*date)", re.I)

# Improved posts patterns
POSTS_PAT = re.compile(r"(\d{1,6})\s*(posts?|vacanc(?:y|ies)|seats?)", re.I)
POSTS_HINT_NEAR = re.compile(r"(total|overall|aggregate)\s*(\d{1,6})\s*(posts?|vacanc(?:y|ies)|seats?)", re.I)

CONSTABLE_HINT = re.compile(r"\b(constable|gd|chsl|12th)\b", re.I)
TEN_HINT = re.compile(r"\b(10th|matric|ssc\s*exam|mts)\b", re.I)
GRAD_HINT = re.compile(r"\b(graduat|cgl|degree|bachelor)\b", re.I)

def clean(s): return re.sub(r"\s+"," ", (s or "").strip())
def host(u):
    try: return urlparse(u or "").netloc.lower()
    except: return ""

OFFICIAL_SUFFIXES = (".gov.in",".nic.in",".gov",".go.in")
ALLOW_HOSTS = set([
    "rbi.org.in","ibps.in","bpsc.bihar.gov.in","bssc.bihar.gov.in","ssc.gov.in",
    "rrbapply.gov.in","nests.tribal.gov.in","examinationservices.nic.in"
])

def is_official(url):
    try:
        h = host(url)
        if not h: return False
        if h in ALLOW_HOSTS: return True
        if any(h.endswith(sfx) for sfx in OFFICIAL_SUFFIXES): return True
        return False
    except:
        return False

BAD_OFFICIAL_HINTS = re.compile(r"(sarkari|adda247|careerpower|testbook|rojgar|freejobalert|shiksha|blog|news)", re.I)

def is_strict_official_link(base_site, url):
    h = host(url)
    if not is_official(url): return False
    if BAD_OFFICIAL_HINTS.search(url): return False
    base_h = host(base_site)
    if base_h and (h==base_h or h in ALLOW_HOSTS or any(h.endswith(s) for s in OFFICIAL_SUFFIXES)):
        return True
    return False

def eligible_title(title):
    t = clean(title)
    if NEG_TOK.search(t) and not ALLOW_UPDATE.search(t): return False
    if not ALLOW_EDU.search(t): return False
    if DISALLOW_STREAM.search(t): return False
    if SKILL_BAD.search(t): return False
    return True

def infer_qualification(title):
    t = title.lower()
    if CONSTABLE_HINT.search(t): return "12th pass"
    if TEN_HINT.search(t): return "10th pass"
    if GRAD_HINT.search(t): return "Any graduate"
    if re.search(r"\b12(th)?\b|intermediate|hsc", t): return "12th pass"
    if re.search(r"\b10(th)?\b|matric|ssc\b", t): return "10th pass"
    if re.search(r"\bgraduate\b|any\s+degree|bachelor", t): return "Any graduate"
    return "Any graduate"

def extract_last_date(title):
    if LAST_DATE_PAT.search(title):
        m = DATE_PAT.search(title)
        if m: return m.group(1).replace("-", "/")
    m = DATE_PAT.search(title)
    return m.group(1).replace("-", "/") if m else "N/A"

def posts_from_text(txt):
    if not txt: return None
    m = POSTS_PAT.search(txt)
    if m:
        try: return int(m.group(1))
        except: return None
    m2 = POSTS_HINT_NEAR.search(txt)
    if m2:
        try: return int(m2.group(2))
        except: return None
    return None

def posts_from_pdf_filename(url):
    # e.g., .../CEN_03_2025_1206_Posts.pdf or .../Notice_420_Vacancies.pdf
    try:
        fn=urlparse(url).path.rsplit("/",1)[-1]
        return posts_from_text(fn.replace("_"," ").replace("-"," "))
    except:
        return None

def mk(title, url, org, dom, source="official"):
    title = clean(title)
    q = infer_qualification(title)
    last = extract_last_date(title)
    rec = {
        "title": title,
        "organization": "",  # keep blank; UI will not show it
        "qualificationLevel": q,
        "domicile": dom or ("All India" if is_official(url) else "All India"),
        "deadline": last,
        "applyLink": url,
        "detailLink": url,
        "source": source,
        "type": "VACANCY",
        "flags": { "trusted": source=="official" }
    }
    p = posts_from_text(title) or posts_from_pdf_filename(url)
    if p: rec["numberOfPosts"]=p
    return rec

def fetch(base, selector):
    try:
        r = requests.get(base, timeout=30, headers=UA)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        out=[]
        for a in soup.select(selector):
            t = a.get_text(" ", strip=True); h=a.get("href","")
            if not t or not h: continue
            if not re.search(r"(advert|recruit|vacanc|notice|notification|exam|cgl|chsl|mts|rrb|officer|grade|constable|clerk|po|so|assistant)", t, re.I):
                continue
            url = h if h.startswith("http") else urljoin(base, h)
            if not is_strict_official_link(base, url):
                continue
            out.append((t, url))
        return out
    except:
        return []

def key_from(title, url):
    raw = clean(title).lower() + "|" + (urlparse(url or "").netloc + urlparse(url or "").path).lower()
    return hashlib.sha1(raw.encode()).hexdigest()[:16]

def run(out_jsonl):
    official=[]
    for base, sel, org, dom in OFFICIAL_SITES:
        for t, u in fetch(base, sel):
            if not eligible_title(t): continue
            rec = mk(t, u, org, dom, "official")
            # Try to fetch surrounding text for posts if still missing and same host
            if "numberOfPosts" not in rec:
                try:
                    rr = requests.get(u, timeout=20, headers=UA)
                    if rr.ok:
                        txt = BeautifulSoup(rr.text, "html.parser").get_text(" ", strip=True)
                        pp = posts_from_text(txt)
                        if pp: rec["numberOfPosts"]=pp
                except: pass
            official.append(rec)
        time.sleep(0.4)

    # Aggregators (kept conservative; prefer official links or strong signals)
    agg_pairs=[]
    for base, sel in AGGREGATORS:
        agg_pairs.extend(fetch(base, sel))
        time.sleep(0.3)

    group = {}
    for t,u in agg_pairs:
        if not eligible_title(t): continue
        k = key_from(t,u)
        group.setdefault(k, []).append((t,u))

    def score_domain(u):
        try: return AGG_SCORES.get(host(u), 0.0)
        except: return 0.0

    agg_kept=[]
    for k, items in group.items():
        items = list({(t,u) for (t,u) in items})
        kept=None
        for t,u in items:
            if is_official(u): kept=(t,u); break
        if kept and is_official(kept[1]):
            rec = mk(kept[0], kept[1], "", "All India", "aggregator")
            agg_kept.append(rec); continue
        if len(items) >= 2:
            t,u = items[0]; agg_kept.append(mk(t,u,"","All India","aggregator")); continue
        t,u = items[0]
        if score_domain(u) >= 0.75 and (u.lower().endswith(".pdf") or "/notice" in u.lower() or "/advert" in u.lower()) and is_official(u):
            agg_kept.append(mk(t,u,"","All India","aggregator"))

    by_sig={}
    for rec in official + agg_kept:
        sig = key_from(rec["title"], rec["detailLink"] or rec["applyLink"])
        if sig in by_sig:
            base = by_sig[sig]
            # prefer integer numberOfPosts if present
            if rec.get("numberOfPosts") and not base.get("numberOfPosts"):
                base["numberOfPosts"]=rec["numberOfPosts"]
            # merge flags minimally
            base["flags"] = { **(base.get("flags") or {}), **(rec.get("flags") or {}) }
            for f in ("qualificationLevel","domicile","deadline","applyLink","detailLink","source","type"):
                if (not base.get(f) or base.get(f)=="N/A") and rec.get(f):
                    base[f]=rec[f]
        else:
            by_sig[sig]=rec

    os.makedirs(os.path.dirname(out_jsonl) or ".", exist_ok=True)
    with open(out_jsonl, "w", encoding="utf-8") as f:
        for rec in by_sig.values():
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv)>1 else "tmp/candidates.jsonl"
    run(out)
