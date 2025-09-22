#!/usr/bin/env python3
# Hybrid collector v2: official + verified aggregators; stronger filters, eligibility, last-date, posts count.
import requests, json, sys, re, time, os, hashlib
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

UA = {"User-Agent":"Mozilla/5.0"}

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
]

NEG_TOK = re.compile(r"\b(result|cutoff|exam\s*date|admit\s*card|syllabus|answer\s*key)\b", re.I)
ALLOW_UPDATE = re.compile(r"\b(corrigendum|extension|extended|addendum|amendment|revised|rectified|last\s*date)\b", re.I)

ALLOW_EDU = re.compile(r"(10th|matric|ssc\b|12th|intermediate|hsc|any\s+graduate|graduate\b)", re.I)
DISALLOW_STREAM = re.compile(r"(teacher|tgt|pgt|prt|b\.?ed|ctet|tet|b\.?tech|m\.?tech|b\.e|m\.e|mca|bca|engineer|developer|scientist|architect|analyst|nursing|pharma|iti|polytechnic|diploma|mba|msc|m\.sc|phd|post\s*graduate)", re.I)
SKILL_BAD = re.compile(r"(steno|shorthand|trade\s+test|cad|sap|oracle|aws|azure|docker|kubernetes|tally\s*erp)", re.I)

DATE_PAT = re.compile(r"(\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b)", re.I)
LAST_DATE_PAT = re.compile(r"(last\s*date|apply\s*by|closing\s*date)", re.I)
POSTS_PAT = re.compile(r"(\b(\d{1,6})\s*(posts?|vacancies)\b|\((\d{1,6})\s*(posts?|vacancies)\))", re.I)

CONSTABLE_HINT = re.compile(r"\b(constable|gd|chsl|12th)\b", re.I)
TEN_HINT = re.compile(r"\b(10th|matric|ssc\s*exam|mts)\b", re.I)
GRAD_HINT = re.compile(r"\b(graduat|cgl|degree|bachelor)\b", re.I)

def clean(s): return re.sub(r"\s+"," ", (s or "").strip())

def is_official(url):
    try:
        host = urlparse(url or "").netloc.lower()
        return any(host.endswith(x) for x in (".gov.in",".nic.in",".gov",".go.in")) or "rbi.org.in" in host
    except:
        return False

def eligible_title(title):
    t = clean(title)
    if NEG_TOK.search(t) and not ALLOW_UPDATE.search(t):
        return False
    if not ALLOW_EDU.search(t):
        return False
    if DISALLOW_STREAM.search(t):
        return False
    if SKILL_BAD.search(t):
        return False
    return True

def infer_qualification(title):
    t = title.lower()
    if CONSTABLE_HINT.search(t): return "12th pass"
    if TEN_HINT.search(t): return "10th pass"
    if GRAD_HINT.search(t): return "Any graduate"
    # fallback from generic allow
    if re.search(r"\b12(th)?\b|intermediate|hsc", t): return "12th pass"
    if re.search(r"\b10(th)?\b|matric|ssc\b", t): return "10th pass"
    if re.search(r"\bgraduate\b|any\s+degree|bachelor", t): return "Any graduate"
    return "Any graduate"

def extract_last_date(title):
    # Prefer dates that appear near "last date"/"apply by"; else first date in title
    t = title
    if LAST_DATE_PAT.search(t):
        m = DATE_PAT.search(t)
        if m: return m.group(1).replace("-", "/")
    m = DATE_PAT.search(t)
    if m: return m.group(1).replace("-", "/")
    return "N/A"

def extract_posts(title):
    m = POSTS_PAT.search(title)
    if not m: return None
    for g in (m.group(2), m.group(4)):
        if g and g.isdigit(): return int(g)
    return None

def mk(title, url, org, dom, source="official"):
    title = clean(title)
    q = infer_qualification(title)
    last = extract_last_date(title)
    rec = {
        "title": title,
        "organization": org or "",
        "qualificationLevel": q,
        "domicile": dom or ("All India" if is_official(url) else "All India"),
        "deadline": last,
        "applyLink": url,
        "detailLink": url,
        "source": source,
        "type": "VACANCY",
        "flags": { "trusted": source=="official" }
    }
    posts = extract_posts(title)
    if posts:
        rec["flags"]["posts"] = posts
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
            out.append((t, url))
        return out
    except:
        return []

def key_from(title, url):
    raw = clean(title).lower() + "|" + (urlparse(url or "").netloc + urlparse(url or "").path).lower()
    return hashlib.sha1(raw.encode()).hexdigest()[:16]

def run(out_jsonl):
    # 1) Collect official
    official=[]
    for base, sel, org, dom in OFFICIAL_SITES:
        for t, u in fetch(base, sel):
            if not eligible_title(t): continue
            official.append(mk(t, u, org, dom, "official"))
        time.sleep(0.5)

    # 2) Collect aggregators
    agg_pairs=[]
    for base, sel in AGGREGATORS:
        agg_pairs.extend(fetch(base, sel))
        time.sleep(0.5)

    # 3) Cross-verify aggregators: keep only if official link OR 2+ aggregator hits agree on same title+url
    agg_index = {}
    for t, u in agg_pairs:
        if not eligible_title(t): continue
        agg_index.setdefault(key_from(t, u), set()).add((t, u))

    aggregator_kept=[]
    for k, items in agg_index.items():
        items = list(items)
        # keep if any official domain link
        for t,u in items:
            if is_official(u):
                aggregator_kept.append(mk(t, u, "N/A", "All India", "aggregator"))
                break
        else:
            if len(items) >= 2:
                t,u = items[0]
                aggregator_kept.append(mk(t, u, "N/A", "All India", "aggregator"))

    # 4) Deduplicate, prefer official
    by_sig={}
    for rec in official + aggregator_kept:
        sig = key_from(rec["title"], rec["detailLink"] or rec["applyLink"])
        if sig in by_sig:
            base = by_sig[sig]
            if rec["source"]=="official" and base["source"]!="official":
                rec["flags"] = { **base.get("flags",{}), **rec.get("flags",{}) }
                by_sig[sig] = rec
            else:
                for f in ("organization","qualificationLevel","domicile","deadline","applyLink","detailLink"):
                    if (not base.get(f) or base.get(f)=="N/A") and rec.get(f):
                        base[f]=rec[f]
                base["flags"] = { **base.get("flags",{}), **rec.get("flags",{}) }
        else:
            by_sig[sig]=rec

    # 5) Ensure tmp dir and write
    os.makedirs(os.path.dirname(out_jsonl) or ".", exist_ok=True)
    with open(out_jsonl, "w", encoding="utf-8") as f:
        for rec in by_sig.values():
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv)>1 else "tmp/candidates.jsonl"
    run(out)
