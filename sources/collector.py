#!/usr/bin/env python3
# Hybrid collector: official + two aggregators with cross-verification.
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
]

# Two aggregator pages for discovery only (titles and links are parsed, but entries are kept
# only when link is official OR another independent source confirms)
AGGREGATORS = [
    ("https://www.freejobalert.com/", "a[href]"),
    ("https://sarkarijobfind.com/", "a[href]"),
]

SIMPLE_ALLOW = re.compile(r"(10th|matric|ssc\b|12th|intermediate|hsc|any\s+graduate|graduate\b)", re.I)
DISALLOW = re.compile(r"(teacher|tgt|pgt|prt|b\.?ed|ctet|tet|b\.?tech|m\.?tech|b\.e|m\.e|mca|bca|engineer|developer|scientist|architect|analyst|nursing|pharma|iti|polytechnic|diploma|mba|msc|m\.sc|phd|post\s*graduate)", re.I)
SKILL_BAD = re.compile(r"(steno|shorthand|trade\s+test|cad|sap|oracle|aws|azure|docker|kubernetes|tally\s*erp)", re.I)

def clean(s):
    return re.sub(r"\s+"," ", (s or "").strip())

def is_official(url):
    try:
        host = urlparse(url or "").netloc.lower()
        return any(host.endswith(x) for x in (".gov.in",".nic.in",".gov",".gouv",".go.in")) or "rbi.org.in" in host
    except:
        return False

def eligible_title(title):
    t = clean(title)
    if not SIMPLE_ALLOW.search(t): return False
    if DISALLOW.search(t): return False
    if SKILL_BAD.search(t): return False
    # Domicile heuristic: accept explicit Bihar or open-all text; otherwise neutral
    return True

def mk(title, url, org, dom, source="official"):
    title = clean(title)
    return {
        "title": title,
        "organization": org or "",
        "qualificationLevel": "Any graduate" if re.search(r"\bgraduate\b", title, re.I) else ("12th pass" if re.search(r"(12th|intermediate|hsc)", title, re.I) else ("10th pass" if re.search(r"(10th|matric|ssc\b)", title, re.I) else "Any graduate")),
        "domicile": dom or ("All India" if is_official(url) else "All India"),
        "deadline": "N/A",
        "applyLink": url,
        "detailLink": url,
        "source": source,
        "type": "VACANCY",
        "flags": { "trusted": source=="official" }
    }

def fetch(base, selector):
    try:
        r = requests.get(base, timeout=30, headers=UA)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        out=[]
        for a in soup.select(selector):
            t = a.get_text(" ", strip=True); h=a.get("href","")
            if not t or not h: continue
            if not re.search(r"(advert|recruit|vacanc|notice|notification|exam|cgl|chsl|mts|rrb|officer|grade|constable|clerk|po|so)", t, re.I):
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
    official_pairs=[]
    for base, sel, org, dom in OFFICIAL_SITES:
        for t, u in fetch(base, sel):
            if not eligible_title(t): continue
            official_pairs.append(mk(t, u, org, dom, "official"))
        time.sleep(0.5)

    # 2) Collect aggregators
    agg_pairs_raw=[]
    for base, sel in AGGREGATORS:
        agg_pairs_raw.extend(fetch(base, sel))
        time.sleep(0.5)

    # 3) Cross-verify: keep aggregator item if link is official OR another aggregator agrees on title+official URL
    agg_index = {}
    for t, u in agg_pairs_raw:
        if not eligible_title(t): continue
        url = u
        agg_index.setdefault(key_from(t, url), []).append((t, url))

    aggregator_kept=[]
    for k, items in agg_index.items():
        # If any link is official, keep that
        ofc = [ (t,u) for t,u in items if is_official(u) ]
        if ofc:
            t,u = ofc[0]
            aggregator_kept.append(mk(t, u, "N/A", "All India", "aggregator"))
            continue
        # Otherwise, require at least 2 independent mentions for same title+url
        if len(items) >= 2:
            t,u = items[0]
            aggregator_kept.append(mk(t, u, "N/A", "All India", "aggregator"))

    # 4) Deduplicate official + aggregator (prefer official)
    by_sig={}
    for rec in official_pairs + aggregator_kept:
        sig = key_from(rec["title"], rec["detailLink"] or rec["applyLink"])
        if sig in by_sig:
            # prefer official over aggregator; enrich links/flags
            if rec["source"]=="official" and by_sig[sig]["source"]!="official":
                base = by_sig[sig]
                rec["flags"] = { **base.get("flags",{}), **rec.get("flags",{}) }
                by_sig[sig] = rec
            else:
                # enrich fields if empty
                base = by_sig[sig]
                for f in ("organization","qualificationLevel","domicile","deadline","applyLink","detailLink"):
                    if (not base.get(f) or base.get(f)=="N/A") and rec.get(f):
                        base[f]=rec[f]
        else:
            by_sig[sig]=rec

    # 5) Write JSONL; ensure directory exists; if none, create empty file
    os.makedirs(os.path.dirname(out_jsonl) or ".", exist_ok=True)
    with open(out_jsonl, "w", encoding="utf-8") as f:
        for rec in by_sig.values():
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv)>1 else "tmp/candidates.jsonl"
    run(out)
