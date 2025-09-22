#!/usr/bin/env python3
import requests, json, sys, re, time
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from tools.eligibility import eligible, education_band

UA = {"User-Agent":"Mozilla/5.0"}

def clean(s):
    import re
    return re.sub(r"\s+"," ", (s or "").strip())

def mk(title, url, org, domicile):
    title = clean(title)
    band = education_band(title)
    return {
        "title": title,
        "organization": org,
        "qualificationLevel": band if band!="N/A" else "Any graduate",
        "domicile": domicile,
        "deadline": "N/A",
        "applyLink": url,
        "detailLink": url,
        "source": "official",
        "type": "VACANCY",
        "flags": { "trusted": True }
    }

def collect_generic(base, selector, org, domicile):
    r = requests.get(base, timeout=25, headers=UA)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    out=[]
    for a in soup.select(selector):
        t=a.get_text(" ", strip=True); h=a.get("href","")
        if not t or not h: continue
        if not re.search(r"(advert|recruit|vacanc|notice|notification|exam|cgl|chsl|mts|rrb|officer|grade|constable)", t, re.I): 
            continue
        url = h if h.startswith("http") else urljoin(base, h)
        if eligible(t):
            out.append(mk(t, url, org, domicile))
    return out

def run(out_jsonl):
    items = []
    # Use sites from provided rules.json list; ensure SSC uses ssc.gov.in
    SITES = [
        ("https://ssc.gov.in/", "a[href]", "SSC", "All India"),
        ("https://bssc.bihar.gov.in/", "#NoticeBoard a[href], a[href*='Advt'], a[href*='advert']", "BSSC", "Bihar"),
        ("https://bpsc.bihar.gov.in/", "a[href]", "BPSC", "Bihar"),
        ("https://www.ibps.in/", "a[href]", "IBPS", "All India"),
        ("https://opportunities.rbi.org.in/Scripts/Vacancies.aspx", "a[href]", "RBI", "All India"),
        ("https://www.rrbapply.gov.in/#/auth/landing/", "a[href]", "RRB", "All India"),
    ]
    for base, selector, org, dom in SITES:
        try:
            items.extend(collect_generic(base, selector, org, dom))
        except Exception:
            continue
        time.sleep(0.5)
    with open(out_jsonl, "w", encoding="utf-8") as f:
        for it in items:
            f.write(json.dumps(it, ensure_ascii=False) + "\n")

if __name__ == "__main__":
    run(sys.argv[1] if len(sys.argv)>1 else "tmp/official.jsonl")
