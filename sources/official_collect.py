#!/usr/bin/env python3
import requests, json, sys, re, time
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from tools.eligibility import eligible, education_band

def clean(s):
    import re
    return re.sub(r"\s+"," ", (s or "").strip())

def mk(title, url, org, domicile, source_site):
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

def collect_ssc():
    BASE = "https://ssc.nic.in"
    r = requests.get(BASE, timeout=25, headers={"User-Agent":"Mozilla/5.0"})
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    out = []
    for a in soup.select("a[href]"):
        t=a.get_text(" ", strip=True); h=a["href"]
        if not t or not h: continue
        if not re.search(r"(recruit|vacanc|notice|advert|mts|chsl|cgl|gd|constable|selection post)", t, re.I): continue
        url = h if h.startswith("http") else urljoin(BASE, h)
        if eligible(t):
            out.append(mk(t, url, "SSC", "All India", "ssc.nic.in"))
    return out

def collect_bssc():
    BASE = "https://bssc.bihar.gov.in/"
    r = requests.get(BASE, timeout=25, headers={"User-Agent":"Mozilla/5.0"})
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    out=[]
    for a in soup.select("#NoticeBoard a[href], a[href*='Advt'], a[href*='advert']"):
        t=a.get_text(" ", strip=True); h=a["href"]
        url = h if h.startswith("http") else urljoin(BASE, h)
        if eligible(t):
            out.append(mk(t, url, "BSSC", "Bihar", "bssc.bihar.gov.in"))
    return out

def collect_bpsc():
    BASE = "https://bpsc.bihar.gov.in/"
    r = requests.get(BASE, timeout=25, headers={"User-Agent":"Mozilla/5.0"})
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    out=[]
    for a in soup.select("a[href]"):
        t=a.get_text(" ", strip=True); h=a["href"]
        if not re.search(r"(advert|recruit|vacanc|notice|assistant|officer|aedo|cgl|inter)", t, re.I): continue
        url = h if h.startswith("http") else urljoin(BASE, h)
        if eligible(t):
            out.append(mk(t, url, "BPSC", "Bihar", "bpsc.bihar.gov.in"))
    return out

def collect_ibps():
    BASE = "https://www.ibps.in/"
    r = requests.get(BASE, timeout=25, headers={"User-Agent":"Mozilla/5.0"})
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    out=[]
    for a in soup.select("a[href]"):
        t=a.get_text(" ", strip=True); h=a["href"]
        if not re.search(r"(crp|rrb|po|clerk|officer|notification|advert|recruit)", t, re.I): continue
        url = h if h.startswith("http") else urljoin(BASE, h)
        if eligible(t):
            out.append(mk(t, url, "IBPS", "All India", "ibps.in"))
    return out

def collect_rbi():
    BASE = "https://opportunities.rbi.org.in/"
    r = requests.get(BASE, timeout=25, headers={"User-Agent":"Mozilla/5.0"})
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    out=[]
    for a in soup.select("a[href]"):
        t=a.get_text(" ", strip=True); h=a["href"]
        if not re.search(r"(recruit|grade|officer|notification)", t, re.I): continue
        url = h if h.startswith("http") else urljoin(BASE, h)
        if eligible(t):
            out.append(mk(t, url, "RBI", "All India", "rbi.org.in"))
    return out

def collect_rrb():
    BASE = "https://www.rrbapply.gov.in/"
    r = requests.get(BASE, timeout=25, headers={"User-Agent":"Mozilla/5.0"})
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    out=[]
    for a in soup.select("a[href]"):
        t=a.get_text(" ", strip=True); h=a["href"]
        if not re.search(r"(cen|ntpc|recruit|vacanc|controller|guard|constable)", t, re.I): continue
        url = h if h.startswith("http") else urljoin(BASE, h)
        if eligible(t):
            out.append(mk(t, url, "RRB", "All India", "rrbapply.gov.in"))
    return out

def run(out_jsonl):
    all_items = []
    for fn in [collect_ssc, collect_bssc, collect_bpsc, collect_ibps, collect_rbi, collect_rrb]:
        try:
            all_items.extend(fn())
        except Exception:
            continue
        time.sleep(0.5)
    with open(out_jsonl, "w", encoding="utf-8") as f:
        for it in all_items:
            f.write(json.dumps(it, ensure_ascii=False) + "\n")

if __name__ == "__main__":
    run(sys.argv[1] if len(sys.argv)>1 else "tmp/official.jsonl")
