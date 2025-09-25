#!/usr/bin/env python3
# qc_and_learn.py v2025-09-26-stable3 — report-driven corrections + update merge + 7-day archive

import json, pathlib, re, argparse, urllib.parse
from datetime import datetime, timedelta, date

P = pathlib.Path
def JLOAD(p, d):
    try:
        if P(p).exists(): return json.loads(P(p).read_text(encoding="utf-8"))
    except: pass
    return d
def JLOADL(p):
    out=[]
    if P(p).exists():
        for line in P(p).read_text(encoding="utf-8").splitlines():
            line=line.strip()
            if not line: continue
            try: out.append(json.loads(line))
            except: pass
    return out
def JWRITE(p, obj): P(p).write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")

ap = argparse.ArgumentParser()
ap.add_argument("--mode", default="nightly")
RUN_MODE = (ap.parse_args().mode or "nightly").lower()

raw = JLOAD("data.json", {"jobListings":[], "archivedListings":[], "transparencyInfo":{}})
jobs = list(raw.get("jobListings") or [])
archived = list(raw.get("archivedListings") or [])

votes = JLOADL("votes.jsonl")
reports = JLOADL("reports.jsonl")
subs = JLOADL("submissions.jsonl")
rules = JLOAD("rules.json", {"captureHints":[], "aggregatorScores":{}})
user_state = JLOAD("user_state.json", {})

def norm_url(u):
    try:
        p=urllib.parse.urlparse(u or "")
        base=p._replace(query="", fragment="")
        s=urllib.parse.urlunparse(base)
        return s.rstrip("/").lower()
    except: return (u or "").rstrip("/").lower()

def parse_date_any(s):
    if not s or s.strip().upper()=="N/A": return None
    s=s.strip()
    for f in ("%Y-%m-%d","%d/%m/%Y","%d-%m-%Y","%d %B %Y","%d %b %Y"):
        try: return datetime.strptime(s,f).date()
        except: pass
    return None

UPD_TOK = ["corrigendum","extension","extended","addendum","amendment","revised","rectified","notice","last date","reopen","re-open","reopened"]
DATE_PAT = re.compile(r"(\d{1,2}[-/]\d{1,2}[-/]\d{2,4}|\d{1,2}\s+[A-Za-z]{3,9}\s+\d{2,4})")
def is_update_title(t): return any(k in (t or "").lower() for k in UPD_TOK)

def normalize_pdf_stem(u):
    try:
        p=urllib.parse.urlparse(u or ""); fn=(p.path or "").rsplit("/",1)[-1].lower()
        fn=re.sub(r"(?i)(corrigendum|extension|extended|addendum|amendment|notice|revised|rectified|reopen|re-open|reopened)","",fn)
        return re.sub(r"[\W_]+","", fn)
    except: return ""

def url_root(u):
    try:
        p=urllib.parse.urlparse(u or "")
        root=p._replace(query="", fragment="")
        path=(root.path or "/").rsplit("/",1)[0]
        return f"{root.scheme}://{root.netloc}{path}"
    except: return u or ""

def adv_no(t):
    m=re.search(r"(advt|advertisement|notice)\s*(no\.?|number)?\s*[:\-]?\s*([A-Za-z0-9\/\-\._]+)", t or "", re.I)
    return m.group(3).lower() if m else ""

POSTS_PAT = re.compile(r"(\d{1,6})\s*(posts?|vacanc(?:y|ies)|seats?)", re.I)
def parse_posts_from_text(txt):
    if not txt: return None
    m = POSTS_PAT.search(txt)
    if m:
        try: return int(m.group(1))
        except: return None
    return None

# Merge updates into parents
parents=[j for j in jobs if not is_update_title(j.get("title"))]
kept=[]; merged=0
for j in jobs:
    if not is_update_title(j.get("title")):
        kept.append(j); continue
    best=None; score=0.0
    for p in parents:
        s=0.0
        if url_root(j.get("applyLink"))==url_root(p.get("applyLink")): s+=0.45
        if normalize_pdf_stem(j.get("applyLink")) and normalize_pdf_stem(j.get("applyLink"))==normalize_pdf_stem(p.get("applyLink")): s+=0.35
        if adv_no(j.get("title")) and adv_no(j.get("title"))==adv_no(p.get("title")): s+=0.25
        if s>score: score, best = s, p
    if best and score>=0.6:
        best.setdefault("updates", []).append({"title": j.get("title"), "link": j.get("applyLink"), "capturedAt": datetime.utcnow().isoformat()+"Z"})
        dates=[m.group(1) for m in DATE_PAT.finditer(j.get("title") or "")]
        parsed=[parse_date_any(x.replace("-","/")) for x in dates if x]
        parsed=[d for d in parsed if d]
        if parsed:
            new_deadline=max(parsed)
            cur=parse_date_any(best.get("deadline"))
            if not cur or new_deadline>cur:
                best["deadline"]=new_deadline.strftime("%d/%m/%Y")
        pcount = parse_posts_from_text(j.get("title"))
        if pcount and not best.get("numberOfPosts"):
            best["numberOfPosts"]=pcount
        merged+=1
    else:
        j["type"]="UPDATE"; j.setdefault("flags",{})["no_parent_found"]=True; kept.append(j)
jobs=kept

# Submissions -> captureHints + append if unique
seen_keys={norm_url(j.get("applyLink")) for j in jobs}
for s in subs:
    if s.get("type")!="missing": continue
    title=(s.get("title") or "").strip()
    url=norm_url((s.get("url") or "").strip())
    site=(s.get("officialSite") or "").strip()
    last=(s.get("lastDate") or s.get("deadline") or "").strip() or "N/A"
    posts=s.get("posts")
    if site and site not in rules["captureHints"]: rules["captureHints"].append(site)
    if not title or not url: continue
    if url in seen_keys: continue
    card={
        "id": f"user_{abs(hash(url))%10**9}",
        "title": title,
        "qualificationLevel": "Any graduate",
        "domicile": "All India",
        "deadline": last,
        "applyLink": url,
        "detailLink": url,
        "source": "official",
        "type": "VACANCY",
        "flags": {"added_from_missing": True, "trusted": True}
    }
    try:
        if isinstance(posts,str) and posts.strip().isdigit(): posts=int(posts.strip())
        if isinstance(posts,int) and posts>0: card["numberOfPosts"]=posts
    except: pass
    jobs.append(card); seen_keys.add(url)

# Reports -> corrections map
report_map = {}
for r in reports:
    if r.get("type")!="report": continue
    jid=(r.get("jobId") or "").strip()
    if not jid: continue
    rec=report_map.setdefault(jid, {"reasons": []})
    rec["reasons"].append(r.get("reasonCode") or "")
    if r.get("lastDate"): rec["lastDate"]=r.get("lastDate").strip()
    if r.get("eligibility"): rec["eligibility"]=r.get("eligibility").strip()
    if r.get("evidenceUrl"): rec["evidenceUrl"]=r.get("evidenceUrl").strip()
    if r.get("posts"): rec["posts"]=r.get("posts")

def keep_date(j):
    d=parse_date_any(j.get("deadline"))
    if d: return d
    ku=j.get("flags",{}).get("keep_until")
    if ku:
        try: return datetime.fromisoformat(ku).date()
        except: return None
    return None

APPLIED=set(); NOTI={}
for jid, st in (user_state or {}).items():
    a=(st or {}).get("action"); ts=(st or {}).get("ts")
    if a=="applied": APPLIED.add(jid)
    elif a=="not_interested":
        try: NOTI[jid]=datetime.fromisoformat((ts or "").replace("Z","")).date()
        except: NOTI[jid]=date.today()

primary=[]; applied_list=[]; other=[]; to_delete=set()
today=date.today()
for j in jobs:
    jid=j.get("id") or f"user_{abs(hash(j.get('applyLink','')))%10**9}"
    j["id"]=jid

    # Apply report-driven corrections
    if jid in report_map:
        info=report_map[jid]; reasons=info.get("reasons", [])
        if "wrong_last_date" in reasons and info.get("lastDate"):
            j["deadline"]=info["lastDate"]
        if "wrong_eligibility" in reasons and info.get("eligibility"):
            j["qualificationLevel"]=info["eligibility"]
        if "bad_link" in reasons and info.get("evidenceUrl"):
            j["applyLink"]=info["evidenceUrl"]; j["detailLink"]=info["evidenceUrl"]; j.setdefault("flags",{})["fixed_link"]=True
        if "duplicate" in reasons or "not_vacancy" in reasons or "last_date_over" in reasons:
            j.setdefault("flags",{})["removed_reason"]="reported_"+("_".join(sorted(set(reasons)))) ; archived.append(j) ; continue
        if info.get("posts") and not j.get("numberOfPosts"):
            try:
                p=int(info["posts"])
                if p>0: j["numberOfPosts"]=p
            except: pass

    last=keep_date(j)
    if last is not None: j["daysLeft"]=(last - today).days

    if not j.get("numberOfPosts"):
        c=parse_posts_from_text(j.get("title")) or j.get("flags",{}).get("posts")
        if c: j["numberOfPosts"]=c

    if jid in APPLIED:
        j.setdefault("flags",{})["applied"]=True
        applied_list.append(j); continue

    if jid in NOTI and (today-NOTI[jid]).days>14:
        to_delete.add(jid); continue

    if last and last < today:
        if (today - last).days > 7:
            j.setdefault("flags",{})["auto_archived"]="expired_7d"
            archived.append(j)
        else:
            other.append(j)
    else:
        primary.append(j)

jobs_out=[j for j in primary+other+applied_list if j["id"] not in to_delete]
for j in jobs_out:
    if not j.get("detailLink"): j["detailLink"]= j.get("applyLink")

def host(u):
    try: return urllib.parse.urlparse(u or "").netloc.lower()
    except: return ""
sources=set()
for h in (rules.get("captureHints") or []):
    try: sources.add(urllib.parse.urlparse(h).netloc.lower())
    except: pass
seen_hosts={}
for j in jobs_out:
    seen_hosts.setdefault(host(j.get("applyLink")),0)
    seen_hosts[host(j.get("applyLink"))]+=1
sources_status=[{"host":h,"items":seen_hosts.get(h,0)} for h in sorted(sources)]

transp = raw.get("transparencyInfo") or {}
transp.update({
    "schemaVersion":"1.6",
    "runMode": RUN_MODE,
    "lastUpdated": datetime.utcnow().isoformat()+"Z",
    "mergedUpdates": merged,
    "totalListings": len(jobs_out),
    "sourcesByStatus": sources_status,
    "archivedCount": len(archived)
})
out = {
    "jobListings": jobs_out,
    "archivedListings": archived,
    "sections": {
        "applied": [j["id"] for j in applied_list],
        "other": [j["id"] for j in other],
        "primary": [j["id"] for j in primary]
    },
    "transparencyInfo": transp
}
JWRITE("data.json", out)
JWRITE("rules.json", rules)
JWRITE("learn.json", {"mergedUpdates": merged,"generatedAt": datetime.utcnow().isoformat()+"Z","runMode": RUN_MODE})
JWRITE("health.json", {"ok": True, **transp})
