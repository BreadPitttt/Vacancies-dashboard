#!/usr/bin/env python3
# qc_and_learn.py — posts propagation + updates + sticky state (2025-09-25-posts)
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
rules = JLOAD("rules.json", {"captureHints":[]})
user_state = JLOAD("user_state.json", {})

def norm_url(u):
    try:
        p=urllib.parse.urlparse(u or "")
        base=p._replace(query="", fragment="")
        return urllib.parse.urlunparse(base).rstrip("/").lower()
    except: return (u or "").rstrip("/").lower()

def parse_deadline(s):
    if not s or s.strip().upper()=="N/A": return None
    for f in ("%d-%m-%Y","%d/%m/%Y","%d %b %Y","%d %B %Y","%Y-%m-%d"):
        try: return datetime.strptime(s.strip(), f).date()
        except: pass
    return None

UPD_TOK = ["corrigendum","extension","extended","addendum","amendment","revised","rectified","notice","last date"]
DATE_PAT = re.compile(r"(\d{1,2}[-/]\d{1,2}[-/]\d{2,4}|\d{1,2}\s+[A-Za-z]{3,9}\s+\d{2,4})")
def is_update_title(t): return any(k in (t or "").lower() for k in UPD_TOK)

def normalize_pdf_stem(u):
    try:
        p=urllib.parse.urlparse(u or ""); fn=(p.path or "").rsplit("/",1)[-1].lower()
        fn=re.sub(r"(?i)(corrigendum|extension|extended|addendum|amendment|notice|revised|rectified)","",fn)
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

# Split parents/updates and merge updates (deadline extension + posts)
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
        parsed=[parse_deadline(x.replace("-","/")) for x in dates if x]
        parsed=[d for d in parsed if d]
        if parsed:
            new_deadline=max(parsed)
            cur=parse_deadline(best.get("deadline"))
            if not cur or new_deadline>cur:
                best["deadline"]=new_deadline.strftime("%d/%m/%Y")
        pcount = parse_posts_from_text(j.get("title"))
        if pcount and not best.get("numberOfPosts"):
            best["numberOfPosts"]=pcount
        merged+=1
    else:
        j["type"]="UPDATE"; j.setdefault("flags",{})["no_parent_found"]=True; kept.append(j)
jobs=kept

# Apply hard reports and collect posts if reported
hard_ids=set(); hard_urls=set(); hard_titles=set(); report_posts={}
for r in reports:
    if r.get("type")=="report":
        jid=(r.get("jobId") or r.get("listingId") or "").strip()
        if jid: hard_ids.add(jid)
        if r.get("url"): hard_urls.add(norm_url(r["url"]))
        if r.get("evidenceUrl"): hard_urls.add(norm_url(r["evidenceUrl"]))
        if r.get("title"): hard_titles.add((r["title"] or "").strip().lower())
        try:
            p = r.get("posts")
            if isinstance(p,str) and p.strip().isdigit(): p=int(p.strip())
            if isinstance(p,int) and p>0: report_posts[jid]=p
        except: pass
tmp=[]
for j in raw.get("jobListings", []):
    jid=j.get("id",""); url=norm_url(j.get("applyLink")); title=(j.get("title") or "").strip().lower()
    if jid in report_posts and not j.get("numberOfPosts"): j["numberOfPosts"]=report_posts[jid]
    if jid in hard_ids or url in hard_urls or title in hard_titles:
        j.setdefault("flags",{})["removed_reason"]="reported_hard"; archived.append(j)
    else:
        tmp.append(j)
jobs=tmp

# Add missing submissions and capture posts if provided
subs = JLOADL("submissions.jsonl")
seen={norm_url(j.get("applyLink")) for j in jobs}
for s in subs:
    if s.get("type")=="missing":
        title=(s.get("title") or "").strip()
        url=(s.get("url") or "").strip()
        site=(s.get("officialSite") or "").strip()
        last=(s.get("lastDate") or s.get("deadline") or "").strip()
        posts = s.get("posts")
        if not title or not url: continue
        if norm_url(url) in seen:
            if site and site not in rules["captureHints"]: rules["captureHints"].append(site)
            continue
        card={
            "id": f"user_{abs(hash(url))%10**9}",
            "title": title,
            "qualificationLevel": "Any graduate",
            "domicile": "All India",
            "deadline": last if last else "N/A",
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
        jobs.append(card)
        if url not in rules["captureHints"]: rules["captureHints"].append(url)
        if site and site not in rules["captureHints"]: rules["captureHints"].append(site)

# Learning: trusted/demote
pin=set(); demote=set()
for v in votes:
    if v.get("type")=="vote":
        if v.get("vote")=="right" and v.get("jobId"): pin.add(v["jobId"])
        if v.get("vote")=="wrong" and v.get("jobId"): demote.add(v["jobId"])
for j in jobs:
    if j["id"] in pin:
        j.setdefault("flags",{})["trusted"]=True
        if not j.get("deadline") or j["deadline"].upper()=="N/A":
            j["flags"]["keep_until"]=(date.today()+timedelta(days=21)).isoformat()
    if j["id"] in demote:
        j.setdefault("flags",{})["demoted"]=True

# Overlay server-side sticky state, compute sections; ensure numberOfPosts is present
APPLIED=set(); NOTI={}
for jid, st in (user_state or {}).items():
    a=(st or {}).get("action"); ts=(st or {}).get("ts")
    if a=="applied": APPLIED.add(jid)
    elif a=="not_interested":
        try: NOTI[jid]=datetime.fromisoformat((ts or "").replace("Z","")).date()
        except: NOTI[jid]=date.today()

def keep_date(j):
    d=parse_deadline(j.get("deadline"))
    if d: return d
    ku=j.get("flags",{}).get("keep_until")
    if ku:
        try: return datetime.fromisoformat(ku).date()
        except: return None
    return None

primary=[]; applied_list=[]; other=[]; to_delete=set()
for j in jobs:
    jid=j["id"]
    last=keep_date(j)
    if last is not None: j["daysLeft"]=(last - date.today()).days
    if not j.get("numberOfPosts"):
        c=parse_posts_from_text(j.get("title")) or j.get("flags",{}).get("posts")
        if c: j["numberOfPosts"]=c
    if jid in APPLIED:
        j.setdefault("flags",{})["applied"]=True
        d=parse_deadline(j.get("deadline"))
        if d and (date.today()-d).days>60:
            j.setdefault("flags",{})["applied_expired"]=True
        applied_list.append(j); continue
    if jid in NOTI and (date.today()-NOTI[jid]).days>14:
        to_delete.add(jid); continue
    if last and last < date.today():
        other.append(j)
    else:
        primary.append(j)

jobs_out=[j for j in primary+other+applied_list if j["id"] not in to_delete]
for j in jobs_out:
    if not j.get("detailLink"): j["detailLink"]= j.get("applyLink")

transp = raw.get("transparencyInfo") or {}
transp.update({
    "schemaVersion":"1.5",
    "runMode": RUN_MODE,
    "lastUpdated": datetime.utcnow().isoformat()+"Z",
    "mergedUpdates": 0,
    "totalListings": len(jobs_out)
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
JWRITE("learn.json", {"generatedAt": datetime.utcnow().isoformat()+"Z","runMode": RUN_MODE})

# keep rules coherent and aggregator scores
rules["blacklistedDomains"] = rules.get("blacklistedDomains", [])
rules["autoRemoveReasons"] = rules.get("autoRemoveReasons", ["expired"])
def domain_of(u):
    try: return urllib.parse.urlparse(u or "").netloc.lower()
    except: return ""
def is_official_host(h):
    return h.endswith(".gov.in") or h.endswith(".nic.in") or h.endswith(".gov") or h.endswith(".go.in") or "rbi.org.in" in h
scores = rules.get("aggregatorScores", {})
def bump(h, delta):
    if not h or is_official_host(h): return
    v = float(scores.get(h, 0.5)) + delta
    v = max(0.0, min(1.0, v))
    scores[h] = round(v, 3)
for v in votes:
    if v.get("type")!="vote": continue
    link = v.get("url") or ""; h = domain_of(link)
    if not h: continue
    if v.get("vote")=="right": bump(h, +0.05)
    elif v.get("vote")=="wrong": bump(h, -0.05)
rules["aggregatorScores"] = scores
JWRITE("rules.json", rules)
JWRITE("health.json", {"ok": True, **transp})
