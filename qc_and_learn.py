# qc_and_learn.py — pin by votes, handle missing(title,url,lastDate), update deadlines from corrigendum
import json, pathlib, re, argparse, urllib.parse
from datetime import datetime, timedelta, date

P = pathlib.Path
def JLOAD(p, d): 
    try:
        if P(p).exists(): return json.loads(P(p).read_text(encoding="utf-8"))
    except: pass
    return d
def JLOADL(p):
    out=[]; 
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

# --- Update merge and deadline refresh ---
UPD_TOK = ["corrigendum","extension","extended","addendum","amendment","revised","rectified","notice"]
def is_update_title(t): return any(k in (t or "").lower() for k in UPD_TOK)
def pdf_base(u):
    try:
        p=urllib.parse.urlparse(u or ""); fn=(p.path or "").rsplit("/",1)[-1]
        fn=re.sub(r"(?i)(corrigendum|extension|extended|addendum|amendment|notice|revised|rectified)","",fn)
        return re.sub(r"[\W_]+","", fn.lower())
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

parents=[j for j in jobs if not is_update_title(j.get("title"))]
kept=[]; merged=0
for j in jobs:
    if not is_update_title(j.get("title")):
        kept.append(j); continue
    best=None; score=0.0
    for p in parents:
        s=0.0
        if url_root(j.get("applyLink"))==url_root(p.get("applyLink")): s+=0.5
        if pdf_base(j.get("applyLink")) and pdf_base(j.get("applyLink"))==pdf_base(p.get("applyLink")): s+=0.3
        if adv_no(j.get("title")) and adv_no(j.get("title"))==adv_no(p.get("title")): s+=0.3
        if s>score: score, best = s, p
    if best and score>=0.6:
        best.setdefault("updates", []).append({"title": j.get("title"), "link": j.get("applyLink"), "capturedAt": datetime.utcnow().isoformat()+"Z"})
        m=re.search(r"(\d{1,2}[-/]\d{1,2}[-/]\d{2,4}|\d{1,2}\s+[A-Za-z]{3,9}\s+\d{2,4})", j.get("title") or "")
        if m: best["deadline"]=m.group(1)
        merged+=1
    else:
        j["type"]="UPDATE"; j.setdefault("flags",{})["no_parent_found"]=True; kept.append(j)
jobs=kept

# --- Hard reports -> archive ---
hard_ids=set(); hard_urls=set(); hard_titles=set()
for r in reports:
    if r.get("type")=="report":
        jid=(r.get("jobId") or r.get("listingId") or "").strip()
        if jid: hard_ids.add(jid)
        if r.get("url"): hard_urls.add(norm_url(r["url"]))
        if r.get("title"): hard_titles.add((r["title"] or "").strip().lower())
jobs=[(archived.append(j) or None) and j for j in jobs if False]  # placeholder to satisfy linter
# filter while preserving original order
tmp=[] 
for j in raw.get("jobListings", []):
    jid=j.get("id",""); url=norm_url(j.get("applyLink")); title=(j.get("title") or "").strip().lower()
    if jid in hard_ids or url in hard_urls or title in hard_titles:
        j.setdefault("flags",{})["removed_reason"]="reported_hard"; archived.append(j)
    else:
        tmp.append(j)
jobs=tmp

# --- Missing submissions -> require title,url,lastDate; add hint; pin until lastDate ---
seen={norm_url(j.get("applyLink")) for j in jobs}
for s in subs:
    if s.get("type")=="missing":
        title=(s.get("title") or "").strip()
        url=(s.get("url") or "").strip()
        last=(s.get("lastDate") or s.get("deadline") or "").strip()
        if not title or not url: continue
        if norm_url(url) in seen: continue
        card={
            "id": f"user_{abs(hash(url))%10**9}",
            "title": title,
            "organization": "",
            "qualificationLevel": "Any graduate",
            "domicile": "All India",
            "deadline": last if last else "N/A",
            "applyLink": url,
            "detailLink": url,
            "source": "official",
            "type": "VACANCY",
            "flags": {"added_from_missing": True, "trusted": True}
        }
        jobs.append(card)
        if url not in rules["captureHints"]: rules["captureHints"].append(url)

# --- Green tick learning: pin till deadline or 21 days ---
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

# --- User state and sectioning ---
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
    # daysLeft for UI
    if last:
        j["daysLeft"]=(last - date.today()).days
    if jid in APPLIED:
        j.setdefault("flags",{})["applied"]=True
        applied_list.append(j); 
        continue
    if jid in NOTI and (date.today()-NOTI[jid]).days>14:
        to_delete.add(jid); continue
    if last and last < date.today():
        other.append(j)
    else:
        primary.append(j)

jobs_out=[j for j in primary+other+applied_list if j["id"] not in to_delete]

# ensure detailLink exists
for j in jobs_out:
    if not j.get("detailLink"):
        j["detailLink"]= j.get("applyLink")

transp = raw.get("transparencyInfo") or {}
transp.update({
    "schemaVersion":"1.4",
    "runMode": RUN_MODE,
    "lastUpdated": datetime.utcnow().isoformat()+"Z",
    "mergedUpdates": merged,
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
JWRITE("learn.json", {"mergedUpdates": merged,"generatedAt": datetime.utcnow().isoformat()+"Z","runMode": RUN_MODE})
rules["blacklistedDomains"] = rules.get("blacklistedDomains", [])
rules["autoRemoveReasons"] = rules.get("autoRemoveReasons", ["expired"])
JWRITE("rules.json", rules)
JWRITE("health.json", {"ok": True, **transp})
