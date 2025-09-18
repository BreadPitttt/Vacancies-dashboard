# qc_and_learn.py — learn, merge, respect user state, 14-day tidy
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
user_state = JLOAD("user_state.json", {})  # {jobId:{action, ts}}

# --- Utilities ---
def norm_url(u):
    try:
        p=urllib.parse.urlparse(u or "")
        base=p._replace(query="", fragment="")
        return urllib.parse.urlunparse(base).rstrip("/").lower()
    except: return (u or "").rstrip("/").lower()

def today(): return date.today()
def parse_deadline(s):
    if not s or s.strip().upper()=="N/A": return None
    for f in ("%d-%m-%Y","%d/%m/%Y","%d %b %Y","%d %B %Y","%Y-%m-%d"):
        try:
            return datetime.strptime(s.strip(), f).date()
        except: pass
    return None

# --- Merge updates (conservative) ---
UPD_TOK = ["corrigendum","extension","extended","addendum","amendment","revised","rectified","notice"]
def is_update_title(t): 
    tl=(t or "").lower()
    return any(k in tl for k in UPD_TOK)

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
        # try date in title to extend deadline
        m=re.search(r"(\d{1,2}[-/]\d{1,2}[-/]\d{2,4}|\d{1,2}\s+[A-Za-z]{3,9}\s+\d{2,4})", j.get("title") or "")
        if m: best["deadline"]=m.group(1)
        merged+=1
    else:
        j["type"]="UPDATE"; j.setdefault("flags",{})["no_parent_found"]=True; kept.append(j)
jobs=kept

# --- Hard reports -> archive (hidden on UI) ---
hard_ids=set(); hard_urls=set(); hard_titles=set(); meta={}
for r in reports:
    if r.get("type")=="report":
        jid=(r.get("jobId") or r.get("listingId") or "").strip()
        if jid: hard_ids.add(jid); meta[jid]=r
        if r.get("url"): hard_urls.add(norm_url(r["url"])); meta[norm_url(r["url"])]=r
        if r.get("title"): hard_titles.add((r["title"] or "").strip().lower()); meta[(r.get("title") or "").strip().lower()]=r

next_jobs=[]  # visible jobs
for j in jobs:
    jid=j.get("id",""); url=norm_url(j.get("applyLink")); title=(j.get("title") or "").strip().lower()
    if jid in hard_ids or url in hard_urls or title in hard_titles:
        j.setdefault("flags",{})["removed_reason"]="reported_hard"
        archived.append(j)
    else:
        next_jobs.append(j)
jobs=next_jobs

# --- Missing submissions -> add hints & seed cards ---
OFFICIAL_OK = lambda d: (d.endswith(".gov.in") or d.endswith(".nic.in") or d in {"ssc.gov.in","ibps.in","opportunities.rbi.org.in","bssc.bihar.gov.in","onlinebssc.com","rrbcdg.gov.in","rrbapply.gov.in","dsssb.delhi.gov.in","bpsc.bihar.gov.in","ccras.nic.in"})
for s in subs:
    if s.get("type")=="missing" and s.get("url"):
        u=norm_url(s["url"]); dom=urllib.parse.urlparse(u).hostname or ""
        if OFFICIAL_OK(dom) and u not in [norm_url(j.get("applyLink")) for j in jobs]:
            jobs.append({
                "id": f"user_{abs(hash(u))%10**9}",
                "title": s.get("title") or "Official notification",
                "organization": "",
                "qualificationLevel": "Any graduate",
                "domicile": "All India",
                "deadline": "N/A",
                "applyLink": u,
                "source": "official",
                "type": "VACANCY",
                "flags": {"added_from_missing": True}
            })
            if u not in rules["captureHints"]: rules["captureHints"].append(u)

# --- Green tick learning and persistence till deadline ---
# A vote "right" pins the job (trusted=true) so it stays until its deadline; a vote "wrong" demotes (could be removed next runs).
pin=set(); demote=set()
for v in votes:
    if v.get("type")=="vote":
        if v.get("vote")=="right" and v.get("jobId"): pin.add(v["jobId"])
        if v.get("vote")=="wrong" and v.get("jobId"): demote.add(v["jobId"])

for j in jobs:
    if j["id"] in pin:
        j.setdefault("flags",{})["trusted"]=True
        # If no deadline, keep for 21 days from first see time
        if not parse_deadline(j.get("deadline")):
            j["flags"]["keep_until"]= (date.today()+timedelta(days=21)).isoformat()
    if j["id"] in demote:
        j.setdefault("flags",{})["demoted"]=True

# --- User state: applied / not_interested ---
# If applied => never auto-delete; show in an Applied section.
# If not_interested or no action => move to Secondary after 14 days; then drop.
APPLIED, NOTI = set(), {}
for jid, st in (user_state or {}).items():
    a = (st or {}).get("action")
    ts = (st or {}).get("ts")
    if a=="applied": APPLIED.add(jid)
    elif a=="not_interested":
        try:
            dt = datetime.fromisoformat(ts.replace("Z","")).date() if ts else today()
        except: dt = today()
        NOTI[jid] = dt

visible=[]; secondary=[]; applied_list=[]
for j in jobs:
    jid=j["id"]
    if jid in APPLIED:
        j.setdefault("flags",{})["applied"]=True
        applied_list.append(j); 
        continue
    # determine expiry policy
    dl = parse_deadline(j.get("deadline"))
    keep_until = j.get("flags",{}).get("keep_until")
    keep_date = None
    if dl: keep_date = dl
    elif keep_until: 
        try: keep_date = datetime.fromisoformat(keep_until).date()
