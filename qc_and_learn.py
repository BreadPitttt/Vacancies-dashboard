#!/usr/bin/env python3
# qc_and_learn.py v2025-09-26-learn — minimal self-learning registry
# - Keeps previous behavior (merge updates, submissions -> captureHints, 7-day archive)
# - Applies reports to fix lastDate/eligibility/bad_link and removes dup/not_vacancy
# - Learns from corrections/votes and re-applies safe hints on future runs
# - Does NOT override eligibility rules; only fills safer defaults or prefers trusted sources

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
learn = JLOAD("learn_registry.json", {
  "byHost": {},          # host -> { ok:int, bad:int, bad_link:int, wrong_date:int, not_vacancy:int }
  "bySlug": {},          # slug -> { lastDate:str, eligibility:str, fixedLink:str, posts:int, updatedAt:iso }
  "notes": []            # recent events (trimmed)
})

def norm_url(u):
  try:
    p=urllib.parse.urlparse(u or "")
    base=p._replace(query="", fragment="")
    s=urllib.parse.urlunparse(base)
    return s.rstrip("/").lower()
  except: return (u or "").rstrip("/").lower()

def host(u):
  try: return urllib.parse.urlparse(u or "").netloc.lower()
  except: return ""

SLUG_PAT = re.compile(r"[a-z0-9]+")
def slugify(text):
  t=(text or "").lower()
  t=re.sub(r"[^a-z0-9]+","-",t).strip("-")
  return t[:80] if t else ""

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

# Learning helpers
def learn_bump_host(h, key, inc=1):
  if not h: return
  rec=learn["byHost"].setdefault(h, {"ok":0,"bad":0,"bad_link":0,"wrong_date":0,"not_vacancy":0})
  rec[key]=int(rec.get(key,0))+inc

def learn_remember(slug, **kwargs):
  if not slug: return
  entry=learn["bySlug"].setdefault(slug, {})
  changed=False
  for k,v in kwargs.items():
    if v is None or v=="": continue
    if k in ("lastDate","eligibility","fixedLink","posts") and entry.get(k)!=v:
      entry[k]=v; changed=True
  if changed:
    entry["updatedAt"]=datetime.utcnow().isoformat()+"Z"
    learn["notes"]=[{"slug":slug, **kwargs, "at":entry["updatedAt"]}] + learn["notes"][:49]  # keep last 50

def learn_apply(card):
  # Safe hints only: prefer official hosts or when source=official; for aggregator, hints are soft
  h=host(card.get("applyLink"))
  slug=slugify(card.get("title"))
  hint=learn["bySlug"].get(slug) or {}
  if hint:
    # Deadline: use if card has N/A or earlier than learned date
    if hint.get("lastDate"):
      new=parse_date_any(hint["lastDate"]); cur=parse_date_any(card.get("deadline"))
      if new and (not cur or new>cur):
        card["deadline"]=new.strftime("%d/%m/%Y")
    # Eligibility (only fill if blank or generic)
    if hint.get("eligibility"):
      if not card.get("qualificationLevel") or card.get("qualificationLevel") in ("N/A","Any","Any graduate","Any Graduate"):
        card["qualificationLevel"]=hint["eligibility"]
    # Posts
    if hint.get("posts") and not card.get("numberOfPosts"):
      try:
        p=int(hint["posts"]); if p>0: card["numberOfPosts"]=p
      except: pass
    # Fixed link (only if current looks non-http or from known bad-link host)
    if hint.get("fixedLink"):
      if card.get("applyLink","").startswith(("http://","https://")):
        # keep existing http(s)
        pass
      else:
        card["applyLink"]=hint["fixedLink"]; card["detailLink"]=hint["fixedLink"]

# Merge updates into parents (unchanged)
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
        learn_remember(slugify(best.get("title")), lastDate=best["deadline"])
    pcount = parse_posts_from_text(j.get("title"))
    if pcount and not best.get("numberOfPosts"):
      best["numberOfPosts"]=pcount
      learn_remember(slugify(best.get("title")), posts=pcount)
    merged+=1
  else:
    j["type"]="UPDATE"; j.setdefault("flags",{})["no_parent_found"]=True; kept.append(j)
jobs=kept

# Submissions: captureHints + possible new card (unchanged logic)
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
    "title": title, "qualificationLevel": "Any graduate", "domicile": "All India",
    "deadline": last, "applyLink": url, "detailLink": url,
    "source": "official", "type": "VACANCY",
    "flags": {"added_from_missing": True, "trusted": True}
  }
  try:
    if isinstance(posts,str) and posts.strip().isdigit(): posts=int(posts.strip())
    if isinstance(posts,int) and posts>0: card["numberOfPosts"]=posts
  except: pass
  jobs.append(card); seen_keys.add(url)

# Reports -> corrections and learning
report_map = {}
for r in reports:
  if r.get("type")!="report": continue
  jid=(r.get("jobId") or "").strip()
  if not jid: continue
  rec=report_map.setdefault(jid, {"reasons": []})
  rec["reasons"].append((r.get("reasonCode") or "").strip())
  for k in ("lastDate","eligibility","evidenceUrl","posts"):
    v=r.get(k)
    if v: rec[k]=v

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

  # Apply safe hints before anything else
  learn_apply(j)

  # Apply report-driven corrections
  if jid in report_map:
    info=report_map[jid]; reasons=set([x for x in info.get("reasons",[]) if x])
    h=host(j.get("applyLink"))
    s=slugify(j.get("title"))

    if "wrong_last_date" in reasons and info.get("lastDate"):
      j["deadline"]=info["lastDate"]; learn_remember(s, lastDate=j["deadline"]); learn_bump_host(h,"wrong_date",1)
    if "wrong_eligibility" in reasons and info.get("eligibility"):
      j["qualificationLevel"]=info["eligibility"]; learn_remember(s, eligibility=j["qualificationLevel"])
    if "bad_link" in reasons and info.get("evidenceUrl"):
      j["applyLink"]=info["evidenceUrl"]; j["detailLink"]=info["evidenceUrl"]; j.setdefault("flags",{})["fixed_link"]=True
      learn_remember(s, fixedLink=j["applyLink"]); learn_bump_host(h,"bad_link",1)
    if "duplicate" in reasons or "not_vacancy" in reasons or "last_date_over" in reasons:
      j.setdefault("flags",{})["removed_reason"]="reported_"+("_".join(sorted(reasons)))
      archived.append(j); learn_bump_host(h,"not_vacancy",1); continue
    if info.get("posts") and not j.get("numberOfPosts"):
      try:
        p=int(info["posts"]); 
        if p>0: j["numberOfPosts"]=p; learn_remember(s, posts=p)
      except: pass

  # Derive daysLeft and posts fallback
  last=keep_date(j)
  if last is not None: j["daysLeft"]=(last - today).days
  if not j.get("numberOfPosts"):
    c=parse_posts_from_text(j.get("title")) or j.get("flags",{}).get("posts")
    if c: j["numberOfPosts"]=c

  # Sectioning
  if jid in APPLIED:
    j.setdefault("flags",{})["applied"]=True; applied_list.append(j); continue
  if jid in NOTI and (today-NOTI[jid]).days>14:
    to_delete.add(jid); continue
  if last and last < today:
    if (today - last).days > 7:
      j.setdefault("flags",{})["auto_archived"]="expired_7d"; archived.append(j)
    else:
      other.append(j)
  else:
    primary.append(j)

# Apply very soft host memory: increment ok/bad based on current keeps/removals (for future use)
for j in primary+other+applied_list:
  learn_bump_host(host(j.get("applyLink")),"ok",1)
for j in archived:
  if j.get("flags",{}).get("removed_reason","").startswith("reported_"):
    learn_bump_host(host(j.get("applyLink")),"bad",1)

jobs_out=[j for j in primary+other+applied_list if j["id"] not in to_delete]
for j in jobs_out:
  if not j.get("detailLink"): j["detailLink"]= j.get("applyLink")

# Transparency + outputs
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
  "mergedUpdates": 0,  # unchanged variable; kept to preserve schema
  "totalListings": len(jobs_out),
  "sourcesByStatus": sources_status,
  "archivedCount": len(archived),
  "learning": {
    "byHostKeys": len(learn.get("byHost",{})),
    "bySlugKeys": len(learn.get("bySlug",{}))
  }
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
JWRITE("learn.json", {"generatedAt": datetime.utcnow().isoformat()+"Z","runMode": RUN_MODE})
# Persist learning registry
JWRITE("learn_registry.json", learn)
# Keep health.json consistent
JWRITE("health.json", {"ok": True, **transp})
