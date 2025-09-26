#!/usr/bin/env python3
# qc_and_learn.py v2025-09-26-learn-pattern — pattern-based self-learning, safe field fixes, and 7-day archive
# Changes vs prior:
# - Fixes Python one-line syntax (posts assignment lines)
# - Adds pattern registry that learns exact "not vacancy" signatures per host (title/path tokens), not global host penalties
# - Keeps previous merge, submissions->captureHints, reports corrections, and sections

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

# Learning registry (pattern-first)
learn = JLOAD("learn_registry.json", {
  "byHost": {},      # optional light stats (ok/bad)
  "bySlug": {},      # safe field hints per slug
  "patterns": {},    # host -> [ { kind:"non_vacancy", titleTokens:[...], pathTokens:[...], addedAt:iso } ]
  "notes": []
})

def note(ev): 
  learn["notes"] = ([{**ev, "at": datetime.utcnow().isoformat()+"Z"}] + learn.get("notes",[]))[:50]

def host(u):
  try: return urllib.parse.urlparse(u or "").netloc.lower()
  except: return ""
def path_tokens(u):
  try:
    p=urllib.parse.urlparse(u or "")
    segs=[s for s in (p.path or "").lower().split("/") if s]
    return segs
  except: return []
def title_tokens(t):
  return [x for x in re.split(r"[^a-z0-9]+",(t or "").lower()) if x]

def norm_url(u):
  try:
    p=urllib.parse.urlparse(u or "")
    base=p._replace(query="", fragment="")
    s=urllib.parse.urlunparse(base)
    return s.rstrip("/").lower()
  except: return (u or "").rstrip("/").lower()

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

# ---------- Pattern learning helpers ----------

def patterns_for_host(h):
  return learn["patterns"].get(h, [])

def mark_non_vacancy_pattern(h, title, url):
  if not h: return
  tt = list(dict.fromkeys(title_tokens(title)))[:8]
  pt = [x for x in path_tokens(url) if len(x)<=40][:6]
  pat = {"kind":"non_vacancy","titleTokens":tt,"pathTokens":pt,"addedAt":datetime.utcnow().isoformat()+"Z"}
  arr = learn["patterns"].setdefault(h, [])
  # de-duplicate by token sets
  def same(a,b): return a["kind"]==b["kind"] and a["titleTokens"]==b["titleTokens"] and a["pathTokens"]==b["pathTokens"]
  if not any(same(p,pat) for p in arr):
    arr.append(pat); note({"learned":"non_vacancy_pattern","host":h,"titleTokens":tt,"pathTokens":pt})

def matches_non_vacancy_pattern(h, title, url):
  arr = patterns_for_host(h)
  tt = set(title_tokens(title))
  pt = set(path_tokens(url))
  for p in arr:
    if p.get("kind")!="non_vacancy": continue
    need_tt = set(p.get("titleTokens",[]))
    need_pt = set(p.get("pathTokens",[]))
    # require at least half of required title tokens and all path tokens present
    if need_pt and not need_pt.issubset(pt): 
      continue
    if not need_tt or len(tt.intersection(need_tt))>=max(1,len(need_tt)//2 or 1):
      return True
  return False

def learn_set_slug(slug, **kw):
  if not slug: return
  rec = learn["bySlug"].setdefault(slug, {})
  changed=False
  for k,v in kw.items():
    if v in (None,""): continue
    if rec.get(k)!=v:
      rec[k]=v; changed=True
  if changed:
    rec["updatedAt"]=datetime.utcnow().isoformat()+"Z"
    note({"slug_hint":slug, **kw})

# ---------- Merge updates into parents ----------
parents=[j for j in jobs if not is_update_title(j.get("title"))]
kept=[]; merged_count=0
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
    # Try to extract extended date and posts from update title
    dates=[m.group(1) for m in DATE_PAT.finditer(j.get("title") or "")]
    parsed=[parse_date_any(x.replace("-","/")) for x in dates if x]
    parsed=[d for d in parsed if d]
    if parsed:
      new_deadline=max(parsed)
      cur=parse_date_any(best.get("deadline"))
      if not cur or new_deadline>cur:
        best["deadline"]=new_deadline.strftime("%d/%m/%Y")
        learn_set_slug(slugify(best.get("title")), lastDate=best["deadline"])
    pcount = parse_posts_from_text(j.get("title"))
    if pcount and not best.get("numberOfPosts"):
      best["numberOfPosts"]=pcount
      learn_set_slug(slugify(best.get("title")), posts=pcount)
    merged_count+=1
  else:
    j["type"]="UPDATE"; j.setdefault("flags",{})["no_parent_found"]=True; kept.append(j)
jobs=kept

# ---------- Submissions: captureHints + add ----------
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
    if isinstance(posts,str) and posts.strip().isdigit():
      posts=int(posts.strip())
    if isinstance(posts,int) and posts>0:
      card["numberOfPosts"]=posts
  except: pass
  jobs.append(card); seen_keys.add(url)

# ---------- Reports to corrections and patterns ----------
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

primary=[]; applied_list=[]; other=[]; to_delete=set()
today=date.today()

for j in jobs:
  jid=j.get("id") or f"user_{abs(hash(j.get('applyLink','')))%10**9}"
  j["id"]=jid

  # Skip by learned non-vacancy pattern (host + tokens), but never if explicit posts and clear deadline exist
  h=host(j.get("applyLink"))
  if matches_non_vacancy_pattern(h, j.get("title",""), j.get("applyLink","")):
    if not (j.get("numberOfPosts") and parse_date_any(j.get("deadline"))):
      j.setdefault("flags",{})["auto_filtered"]="learn_non_vacancy"
      archived.append(j)
      continue

  # Apply report-driven corrections
  if jid in report_map:
    info=report_map[jid]; reasons=set([x for x in info.get("reasons",[]) if x])
    s=slugify(j.get("title"))

    if "wrong_last_date" in reasons and info.get("lastDate"):
      j["deadline"]=info["lastDate"]; learn_set_slug(s, lastDate=j["deadline"])
    if "wrong_eligibility" in reasons and info.get("eligibility"):
      j["qualificationLevel"]=info["eligibility"]; learn_set_slug(s, eligibility=j["qualificationLevel"])
    if "bad_link" in reasons and info.get("evidenceUrl"):
      j["applyLink"]=info["evidenceUrl"]; j["detailLink"]=info["evidenceUrl"]; j.setdefault("flags",{})["fixed_link"]=True
      learn_set_slug(s, fixedLink=j["applyLink"])
    if "duplicate" in reasons or "not_vacancy" in reasons or "last_date_over" in reasons:
      j.setdefault("flags",{})["removed_reason"]="reported_"+("_".join(sorted(reasons)))
      # Learn precise non-vacancy pattern for this host
      if "not_vacancy" in reasons:
        mark_non_vacancy_pattern(host(j.get("applyLink")), j.get("title",""), j.get("applyLink",""))
      archived.append(j); 
      continue
    if info.get("posts") and not j.get("numberOfPosts"):
      try:
        p=int(info["posts"])
        if p>0:
          j["numberOfPosts"]=p
          learn_set_slug(s, posts=p)
      except: pass

  # Days left + posts fallback
  last=keep_date(j)
  if last is not None: j["daysLeft"]=(last - today).days
  if not j.get("numberOfPosts"):
    c=parse_posts_from_text(j.get("title")) or j.get("flags",{}).get("posts")
    if c: j["numberOfPosts"]=c

  # Sectioning (unchanged; relies on user_state overlay at render time)
  # We do not have user_state in this script; UI overlays it.
  if last and last < today:
    if (today - last).days > 7:
      j.setdefault("flags",{})["auto_archived"]="expired_7d"
      archived.append(j)
    else:
      other.append(j)
  else:
    primary.append(j)

# Transparency + outputs
def host_only(u):
  try: return urllib.parse.urlparse(u or "").netloc.lower()
  except: return ""
sources=set()
for h in (rules.get("captureHints") or []):
  try: sources.add(urllib.parse.urlparse(h).netloc.lower())
  except: pass
seen_hosts={}
for j in primary+other:
  seen_hosts.setdefault(host_only(j.get("applyLink")),0)
  seen_hosts[host_only(j.get("applyLink"))]+=1
sources_status=[{"host":h,"items":seen_hosts.get(h,0)} for h in sorted(sources)]

transp = raw.get("transparencyInfo") or {}
transp.update({
  "schemaVersion":"1.7",
  "runMode": RUN_MODE,
  "lastUpdated": datetime.utcnow().isoformat()+"Z",
  "mergedUpdates": merged_count,
  "totalListings": len(primary)+len(other),
  "sourcesByStatus": sources_status,
  "archivedCount": len(archived),
  "learning": {
    "hosts": len(learn.get("byHost",{})),
    "slugs": len(learn.get("bySlug",{})),
    "patterns": { h: len(v) for h,v in learn.get("patterns",{}).items() }
  }
})

out = {
  "jobListings": primary+other,   # applied section is overlaid by UI from single-tenant KV
  "archivedListings": archived,
  "sections": {
    "applied": [],   # UI overlays from USER_STATE
    "other": [j["id"] for j in other],
    "primary": [j["id"] for j in primary]
  },
  "transparencyInfo": transp
}
JWRITE("data.json", out)
JWRITE("rules.json", rules)
JWRITE("learn_registry.json", learn)
JWRITE("learn.json", {"generatedAt": datetime.utcnow().isoformat()+"Z","runMode": RUN_MODE})
JWRITE("health.json", {"ok": True, **transp})
