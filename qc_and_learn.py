# qc_and_learn.py — generator with learning and modes
import json, pathlib, datetime, re, urllib.parse, argparse
from collections import Counter

def P(p): return pathlib.Path(p)
def read_json(p, default):
    try:
        if P(p).exists(): return json.loads(P(p).read_text(encoding="utf-8"))
    except: pass
    return default
def read_jsonl(p):
    out=[]; 
    if P(p).exists():
        for line in P(p).read_text(encoding="utf-8").splitlines():
            line=line.strip()
            if not line: continue
            try: out.append(json.loads(line))
            except: pass
    return out
def write_json(p, obj): P(p).write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")

def norm_url(u):
    try:
        p=urllib.parse.urlparse(u or "")
        q=urllib.parse.parse_qsl(p.query, keep_blank_values=True)
        q=[(k,v) for (k,v) in q if k.lower() not in ("utm_source","utm_medium","utm_campaign","utm_term","utm_content","ref")]
        p=p._replace(query=urllib.parse.urlencode(q))
        return urllib.parse.urlunparse(p).rstrip("/").lower()
    except: return (u or "").rstrip("/").lower()
def norm_title(t): return re.sub(r"\s+"," ", (t or "").strip().lower())
def parse_deadline(deadline):
    if not deadline or deadline.strip().upper()=="N/A": return None
    for fmt in ("%Y-%m-%d","%d/%m/%Y","%d-%m-%Y","%d %B %Y","%d %b %Y"):
        try: return datetime.datetime.strptime(deadline.strip(), fmt).date()
        except: pass
    return None
OFFICIAL_WHITELIST = {"ssc.gov.in","upsconline.nic.in","upsconline.gov.in","rpsc.rajasthan.gov.in","ibps.in","bpsc.bih.nic.in","opportunities.rbi.org.in","dsssb.delhi.gov.in","bssc.bihar.gov.in","rrbcdg.gov.in","ccras.nic.in"}

def infer_reason(listing):
    title=(listing.get("title") or "").lower()
    url=norm_url(listing.get("applyLink") or listing.get("url") or "")
    dom=urllib.parse.urlparse(url).hostname or ""
    d=parse_deadline(listing.get("deadline"))
    today=datetime.date.today()
    if d and d<today: return {"code":"expired","details":listing.get("deadline")}
    if dom and dom not in OFFICIAL_WHITELIST and any(x in (dom or "") for x in ["freejobalert","sarkariresult","resultbharat","adda247"]):
        return {"code":"ad_redirect","details":dom}
    qual=(listing.get("qualificationLevel") or "").lower()
    text=" ".join([title, qual])
    if re.search(r"\b(10th|12th|class\s*10|class\s*12|matric|intermediate)\b", text):
        return {"code":"eligibility_low","details":"10th/12th detected"}
    if not listing.get("organization") or not listing.get("deadline"):
        return {"code":"incomplete_extraction","details":"missing org/deadline"}
    return {"code":"unknown","details":dom}

def index_listing(lst):
    by_id, by_url, by_title = {}, {}, {}
    for j in lst:
        jid=(j.get("id") or j.get("jobId") or "").strip()
        if jid: by_id[jid]=j
        u=norm_url(j.get("applyLink") or j.get("url") or "")
        if u: by_url[u]=j
        t=norm_title(j.get("title") or "")
        if t: by_title[t]=j
    return by_id, by_url, by_title

ap = argparse.ArgumentParser()
ap.add_argument("--mode", default="nightly")
RUN_MODE = (ap.parse_args().mode or "nightly").lower()

raw = read_json("data.json", {"jobListings": [], "transparencyInfo": {}})
listings = list(raw.get("jobListings") or [])
reports = read_jsonl("reports.jsonl")
votes   = read_jsonl("votes.jsonl")
subs    = read_jsonl("submissions.jsonl")
rules   = read_json("rules.json", {"blacklistedDomains": [], "autoRemoveReasons": ["expired"], "minQualification": None, "captureHints": []})

if RUN_MODE == "weekly":
    rules["blacklistedDomains"] = []
    rules["autoRemoveReasons"] = ["expired"]

# Accept missing from official-like domains and learn hints
added_from_missing = 0
for s in subs:
    if s.get("type")=="missing" and s.get("url") and s.get("title"):
        u = norm_url(s["url"]); dom = urllib.parse.urlparse(u).hostname or ""
        exists = any(norm_url(j.get("applyLink") or j.get("url") or "") == u for j in listings)
        if not exists and (dom in OFFICIAL_WHITELIST or dom.endswith(".gov.in") or dom.endswith(".nic.in")):
            listings.append({
              "id": f"user_{abs(hash(u))%10**9}",
              "title": s["title"],
              "organization": "",
              "qualificationLevel": "",
              "domicile": "All India",
              "deadline": "N/A",
              "applyLink": u,
              "source": "official",
              "type": "UPDATE",
              "flags": {"added_from_missing": True}
            })
            added_from_missing += 1
            if u not in rules["captureHints"]: rules["captureHints"].append(u)

# Hard removals
hard_ids, hard_urls, hard_titles, hard_meta = set(), set(), set(), {}
for r in reports:
    if r.get("type")=="report":
        jid=(r.get("jobId") or r.get("listingId") or "").strip()
        if jid: hard_ids.add(jid); hard_meta[jid]={"evidenceUrl": r.get("url") or r.get("evidenceUrl"), "note": r.get("note")}
        if r.get("url"):
            u=norm_url(r.get("url")); hard_urls.add(u); hard_meta[u]={"evidenceUrl": r.get("url"), "note": r.get("note")}
        if r.get("title"):
            t=norm_title(r.get("title")); hard_titles.add(t); hard_meta[t]={"evidenceUrl": r.get("url") or r.get("evidenceUrl"), "note": r.get("note")}

# Learn from votes
by_id, by_url, by_title = index_listing(listings)
def domain_of(u):
    try: return urllib.parse.urlparse(u or "").hostname or ""
    except: return ""
learn_reasons, rightR, wrongR, domR, domW = Counter(), Counter(), Counter(), Counter(), Counter()
for v in votes:
    if v.get("type")!="vote" or v.get("vote") not in ("right","wrong"): continue
    j = by_id.get(v.get("jobId","")) or by_url.get(norm_url(v.get("url",""))) or by_title.get(norm_title(v.get("title","")))
    if not j: j={"title": v.get("title",""), "url": v.get("url",""), "deadline": None, "qualificationLevel": None}
    reason = infer_reason(j)
    code = reason["code"]
    learn_reasons[code]+=1
    if v["vote"]=="right":
        rightR[code]+=1; domR[domain_of(v.get("url"))]+=1
    else:
        wrongR[code]+=1; domW[domain_of(v.get("url"))]+=1

for code, cnt in rightR.items():
    if cnt>=2 and wrongR.get(code,0)==0 and code in rules.get("autoRemoveReasons", []):
        rules["autoRemoveReasons"]=[c for c in rules["autoRemoveReasons"] if c!=code]
for code, cnt in wrongR.items():
    if cnt>=2 and rightR.get(code,0)==0 and code not in rules.get("autoRemoveReasons", []):
        rules["autoRemoveReasons"].append(code)
for dom, cnt in domR.items():
    if cnt>=2 and domW.get(dom,0)==0 and dom in rules.get("blacklistedDomains", []):
        rules["blacklistedDomains"]=[d for d in rules["blacklistedDomains"] if d!=dom]
for dom, cnt in domW.items():
    if cnt>=2 and domR.get(dom,0)==0 and dom not in rules.get("blacklistedDomains", []):
        rules["blacklistedDomains"].append(dom)

# Suppression
removedByHard=0; removedByAuto=0
kept, archived = [], []
for job in listings:
    jid=(job.get("id") or job.get("jobId") or "").strip()
    url=norm_url(job.get("applyLink") or job.get("url") or "")
    title=norm_title(job.get("title") or "")
    dom=domain_of(url)
    drop=None; meta=None

    if jid and jid in hard_ids: drop="reported_hard"; meta=hard_meta.get(jid)
    elif url and url in hard_urls: drop="reported_hard"; meta=hard_meta.get(url)
    elif title and title in hard_titles: drop="reported_hard"; meta=hard_meta.get(title)

    if not drop:
        rsn=infer_reason(job)["code"]
        if (dom in rules.get("blacklistedDomains", [])) or (rsn in set(rules.get("autoRemoveReasons", []))):
            drop=f"auto_{rsn}"

    if drop:
        job.setdefault("flags",{})["removed_reason"]=drop
        if meta:
            job["flags"]["evidence_url"]=meta.get("evidenceUrl"); job["flags"]["note"]=meta.get("note")
        archived.append(job)
        if drop=="reported_hard": removedByHard+=1
        else: removedByAuto+=1
    else:
        kept.append(job)

transp = raw.get("transparencyInfo") or {}
transp["totalListings"]=len(kept)
transp["lastUpdated"]=datetime.datetime.utcnow().isoformat()+"Z"
transp["removedByHardReports"]=removedByHard
transp["removedByAutoReasons"]=removedByAuto
transp["autoRemoveReasons"]=rules.get("autoRemoveReasons", [])
transp["blacklistedDomains"]=rules.get("blacklistedDomains", [])
transp["addedFromMissing"]=added_from_missing
transp["runMode"]=RUN_MODE

data={"jobListings": kept, "archivedListings": archived, "transparencyInfo": transp}
write_json("data.json", data)
health={"ok": True, "checkedAt": datetime.datetime.utcnow().isoformat()+"Z", **transp}
write_json("health.json", health)
learn={
  "reasons": dict(learn_reasons),
  "rightReasons": dict(rightR),
  "wrongReasons": dict(wrongR),
  "byDomainRight": dict(domR),
  "byDomainWrong": dict(domW),
  "missingAccepted": added_from_missing,
  "generatedAt": datetime.datetime.utcnow().isoformat()+"Z",
  "runMode": RUN_MODE
}
write_json("learn.json", learn)
write_json("rules.json", rules)

