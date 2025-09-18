# qc_and_learn.py — apply reports/votes, infer reasons, evolve rules, write data+health+learn

import json, pathlib, datetime, re, urllib.parse
from collections import Counter, defaultdict

def path(p): return pathlib.Path(p)

def read_json(p, default):
    try:
        if path(p).exists():
            return json.loads(path(p).read_text(encoding="utf-8"))
    except: pass
    return default

def read_jsonl(p):
    out=[]
    if path(p).exists():
        for line in path(p).read_text(encoding="utf-8").splitlines():
            line=line.strip()
            if not line: continue
            try: out.append(json.loads(line))
            except: pass
    return out

def write_json(p, obj):
    path(p).write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")

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

OFFICIAL_WHITELIST = {"ssc.nic.in","upsconline.nic.in","upsconline.gov.in","rpsc.rajasthan.gov.in"}
ADS_HOSTS = {"freejobalert.com","www.freejobalert.com","sarkariresult.com","www.sarkariresult.com"}

def infer_reason(listing):
    title=(listing.get("title") or "").lower()
    url=norm_url(listing.get("applyLink") or listing.get("url") or "")
    dom=urllib.parse.urlparse(url).hostname or ""
    dea=listing.get("deadline")
    ddate=parse_deadline(dea)
    today=datetime.date.today()

    if ddate and ddate<today: return {"code":"expired", "details":dea}
    if dom and dom in ADS_HOSTS and dom not in OFFICIAL_WHITELIST: return {"code":"ad_redirect","details":dom}
    qual=(listing.get("qualificationLevel") or "").lower()
    text=" ".join([title, qual])
    if re.search(r"\b(10th|12th|class\s*10|class\s*12|matric|intermediate)\b", text):
        return {"code":"eligibility_low","details":"10th/12th detected"}
    if not listing.get("organization") or not listing.get("deadline"):
        return {"code":"incomplete_extraction","details":"missing org/deadline"}
    return {"code":"unknown","details":dom}

# 1) Load inputs
raw_data = read_json("data.json", {"jobListings": [], "transparencyInfo": {}})
listings = list(raw_data.get("jobListings") or [])
reports = read_jsonl("reports.jsonl")
votes   = read_jsonl("votes.jsonl")
rules   = read_json("rules.json", {"blacklistedDomains": [], "autoRemoveReasons": ["expired"], "minQualification": None})

# 2) Build hard removal sets from modal reports
hard_ids, hard_urls, hard_titles, hard_meta = set(), set(), set(), {}
for r in reports:
    if r.get("type")=="report":
        jid=(r.get("jobId") or r.get("listingId") or "").strip()
        if jid:
            hard_ids.add(jid); hard_meta[jid]={"evidenceUrl": r.get("url") or r.get("evidenceUrl"), "note": r.get("note")}
        if r.get("url"):
            u=norm_url(r.get("url")); hard_urls.add(u); hard_meta[u]={"evidenceUrl": r.get("url"), "note": r.get("note")}
        if r.get("title"):
            t=norm_title(r.get("title")); hard_titles.add(t); hard_meta[t]={"evidenceUrl": r.get("url") or r.get("evidenceUrl"), "note": r.get("note")}

# 3) Build learning from votes (both wrong and right)
# Attach refined reason by looking up the listing
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

by_id, by_url, by_title = index_listing(listings)

learn_tally = {
  "reasons": Counter(),
  "rightReasons": Counter(),
  "wrongReasons": Counter(),
  "byDomainRight": Counter(),
  "byDomainWrong": Counter()
}

def domain_of(u):
    try: return urllib.parse.urlparse(u or "").hostname or ""
    except: return ""

enriched_votes=[]
for v in votes:
    if v.get("type")!="vote" or v.get("vote") not in ("right","wrong"): continue
    # lookup
    j = by_id.get(v.get("jobId","")) or by_url.get(norm_url(v.get("url",""))) or by_title.get(norm_title(v.get("title","")))
    if not j:
        # fallback synthetic listing for reason inference
        j={"title": v.get("title",""), "url": v.get("url",""), "deadline": None, "qualificationLevel": None}
    reason = infer_reason(j)
    v2=dict(v); v2["reason"]=reason
    enriched_votes.append(v2)
    learn_tally["reasons"][reason["code"]]+=1
    if v.get("vote")=="right":
        learn_tally["rightReasons"][reason["code"]]+=1
        learn_tally["byDomainRight"][domain_of(v.get("url"))]+=1
    else:
        learn_tally["wrongReasons"][reason["code"]]+=1
        learn_tally["byDomainWrong"][domain_of(v.get("url"))]+=1

# 4) Promote/adjust rules based on RIGHT and WRONG
# Accept patterns: if a reason frequently appears with RIGHT and rarely with WRONG, remove it from autoRemoveReasons
for code, cnt in learn_tally["rightReasons"].items():
    wrong = learn_tally["wrongReasons"].get(code, 0)
    if cnt >= 2 and wrong == 0 and code in rules.get("autoRemoveReasons", []):
        rules["autoRemoveReasons"] = [c for c in rules["autoRemoveReasons"] if c != code]

# Reject patterns: if a reason appears WRONG >=2 and RIGHT ==0, add to autoRemoveReasons
for code, cnt in learn_tally["wrongReasons"].items():
    right = learn_tally["rightReasons"].get(code, 0)
    if cnt >= 2 and right == 0 and code not in rules.get("autoRemoveReasons", []):
        rules["autoRemoveReasons"].append(code)

# Domain acceptance: if a domain has 2 RIGHT and 0 WRONG, ensure it is not blacklisted
for dom, cnt in learn_tally["byDomainRight"].items():
    if cnt >= 2 and learn_tally["byDomainWrong"].get(dom,0) == 0 and dom in rules.get("blacklistedDomains", []):
        rules["blacklistedDomains"] = [d for d in rules["blacklistedDomains"] if d != dom]

# Domain rejection: 2 WRONG and 0 RIGHT -> blacklist
for dom, cnt in learn_tally["byDomainWrong"].items():
    if cnt >= 2 and learn_tally["byDomainRight"].get(dom,0) == 0 and dom not in rules.get("blacklistedDomains", []):
        rules["blacklistedDomains"].append(dom)

# 5) Apply suppression to listings
today=datetime.date.today()
removedByHard=0
removedByAuto=0
kept, archived = [], []
for job in listings:
    jid=(job.get("id") or job.get("jobId") or "").strip()
    url=norm_url(job.get("applyLink") or job.get("url") or "")
    title=norm_title(job.get("title") or "")
    dom=domain_of(url)
    drop_reason=None
    meta=None

    # Hard report
    if jid and jid in hard_ids: drop_reason="reported_hard"; meta=hard_meta.get(jid)
    elif url and url in hard_urls: drop_reason="reported_hard"; meta=hard_meta.get(url)
    elif title and title in hard_titles: drop_reason="reported_hard"; meta=hard_meta.get(title)

    # Auto reasons from current rules (only if not already hard-dropped)
    if not drop_reason:
        reason=infer_reason(job)
        if (dom in rules.get("blacklistedDomains", [])) or (reason["code"] in set(rules.get("autoRemoveReasons", []))):
            drop_reason = f"auto_{reason['code']}"

    if drop_reason:
        job.setdefault("flags",{})["removed_reason"]=drop_reason
        if meta:
            job["flags"]["evidence_url"]=meta.get("evidenceUrl")
            job["flags"]["note"]=meta.get("note")
        archived.append(job)
        if drop_reason=="reported_hard": removedByHard+=1
        else: removedByAuto+=1
    else:
        kept.append(job)

# 6) Write outputs
transparency = raw_data.get("transparencyInfo") or {}
transparency["totalListings"]=len(kept)
transparency["lastUpdated"]=datetime.datetime.utcnow().isoformat()+"Z"
transparency["removedByHardReports"]=removedByHard
transparency["removedByAutoReasons"]=removedByAuto
transparency["autoRemoveReasons"]=rules.get("autoRemoveReasons", [])
transparency["blacklistedDomains"]=rules.get("blacklistedDomains", [])

data={
  "jobListings": kept,
  "archivedListings": archived,
  "transparencyInfo": transparency
}
write_json("data.json", data)

# health mirrors transparency for quick consumption
health={"ok": True, "checkedAt": datetime.datetime.utcnow().isoformat()+"Z", **transparency}
write_json("health.json", health)

# learn and rules snapshots
write_json("learn.json", {
  "reasons": dict(learn_tally["reasons"]),
  "rightReasons": dict(learn_tally["rightReasons"]),
  "wrongReasons": dict(learn_tally["wrongReasons"]),
  "byDomainRight": dict(learn_tally["byDomainRight"]),
  "byDomainWrong": dict(learn_tally["byDomainWrong"]),
  "generatedAt": datetime.datetime.utcnow().isoformat()+"Z"
})
write_json("rules.json", rules)
