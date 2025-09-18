# qc_and_learn.py — generator with learning, update-merging, and modes
import json, pathlib, datetime, re, urllib.parse, argparse
from collections import Counter

def P(p): return pathlib.Path(p)
def JLOAD(p, default):
    try:
        if P(p).exists(): return json.loads(P(p).read_text(encoding="utf-8"))
    except: pass
    return default
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

def norm_url(u):
    try:
        p=urllib.parse.urlparse(u or "")
        q=urllib.parse.parse_qsl(p.query, keep_blank_values=True)
        q=[(k,v) for (k,v) in q if k.lower() not in ("utm_source","utm_medium","utm_campaign","utm_term","utm_content","ref")]
        p=p._replace(query=urllib.parse.urlencode(q))
        return urllib.parse.urlunparse(p).rstrip("/").lower()
    except: return (u or "").rstrip("/").lower()
def norm_title(t): return re.sub(r"\s+"," ", (t or "").strip().lower())
def today(): return datetime.date.today()

ap = argparse.ArgumentParser()
ap.add_argument("--mode", default="nightly")
RUN_MODE = (ap.parse_args().mode or "nightly").lower()

raw = JLOAD("data.json", {"jobListings": [], "transparencyInfo": {}})
listings = list(raw.get("jobListings") or [])
reports = JLOADL("reports.jsonl")
votes   = JLOADL("votes.jsonl")
subs    = JLOADL("submissions.jsonl")
rules   = JLOAD("rules.json", {"blacklistedDomains": [], "autoRemoveReasons": ["expired"], "minQualification": None, "captureHints": []})

if RUN_MODE == "weekly":
    rules["blacklistedDomains"] = []
    rules["autoRemoveReasons"] = ["expired"]

# Accept official-like missing submissions and learn captureHints
OFFICIAL_WHITELIST = {"ssc.nic.in","ssc.gov.in","upsconline.gov.in","upsconline.nic.in","ibps.in","bpsc.bih.nic.in","bpsc.bihar.gov.in","opportunities.rbi.org.in","dsssb.delhi.gov.in","bssc.bihar.gov.in","onlinebssc.com","rrbcdg.gov.in","rrbapply.gov.in","ccras.nic.in"}
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

# -------- Corrigendum/Extension merger (conservative) --------
UPDATE_TOKENS = ["corrigendum","amendment","addendum","extension","extended","notice","revised","rectified"]
ADV_RE = re.compile(r"(advt|advertisement|notice)\s*(no\.?|number)?\s*[:\-]?\s*([A-Za-z0-9\/\-\._]+)", re.I)

def is_update_title(t):
    tl = (t or "").lower()
    return any(tok in tl for tok in UPDATE_TOKENS)

def url_root(u):
    try:
        p = urllib.parse.urlparse(u or "")
        base = p._replace(query="", fragment="")
        path = base.path.rsplit("/",1)[0] if "/" in (base.path or "") else (base.path or "/")
        return f"{base.scheme}://{base.netloc}{path}"
    except:
        return u or ""

def pdf_base(u):
    try:
        p = urllib.parse.urlparse(u or ""); fn = (p.path or "").rsplit("/",1)[-1]
        fn = re.sub(r"(?i)(corrigendum|addendum|amendment|notice|extension|revised|rectified)","", fn)
        fn = re.sub(r"[\-_\.]?(v\d+|ver\d+|\d{1,2}[-_]\d{1,2}[-_]\d{2,4}|\d{8})","", fn)
        return fn.strip().lower()
    except:
        return ""

def adv_no(s):
    m = ADV_RE.search(s or "")
    return (m.group(3).strip().lower() if m else "")

def salient_tokens(t):
    t = re.sub(r"[\W_]+"," ", (t or "").lower())
    bad = set(["advt","advertisement","notice","recruitment","vacancy","post","posts","exam","notification","apply","online","last","date","extended","extension","corrigendum","addendum","amendment","revised","rectified","to","for","of","and","the"])
    toks = [w for w in t.split() if len(w)>3 and w not in bad]
    return set(toks[:6])

def parent_score(update, parent):
    s = 0.0
    uurl = norm_url(update.get("applyLink") or update.get("url") or "")
    purl = norm_url(parent.get("applyLink") or parent.get("url") or "")
    if uurl and purl and url_root(uurl) == url_root(purl): s += 0.5
    if pdf_base(uurl) and pdf_base(uurl) == pdf_base(purl): s += 0.3
    if any(tok in (update.get("title","").lower()) for tok in UPDATE_TOKENS): s += 0.2
    ua, pa = adv_no(update.get("title","")), adv_no(parent.get("title",""))
    if ua and pa and ua == pa: s += 0.3
    if parent.get("organization") and parent["organization"] in (update.get("title","").upper()): s += 0.1
    if len(salient_tokens(update.get("title")) & salient_tokens(parent.get("title"))) >= 2: s += 0.1
    return min(1.0, s)

def has_hard_anchor(update, parent):
    uurl = norm_url(update.get("applyLink") or update.get("url") or "")
    purl = norm_url(parent.get("applyLink") or parent.get("url") or "")
    if uurl and purl and url_root(uurl) == url_root(purl): return True
    ua, pa = adv_no(update.get("title","")), adv_no(parent.get("title",""))
    if ua and pa and ua == pa: return True
    if pdf_base(uurl) and pdf_base(uurl) == pdf_base(purl): return True
    return False

# Index potential parents (non-update titles)
parents = []
for j in listings:
    if not is_update_title(j.get("title")):
        parents.append(j)

def try_merge_updates(items, parents):
    merged = 0
    kept = []
    for j in items:
        t = j.get("title") or ""
        if not is_update_title(t):
            kept.append(j); continue
        # score each parent
        best, best_s = None, 0.0
        for p in parents:
            s = parent_score(j, p)
            if s > best_s:
                best_s, best = s, p
        if best and best_s >= 0.6 and has_hard_anchor(j, best):
            # attach update to parent
            best.setdefault("updates", []).append({
                "title": t,
                "link": j.get("applyLink") or j.get("url"),
                "capturedAt": datetime.datetime.utcnow().isoformat()+"Z"
            })
            # naive deadline extend detection
            m = re.search(r"(\d{1,2}[-/]\d{1,2}[-/]\d{2,4}|\d{1,2}\s+[A-Za-z]{3,9}\s+\d{2,4})", t)
            if m:
                best["deadline"] = m.group(1)
            merged += 1
        else:
            # keep as separate UPDATE with flag, so no fresh vacancy is lost
            j["type"] = "UPDATE"
            j.setdefault("flags",{})["no_parent_found"]=True
            kept.append(j)
    return kept, merged

listings, merged_updates = try_merge_updates(listings, parents)

# ---------------- Hard reports ----------------
hard_ids, hard_urls, hard_titles, hard_meta = set(), set(), set(), {}
for r in reports:
    if r.get("type")=="report":
        jid=(r.get("jobId") or r.get("listingId") or "").strip()
        if jid: hard_ids.add(jid); hard_meta[jid]={"evidenceUrl": r.get("url") or r.get("evidenceUrl"), "note": r.get("note")}
        if r.get("url"):
            u=norm_url(r.get("url")); hard_urls.add(u); hard_meta[u]={"evidenceUrl": r.get("url"), "note": r.get("note")}
        if r.get("title"):
            t=norm_title(r.get("title")); hard_titles.add(t); hard_meta[t]={"evidenceUrl": r.get("url") or r.get("evidenceUrl"), "note": r.get("note")}

# ---------------- Learn from votes (unchanged) ----------------
def domain_of(u):
    try: return urllib.parse.urlparse(u or "").hostname or ""
    except: return ""
learn, rightR, wrongR, domR, domW = Counter(), Counter(), Counter(), Counter(), Counter()
def infer_reason(listing):
    t=(listing.get("title") or "").lower()
    u=norm_url(listing.get("applyLink") or listing.get("url") or "")
    dom=urllib.parse.urlparse(u).hostname or ""
    qual=(listing.get("qualificationLevel") or "").lower()
    if "10th" in qual or "12th" in qual: return {"code":"eligibility_low","details":qual}
    if not listing.get("organization") or not listing.get("deadline"):
        return {"code":"incomplete_extraction","details":"missing org/deadline"}
    return {"code":"unknown","details":dom}

for v in votes:
    if v.get("type")!="vote" or v.get("vote") not in ("right","wrong"): continue
    # simplistic learning bucket
    reason = infer_reason({"title": v.get("title",""), "url": v.get("url",""), "deadline": None, "qualificationLevel": None})
    code = reason["code"]
    learn[code]+=1
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

# ---------------- Suppression & output ----------------
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

    if drop:
        job.setdefault("flags",{})["removed_reason"]=drop
        if meta:
            job["flags"]["evidence_url"]=meta.get("evidenceUrl"); job["flags"]["note"]=meta.get("note")
        archived.append(job)
    else:
        kept.append(job)

transp = raw.get("transparencyInfo") or {}
transp.update({
  "totalListings": len(kept),
  "lastUpdated": datetime.datetime.utcnow().isoformat()+"Z",
  "runMode": RUN_MODE,
  "addedFromMissing": added_from_missing,
  "mergedUpdates": merged_updates,
  "autoRemoveReasons": rules.get("autoRemoveReasons", []),
  "blacklistedDomains": rules.get("blacklistedDomains", [])
})

data={"jobListings": kept, "archivedListings": archived, "transparencyInfo": transp}
JWRITE("data.json", data)
JWRITE("health.json", {"ok": True, "checkedAt": datetime.datetime.utcnow().isoformat()+"Z", **transp})
JWRITE("learn.json", {
  "updateTokens": UPDATE_TOKENS,
  "mergedUpdates": merged_updates,
  "rightReasons": dict(rightR),
  "wrongReasons": dict(wrongR),
  "byDomainRight": dict(domR),
  "byDomainWrong": dict(domW),
  "generatedAt": datetime.datetime.utcnow().isoformat()+"Z",
  "runMode": RUN_MODE
})
JWRITE("rules.json", rules)
