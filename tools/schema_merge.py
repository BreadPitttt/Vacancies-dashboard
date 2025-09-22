#!/usr/bin/env python3
import json, sys, re, hashlib
from datetime import datetime

def norm(s): return re.sub(r"\s+"," ", (s or "").strip())
def norm_date(s):
    s = (s or "").strip()
    for fmt in ("%d/%m/%Y","%d-%m-%Y","%Y-%m-%d"):
        try:
            from datetime import datetime
            return datetime.strptime(s, fmt).strftime("%d/%m/%Y")
        except Exception:
            pass
    return s

def make_key(item):
    title = norm(item.get("title","")).lower()
    link  = (item.get("detailLink") or item.get("applyLink") or "").lower()
    date  = norm_date(item.get("deadline","")).lower()
    raw = f"{title}|{link}|{date}"
    return hashlib.sha1(raw.encode()).hexdigest()[:16]

def compute_days_left(deadline_ddmmyyyy):
    try:
        d = datetime.strptime(deadline_ddmmyyyy, "%d/%m/%Y")
        left = (d.date() - datetime.utcnow().date()).days
        return max(left, 0)
    except Exception:
        return None

def validate(i):
    out = {
        "id": i.get("id") or ("src_" + make_key(i)),
        "title": norm(i.get("title")),
        "organization": norm(i.get("organization") or ""),
        "qualificationLevel": norm(i.get("qualificationLevel") or ""),
        "domicile": norm(i.get("domicile") or ""),
        "deadline": norm_date(i.get("deadline") or ""),
        "applyLink": (i.get("applyLink") or "").strip(),
        "detailLink": (i.get("detailLink") or "").strip(),
        "source": i.get("source") or "official",
        "type": i.get("type") or "VACANCY",
        "flags": i.get("flags") or {},
    }
    dl = compute_days_left(out["deadline"])
    if dl is not None:
        out["daysLeft"] = dl
    return out

def merge(existing, candidates):
    idx = { make_key(x): x for x in existing }
    added = 0
    for raw in candidates:
        v = validate(raw)
        k = make_key(v)
        if k in idx:
            # shallow enrich without changing id
            ex = idx[k]
            for f in ["organization","qualificationLevel","domicile","deadline","applyLink","detailLink","source","type"]:
                if v.get(f) and (not ex.get(f) or ex.get(f)=="N/A"):
                    ex[f] = v[f]
            ex["flags"] = { **(ex.get("flags") or {}), **(v.get("flags") or {}) }
            if v.get("daysLeft") is not None:
                ex["daysLeft"] = v["daysLeft"]
        else:
            existing.append(v)
            idx[k] = v
            added += 1
    # sort soonest deadline first; N/A goes to bottom
    def sort_key(it):
        dd = it.get("deadline","")
        try:
            dt = datetime.strptime(dd,"%d/%m/%Y")
            return (0, dt, it.get("title",""))
        except Exception:
            return (1, datetime.max, it.get("title",""))
    existing.sort(key=sort_key)
    return existing, added

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python tools/schema_merge.py data.json candidates.jsonl out.json")
        sys.exit(2)
    data_path, cand_path, out_path = sys.argv[1], sys.argv[2], sys.argv[3]
    data = json.load(open(data_path,"r",encoding="utf-8"))
    existing = data.get("jobListings") or []
    cands = [json.loads(line) for line in open(cand_path,"r",encoding="utf-8") if line.strip()]
    merged, added = merge(existing, cands)
    data["jobListings"] = merged
    data.setdefault("archivedListings", data.get("archivedListings") or [])
    data.setdefault("sections", data.get("sections") or {"applied":[],"other":[],"primary":[]})
    data.setdefault("transparencyInfo", {})
    data["transparencyInfo"]["totalListings"] = len(merged)
    json.dump(data, open(out_path,"w",encoding="utf-8"), indent=2, ensure_ascii=False)
    print(json.dumps({"added":added}))
