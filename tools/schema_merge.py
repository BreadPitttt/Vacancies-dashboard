#!/usr/bin/env python3
# Merge with fuzzy title normalization and stable sorting
import json, sys, re, hashlib
from datetime import datetime

def norm_spaces(s): return re.sub(r"\s+"," ", (s or "").strip())

def fuzzy_title(s):
    s = (s or "").lower()
    s = re.sub(r"[\(\)\[\]\{\}]", " ", s)
    s = re.sub(r"[^\w\s/:-]", " ", s)  # keep slashes and hyphens for advt no
    s = re.sub(r"\b(notice|notification|advertisement|advt|recruitment|online\s*form|apply\s*online)\b", " ", s)
    s = re.sub(r"\b(corrigendum|extension|extended|addendum|amendment|revised|rectified)\b", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def norm_date(s):
    s = (s or "").strip()
    for fmt in ("%d/%m/%Y","%d-%m-%Y","%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).strftime("%d/%m/%Y")
        except Exception:
            pass
    return s if s else "N/A"

def make_key(item):
    title = fuzzy_title(item.get("title",""))
    link  = (item.get("detailLink") or item.get("applyLink") or "").lower()
    link  = re.sub(r"[?#].*$", "", link)
    date  = norm_date(item.get("deadline","")).lower()
    raw = f"{title}|{link}|{date}"
    return hashlib.sha1(raw.encode()).hexdigest()[:16]

def compute_days_left(deadline_ddmmyyyy):
    try:
        d = datetime.strptime(deadline_ddmmyyyy, "%d/%m/%Y")
        return max((d.date() - datetime.utcnow().date()).days, 0)
    except Exception:
        return None

def validate(i):
    out = {
        "id": i.get("id") or ("src_" + make_key(i)),
        "title": norm_spaces(i.get("title")),
        "organization": norm_spaces(i.get("organization") or ""),
        "qualificationLevel": norm_spaces(i.get("qualificationLevel") or ""),
        "domicile": norm_spaces(i.get("domicile") or ""),
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
            ex = idx[k]
            for f in ["organization","qualificationLevel","domicile","deadline","applyLink","detailLink","source","type"]:
                if v.get(f) and (not ex.get(f) or ex.get(f)=="N/A"):
                    ex[f] = v[f]
            ex["flags"] = { **(ex.get("flags") or {}), **(v.get("flags") or {}) }
            if v.get("daysLeft") is not None:
                ex["daysLeft"] = v["daysLeft"]
        else:
            existing.append(v); idx[k]=v; added += 1
    def sort_key(it):
        dd = it.get("deadline","N/A")
        try:
            dt = datetime.strptime(dd,"%d/%m/%Y")
            return (0, dt, it.get("title",""))
        except Exception:
            return (1, datetime.max, it.get("title",""))
    existing.sort(key=sort_key)
    return existing, added

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python tools/schema_merge.py data.json tmp/candidates.jsonl data.json")
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
