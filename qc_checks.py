# qc_checks.py — single-source post-generation data validation
# Purpose:
#  - Validate final data.json produced by qc_and_learn.py
#  - Fail fast with clear messages (non-zero exit)
#  - Never mutate files; only read and report

import json, sys, pathlib
from urllib.parse import urlparse
from datetime import datetime, date

def is_http_url(u:str)->bool:
    if not u: return False
    try:
        p = urlparse(u)
        return p.scheme in ("http","https") and bool(p.netloc)
    except: 
        return False

def parse_date_any(s:str):
    # Minimal, dependency-free date parser supporting common formats
    # Returns None if cannot parse
    if not s or s.strip().upper()=="N/A": return None
    s = s.strip()
    fmts = ("%Y-%m-%d","%d/%m/%Y","%d-%m-%Y","%d %B %Y","%d %b %Y")
    for f in fmts:
        try:
            return datetime.strptime(s, f).date()
        except:
            pass
    return None

def main():
    path = pathlib.Path("data.json")
    if not path.exists():
        print("qc: data.json missing")
        sys.exit(2)

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"qc: data.json is not valid JSON: {e}")
        sys.exit(2)

    listings = data.get("jobListings", [])
    archived = data.get("archivedListings", [])
    tinfo = data.get("transparencyInfo", {})

    problems = []
    seen_ids = set()
    today = date.today()

    # Basic shape checks
    if not isinstance(listings, list):
        problems.append("jobListings must be a list")
    if not isinstance(archived, list):
        problems.append("archivedListings must be a list")

    # Validate active listings
    for i, rec in enumerate(listings):
        rid = rec.get("id")
        if not rid:
            problems.append(f"[active #{i}] missing id")
        elif rid in seen_ids:
            problems.append(f"[active #{i}] duplicate id: {rid}")
        else:
            seen_ids.add(rid)

        title = (rec.get("title") or "").strip()
        if len(title) < 6:
            problems.append(f"[{rid}] short title: {title!r}")

        al, pl = rec.get("applyLink"), rec.get("pdfLink")
        if not (is_http_url(al) or is_http_url(pl)):
            problems.append(f"[{rid}] neither applyLink nor pdfLink is a valid URL")

        # Deadline sanity: allow N/A, else must be today or future
        dl = rec.get("deadline")
        if dl and dl.strip().upper() != "N/A":
            d = parse_date_any(dl)
            if not d:
                problems.append(f"[{rid}] invalid deadline format: {dl}")
            elif d < today:
                problems.append(f"[{rid}] deadline in past but still active: {dl}")

        src = rec.get("source")
        if src not in ("official","aggregator"):
            problems.append(f"[{rid}] invalid source: {src}")

        typ = rec.get("type")
        if typ not in ("VACANCY","UPDATE"):
            problems.append(f"[{rid}] invalid type: {typ}")

    # Archived listings minimal checks (optional but helpful)
    for i, rec in enumerate(archived):
        rid = rec.get("id") or f"(archived-idx:{i})"
        rr = rec.get("flags", {}).get("removed_reason")
        if not rr:
            problems.append(f"[{rid}] archived without removed_reason")

    # Transparency consistency
    if "totalListings" in tinfo and isinstance(tinfo["totalListings"], int):
        if tinfo["totalListings"] != len(listings):
            problems.append(f"transparencyInfo.totalListings={tinfo['totalListings']} but jobListings count={len(listings)}")

    # Final report
    if problems:
        print("qc: FAIL")
        for msg in problems:
            print(" -", msg)
        sys.exit(1)

    print(f"qc: OK (active={len(listings)}, archived={len(archived)})")
    sys.exit(0)

if __name__ == "__main__":
    main()
