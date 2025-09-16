# qc_checks.py — pragmatic data sanity checks with clear exit codes

import json, sys, pathlib
from urllib.parse import urlparse
from datetime import datetime
import dateparser

def is_http_url(u):
    if not u: return False
    try:
        p = urlparse(u)
        return p.scheme in ("http","https") and bool(p.netloc)
    except: return False

def main():
    p = pathlib.Path("data.json")
    if not p.exists():
        print("qc: data.json missing")
        sys.exit(2)

    data = json.loads(p.read_text(encoding="utf-8"))
    listings = data.get("jobListings", [])

    problems = []
    seen_ids = set()
    today = datetime.utcnow().date()

    for i, rec in enumerate(listings):
        rid = rec.get("id")
        if not rid:
            problems.append(f"[rec#{i}] missing id")
        elif rid in seen_ids:
            problems.append(f"[rec#{i}] duplicate id: {rid}")
        else:
            seen_ids.add(rid)

        title = rec.get("title","").strip()
        if len(title) < 6:
            problems.append(f"[{rid}] short title")

        al, pl = rec.get("applyLink"), rec.get("pdfLink")
        if not (is_http_url(al) or is_http_url(pl)):
            problems.append(f"[{rid}] neither applyLink nor pdfLink is a valid URL")

        dl = rec.get("deadline")
        if dl:
            try:
                d = dateparser.parse(dl, settings={"DATE_ORDER":"DMY"}).date()
                if d < today:
                    problems.append(f"[{rid}] deadline in past: {dl}")
            except Exception:
                problems.append(f"[{rid}] invalid deadline format: {dl}")

        src = rec.get("source")
        if src not in ("official","aggregator"):
            problems.append(f"[{rid}] invalid source: {src}")

        typ = rec.get("type")
        if typ not in ("VACANCY","UPDATE"):
            problems.append(f"[{rid}] invalid type: {typ}")

    if problems:
        print("qc: FAIL")
        for msg in problems:
            print(" -", msg)
        sys.exit(1)

    print(f"qc: OK ({len(listings)} records)")
    sys.exit(0)

if __name__ == "__main__":
    main()
