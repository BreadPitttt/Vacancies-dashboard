import json, sys, re, requests, dateparser
from datetime import datetime, timezone

U = datetime.now(timezone.utc).date()
ALLOWED_SKILLS = ("typing","computer","pet","pst","physical","ms office","ccc","dca")

def head_ok(url):
    try:
        r = requests.head(url, timeout=20, allow_redirects=True)
        if r.status_code in (405, 501):  # HEAD not allowed -> try GET
            r = requests.get(url, timeout=20, stream=True)
        return 200 <= r.status_code < 300
    except Exception:
        return False

data = json.load(open("data.json", encoding="utf-8"))

seen = set()
bad = 0
for j in data.get("jobListings", []):
    # 1) unique ID
    if j["id"] in seen:
        print("duplicate id:", j["id"]); bad += 1
    seen.add(j["id"])

    # 2) active items shouldn't be expired
    dl = j.get("deadline")
    if dl:
        dt = dateparser.parse(dl)
        if dt and dt.date() < U:
            print("expired listing still active:", j["id"], dl); bad += 1

    # 3) links reachable if present
    for k in ("applyLink","pdfLink"):
        v = j.get(k)
        if v and not head_ok(v):
            print("bad link:", k, v); bad += 1

# 4) allowed skills only if mentioned (simple text flag; scraper enforces real logic)
# Implemented in scraper; this is a placeholder guard.

if bad:
    sys.exit(1)
print("qc: ok")
