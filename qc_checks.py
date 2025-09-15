import json, sys, requests, dateparser
from datetime import datetime, timezone

U = datetime.now(timezone.utc).date()

def head_ok(url):
    try:
        r = requests.head(url, timeout=20, allow_redirects=True)
        if r.status_code in (405, 501):
            r = requests.get(url, timeout=20, stream=True)
        return 200 <= r.status_code < 300
    except Exception:
        return False

data = json.load(open("data.json", encoding="utf-8"))

seen = set()
bad = 0
for j in data.get("jobListings", []):
    if j["id"] in seen:
        print("duplicate id:", j["id"]); bad += 1
    seen.add(j["id"])
    dl = j.get("deadline")
    if dl:
        dt = dateparser.parse(dl)
        if dt and dt.date() < U:
            print("expired listing still active:", j["id"], dl); bad += 1
    for k in ("applyLink","pdfLink"):
        v = j.get(k)
        if v and not head_ok(v):
            print("bad link:", k, v); bad += 1

if bad:
    sys.exit(1)
print("qc: ok")
