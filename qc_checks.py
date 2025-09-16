import json, sys, requests, dateparser
from datetime import datetime, timezone
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

U = datetime.now(timezone.utc).date()

def session_with_retries():
    s = requests.Session()
    retry = Retry(total=3, connect=3, read=3, backoff_factor=0.5,
                  status_forcelist=[429,500,502,503,504], allowed_methods={"GET","HEAD"})
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("http://", adapter); s.mount("https://", adapter)
    return s

HTTP = session_with_retries()

def head_ok(url):
    try:
        r = HTTP.head(url, timeout=10, allow_redirects=True)
        if r.status_code in (405, 501):
            r = HTTP.get(url, timeout=10, stream=True)
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
