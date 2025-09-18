# qc_checks.py — validate final data.json (schema v1.3 compatible)
import json, sys, pathlib
from urllib.parse import urlparse
from datetime import datetime, date

def is_http_url(u):
  if not u: return False
  try:
    p=urlparse(u); return p.scheme in ("http","https") and bool(p.netloc)
  except: return False

def parse_date_any(s):
  if not s or s.strip().upper()=="N/A": return None
  s=s.strip()
  for f in ("%Y-%m-%d","%d/%m/%Y","%d-%m-%Y","%d %B %Y","%d %b %Y"):
    try: return datetime.strptime(s,f).date()
    except: pass
  return None

def main():
  p = pathlib.Path("data.json")
  if not p.exists(): print("qc: data.json missing"); sys.exit(2)
  try:
    data=json.loads(p.read_text(encoding="utf-8"))
  except Exception as e:
    print(f"qc: invalid JSON: {e}"); sys.exit(2)

  listings=data.get("jobListings", []); archived=data.get("archivedListings", []); tinfo=data.get("transparencyInfo", {})
  problems=[]; seen=set(); today=date.today()

  if not isinstance(listings,list): problems.append("jobListings must be list")
  if not isinstance(archived,list): problems.append("archivedListings must be list")

  for i, rec in enumerate(listings):
    rid=rec.get("id")
    if not rid: problems.append(f"[active #{i}] missing id")
    elif rid in seen: problems.append(f"[active #{i}] duplicate id: {rid}")
    else: seen.add(rid)
    title=(rec.get("title") or "").strip()
    if len(title)<6: problems.append(f"[{rid}] short title")
    al, pl = rec.get("applyLink"), rec.get("pdfLink")
    if not (is_http_url(al) or is_http_url(pl)): problems.append(f"[{rid}] invalid URLs")
    dl=rec.get("deadline")
    if dl and dl.strip().upper()!="N/A":
      d=parse_date_any(dl)
      if not d: problems.append(f"[{rid}] invalid deadline format: {dl}")
    src=rec.get("source")
    if src not in ("official","aggregator"): problems.append(f"[{rid}] invalid source: {src}")
    typ=rec.get("type")
    if typ not in ("VACANCY","UPDATE"): problems.append(f"[{rid}] invalid type: {typ}")

  for i, rec in enumerate(archived):
    rr=rec.get("flags",{}).get("removed_reason")
    if not rr: problems.append(f"[archived #{i}] missing removed_reason")

  if isinstance(tinfo.get("totalListings"),int) and tinfo["totalListings"]!=len(listings):
    problems.append("transparencyInfo.totalListings mismatch")

  if problems:
    print("qc: FAIL"); [print(" -",m) for m in problems]; sys.exit(1)
  print(f"qc: OK (active={len(listings)}, archived={len(archived)})"); sys.exit(0)

if __name__=="__main__": main()
