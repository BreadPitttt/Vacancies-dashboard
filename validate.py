# validate.py — JSON Schema validation for data.json with enum-safe 'source' and optional 'verifiedBy'

import json, sys, pathlib
from jsonschema import Draft202012Validator

SCHEMA = {
  "type": "object",
  "required": ["jobListings", "transparencyInfo"],
  "properties": {
    "jobListings": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["id","slug","title","organization","qualificationLevel","domicile","source","type","extractedAt"],
        "properties": {
          "id": {"type":"string"},
          "slug": {"type":"string"},
          "title": {"type":"string"},
          "organization": {"type":"string"},
          "qualificationLevel": {"type":"string"},
          "domicile": {"type":"string"},
          "source": {"type":"string","enum":["official","aggregator"]},  # keep enum strict. [3]
          "verifiedBy": {"type":"string","enum":["official","multi-aggregator","single-aggregator"]},  # optional confidence. [3]
          "type": {"type":"string","enum":["VACANCY","UPDATE"]},
          "updateSummary": {"type":["string","null"]},
          "relatedTo": {"type":["string","null"]},
          "deadline": {"type":["string","null"]},
          "applyLink": {"type":["string","null"]},
          "pdfLink": {"type":["string","null"]},
          "extractedAt": {"type":"string"}
        },
        "additionalProperties": True
      }
    },
    "transparencyInfo": {
      "type": "object",
      "required": ["lastUpdated","totalListings"],
      "properties": {
        "lastUpdated": {"type":"string"},
        "totalListings": {"type":"number"},
        "aggCounts": {"type":"object"},
        "officialCounts": {"type":"object"},   # new telemetry accepted. [3]
        "telegramCounts": {"type":"object"},
        "pendingFromTelegram": {"type":"array","items":{"type":"string"}},
        "notes": {"type":"string"}
      },
      "additionalProperties": True
    }
  },
  "additionalProperties": True
}

def main():
  p = pathlib.Path("data.json")
  data = json.loads(p.read_text(encoding="utf-8"))
  v = Draft202012Validator(SCHEMA)
  errors = sorted(v.iter_errors(data), key=lambda e: e.path)
  if errors:
    for e in errors:
      print(f"Schema error at {'/'.join([str(x) for x in e.path])}: {e.message}")
    sys.exit(1)
  print("Schema OK")

if __name__ == "__main__":
  main()
