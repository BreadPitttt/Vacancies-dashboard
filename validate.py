# validate.py — MODIFIED: Now includes strict 'format' validation for date and date-time strings.

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
          "source": {"type":"string", "enum":["official","aggregator"]},
          "verifiedBy": {"type":"string", "enum":["official","multi-aggregator","single-aggregator"]},
          "type": {"type":"string", "enum":["VACANCY","UPDATE"]},
          "updateSummary": {"type":["string","null"]},
          "relatedTo": {"type":["string","null"]},
          # CHANGE: Added "format": "date". This checks if the string is a valid date (e.g., "2025-10-20").
          "deadline": {"type":["string","null"], "format": "date"},
          "applyLink": {"type":["string","null"]},
          "pdfLink": {"type":["string","null"]},
          # CHANGE: Added "format": "date-time". This checks for a full timestamp (e.g., "2025-09-16T10:36:00Z").
          "extractedAt": {"type":"string", "format": "date-time"}
        },
        "additionalProperties": True
      }
    },
    "transparencyInfo": {
      "type": "object",
      "required": ["lastUpdated","totalListings"],
      "properties": {
        # CHANGE: Also added "format": "date-time" here for consistency.
        "lastUpdated": {"type":"string", "format": "date-time"},
        "totalListings": {"type":"number"},
        "aggCounts": {"type":"object"},
        "officialCounts": {"type":"object"},
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
  if not p.exists():
    print(f"Validation failed: {p} not found.")
    sys.exit(1)
    
  try:
    data = json.loads(p.read_text(encoding="utf-8"))
  except json.JSONDecodeError as e:
    print(f"Validation failed: {p} is not valid JSON. Error: {e}")
    sys.exit(1)

  v = Draft202012Validator(SCHEMA)
  errors = sorted(v.iter_errors(data), key=lambda e: e.path)
  
  if errors:
    print(f"Validation failed with {len(errors)} schema errors:")
    for e in errors:
      # This provides a more readable path to the error.
      path = " -> ".join([str(x) for x in e.path])
      print(f"- At path '{path}': {e.message}")
    sys.exit(1)
    
  print("Schema OK. data.json is valid.")

if __name__ == "__main__":
  main()
