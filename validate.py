import json, sys
from jsonschema import validate, Draft202012Validator

schema = json.load(open("schema.json", encoding="utf-8"))
data = json.load(open("data.json", encoding="utf-8"))

errors = sorted(Draft202012Validator(schema).iter_errors(data), key=lambda e: e.path)
if errors:
    for e in errors:
        print(f"Schema error at /{'/'.join(map(str,e.path))}: {e.message}")
    sys.exit(1)
print("schema: ok")
