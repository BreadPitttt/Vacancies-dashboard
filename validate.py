import json, sys
from jsonschema import validate, Draft202012Validator

with open("schema.json", encoding="utf-8") as f:
    schema = json.load(f)
with open("data.json", encoding="utf-8") as f:
    data = json.load(f)

errors = sorted(Draft202012Validator(schema).iter_errors(data), key=lambda e: e.path)
if errors:
    for e in errors:
        print(f"Schema error at /{'/'.join(map(str,e.path))}: {e.message}")
    sys.exit(1)
print("schema: ok")
