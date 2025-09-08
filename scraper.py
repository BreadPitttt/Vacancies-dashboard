# Minimal nightly script:
# - stamps transparencyInfo.lastUpdated
# - leaves hooks to add official sources, signature validation (pyHanko), and table extraction (pdfplumber)
import json, time, pathlib

# Add sources here later (official pages/PDFs)
OFFICIAL_SOURCES = []  # e.g., [{"title":"...", "pdf":"https://...", "apply":"https://..."}]

def main():
    # Load existing JSON if present
    p = pathlib.Path("data.json")
    data = {"jobListings": [], "transparencyInfo": {}}
    if p.exists():
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            pass

    # TODO: fetch OFFICIAL_SOURCES, validate PDFs with pyHanko, and parse tables with pdfplumber
    # Hooks:
    #   - pyHanko signature check (see docs): validate signer trust + integrity before adding details
    #   - pdfplumber: extract exam pattern tables (sections/marks/duration) if needed

    # Update freshness
    data.setdefault("transparencyInfo", {})
    data["transparencyInfo"]["lastUpdated"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    # Save
    p.write_text(json.dumps(data, indent=2), encoding="utf-8")
    print("Updated data.json at", data["transparencyInfo"]["lastUpdated"])

if __name__ == "__main__":
    main()
