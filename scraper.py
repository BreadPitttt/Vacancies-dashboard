# scraper.py — minimal nightly refresher with simple PDF validation and optional table extraction.
# Place this file at the repo root next to data.json. The workflow installs requirements and runs this script.

import json
import time
from pathlib import Path
import tempfile
import requests

# Optional table extraction (works for many simple PDFs)
import pdfplumber

# Simple signature integrity check using pyHanko
from pyhanko.pdf_utils.reader import PdfFileReader
from pyhanko.sign.validation import validate_pdf_signature
from pyhanko_certvalidator import ValidationContext

DATA_PATH = Path("data.json")

# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------

def load_data() -> dict:
    """Load existing data.json or return a fresh structure."""
    base = {"jobListings": [], "transparencyInfo": {}}
    if DATA_PATH.exists():
        try:
            base = json.loads(DATA_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    if "jobListings" not in base:
        base["jobListings"] = []
    if "transparencyInfo" not in base:
        base["transparencyInfo"] = {}
    return base

def save_data(data: dict) -> None:
    """Write data.json prettily."""
    DATA_PATH.write_text(json.dumps(data, indent=2), encoding="utf-8")

def stamp_last_updated(data: dict) -> None:
    """Set transparencyInfo.lastUpdated to the current UTC time."""
    data.setdefault("transparencyInfo", {})
    data["transparencyInfo"]["lastUpdated"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

def download_pdf(url: str) -> Path:
    """Download a PDF to a temp file and return its Path."""
    fd, tmp_name = tempfile.mkstemp(suffix=".pdf")
    Path(tmp_name).write_bytes(requests.get(url, timeout=30).raise_for_status() or requests.get(url, timeout=30).content)
    return Path(tmp_name)

def validate_pdf_simple(pdf_path: Path, require_trust: bool = False) -> bool:
    """
    Basic validation:
    - True only if a signature exists and the document bytes are intact.
    - If require_trust=True, also demand a trusted signer (configure trust roots first).
    """
    try:
        with pdf_path.open("rb") as f:
            reader = PdfFileReader(f)
            if not reader.embedded_signatures:
                return False
            # Integrity-only by default; add trust roots later if strict signer trust is required.
            vc = ValidationContext(trust_roots=[])
            status = validate_pdf_signature(reader.embedded_signatures[0], vc, skip_diff=True)
            if not status.intact:
                return False
            if require_trust and not status.trusted:
                return False
            return True
    except Exception:
        return False

def extract_first_table(pdf_path: Path):
    """
    Try to extract the first table found (list of rows). Returns None if none found.
    Keep it minimal for now.
    """
    try:
        with pdfplumber.open(str(pdf_path)) as pdf:
            for page in pdf.pages:
                tbl = page.extract_table()
                if tbl:
                    return tbl
    except Exception:
        pass
    return None

def safe_add_listing(data: dict, src: dict) -> None:
    """
    Download, validate, optionally parse a table, then append a listing
    only if the PDF passes validation.
    """
    try:
        pdf_path = download_pdf(src["pdf"])
        if not validate_pdf_simple(pdf_path):  # set require_trust=True later after adding trust roots
            print("PDF failed validation, skipping:", src["pdf"])
            return
        table = extract_first_table(pdf_path) or []
        listing = {
            "id": f'{src.get("organization","Org")}:{src.get("title","Title")}',
            "title": src.get("title", ""),
            "organization": src.get("organization", ""),
            "pdfLink": src.get("pdf", ""),
            "applyLink": src.get("apply", ""),
            "qualificationLevel": src.get("qualificationLevel", ""),
            "domicile": src.get("domicile", ""),
            "source": src.get("source", "official"),
            "deadline": src.get("deadline"),
            "parsedTableRows": len(table)
        }
        data["jobListings"].append(listing)
        print("Added listing:", listing["id"])
    except Exception as e:
        print("Error processing source:", e)

# -------------------------------------------------------------------
# Example source (replace with real official notice when ready)
# -------------------------------------------------------------------
EXAMPLE = {
    "title": "Sample Vacancy",
    "organization": "Example Dept.",
    "pdf": "https://example.com/notification.pdf",  # replace with a real official PDF when available
    "apply": "https://example.com/apply",
    "qualificationLevel": "Graduate",
    "domicile": "All India",
    "source": "official",
    "deadline": "2025-09-30"
}

# -------------------------------------------------------------------
# Main
# -------------------------------------------------------------------

def main():
    data = load_data()

    # Demo: try to add a single listing from EXAMPLE (safe no-op if PDF URL is placeholder or invalid).
    if EXAMPLE["pdf"].startswith("https://example.com/"):
        print("Example PDF URL is a placeholder; skipping add. Replace with a real official PDF to test.")
    else:
        safe_add_listing(data, EXAMPLE)

    # Always stamp freshness so the site visibly updates each run.
    stamp_last_updated(data)
    save_data(data)
    print("Updated data.json")

if __name__ == "__main__":
    main()
