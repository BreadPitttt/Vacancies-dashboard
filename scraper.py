# scraper.py — production-ready: stable IDs, explicit decoding, shorter timeouts,
# retry-hardening, non-HTML skip, per-source caps, and 8-thread concurrent fetch.
# All try blocks have matching except blocks to avoid SyntaxError.

import json, re, hashlib, threading
from pathlib import Path
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin, urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from bs4 import BeautifulSoup, UnicodeDammit
import dateparser
import urllib.robotparser as robotparser
from concurrent.futures import ThreadPoolExecutor, as_completed

DATA_PATH = Path("data.json")
UTC_NOW = datetime.now(timezone.utc)

# Tunables
CONNECT_TO = 6          # seconds (connect)
READ_TO    = 12         # seconds (read)
LIST_TO    = (CONNECT_TO, READ_TO)
DETAIL_TO  = (CONNECT_TO, READ_TO)
HEAD_TO    = 8
MAX_WORKERS = 8
PER_SOURCE_MAX = 120     # bound total work per source
