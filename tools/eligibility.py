#!/usr/bin/env python3
import re

TEACHER = {"teacher","tgt","pgt","prt","faculty","lecturer","assistant professor","professor","b.ed","ctet","tet "}
TECH = {"b.tech","btech","b.e","m.tech","m.e","mca","bca","engineer","developer","scientist","architect","analyst","devops","cloud","ml","ai","research","nursing","pharmac","iti","polytechnic","diploma"}
PG = {"mba","pg ","post graduate","postgraduate","phd","m.phil","mcom","m.com","ma ","m.a","msc","m.sc","mca","mba"}

OPEN = {"all india","any state","open to all","pan india","indian citizens","across india","from any state","other state candidates","outside state"}
CLOSE = {"domicile","resident","locals only","local candidates","state quota","only for domicile"}

def clean(s): 
    import re
    return re.sub(r"\s+"," ", (s or "").strip())

def education_band(text):
    t=(text or "").lower()
    if any(k in t for k in ["10th","matric","ssc "]): return "10th pass"
    if any(k in t for k in ["12th","intermediate","hsc"]): return "12th pass"
    if "any graduate" in t or "any degree" in t or re.search(r"\bgraduate\b", t): return "Any graduate"
    return "N/A"

def allow_skills(text):
    t=(text or "").lower()
    # allow none or simple skills; disallow complex trades/certs
    bad = any(x in t for x in ["steno","shorthand","trade test","cad","sap","oracle","aws","azure","docker","kubernetes","tally erp"])
    return not bad

def allow_domicile(title):
    t=(title or "").lower()
    if "bihar" in t and not any(k in t for k in CLOSE): return True
    if any(k in t for k in OPEN): return True
    if "only for domicile" in t and "bihar" not in t: return False
    return True

def exclude_streams(text):
    t=(text or "").lower()
    if any(k in t for k in TEACHER): return True
    if any(k in t for k in TECH): return True
    if any(k in t for k in PG): return True
    return False

def eligible(title):
    t = clean(title).lower()
    if exclude_streams(t): return False
    if not allow_skills(t): return False
    if not allow_domicile(t): return False
    band = education_band(t)
    return band in {"10th pass","12th pass","Any graduate"}
