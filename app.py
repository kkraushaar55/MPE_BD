"""
Live Business Development Jobs Dashboard (Compliant)

What it does
------------
- Pulls open engineering roles from compliant sources:
  • Greenhouse Job Board API (direct employer boards)
  • Lever Postings API (direct employer boards)
  • Adzuna Jobs API (broad market aggregator; requires API key)
  • USAJOBS API (US federal roles; optional)
- De‑dupes, normalizes, and filters OUT staffing agencies
- Heuristically assigns industry buckets (Automotive, Aerospace, Energy, Manufacturing, etc.)
- Lets you upload a CSV of companies (and their ATS) to crawl directly (no scraping of LinkedIn/Indeed)
- Exports to CSV; shows trend/summary metrics in a small dashboard

Why no LinkedIn/Indeed scraping? Terms of Service prohibit automated scraping. Use their official programs/data partners if you need that coverage.

Quick start
-----------
1) Save this file as `app.py`
2) Python 3.10+
3) `pip install streamlit pandas requests python-dotenv tldextract`
4) Create a `.env` with any keys you have (optional):
   ADZUNA_APP_ID=your_id
   ADZUNA_APP_KEY=your_key
   USAJOBS_USER_AGENT=youremail@domain.com
   USAJOBS_AUTH_KEY=your_usajobs_key
5) Run: `streamlit run app.py`
6) (Recommended) Prepare a small CSV with columns: company, ats, token, domain, industry
   - `ats` must be one of: greenhouse, lever
   - `token` is the employer slug used by that ATS (examples in code comments)

Notes
-----
• This app avoids prohibited scraping and focuses on public/official APIs or employer job boards.
• You can extend with more connectors (Workday, SuccessFactors) if you have approved endpoints or explicit permissions.

"""

import os
import re
import time
import json
import math
import queue
import random
import typing as t
from dataclasses import dataclass

import requests
import pandas as pd
import streamlit as st
import tldextract
from dotenv import load_dotenv

# ---------------------------
# Config & Constants
# ---------------------------
load_dotenv()

ADZUNA_APP_ID = os.getenv("ADZUNA_APP_ID", "")
ADZUNA_APP_KEY = os.getenv("ADZUNA_APP_KEY", "")
USAJOBS_USER_AGENT = os.getenv("USAJOBS_USER_AGENT", "")
USAJOBS_AUTH_KEY = os.getenv("USAJOBS_AUTH_KEY", "")

USER_AGENT = "Mozilla/5.0 (BD Jobs Dashboard)"
TIMEOUT = 20

AGENCY_KEYWORDS = {
    "staffing", "recruiting", "recruitment", "talent solutions", "talent acquisition services",
    "search firm", "headhunter", "agency", "placement", "temp", "temporary services"
}

# Common US staffing brands to exclude when seen as the posting company
AGENCY_BLOCKLIST = {
    "adecco", "randstad", "robert half", "aerotek", "insight global", "kelly services", "kelly",
    "kforce", "apex systems", "beacon hill", "yoh", "asgn", "system one", "manpower", "manpowergroup",
    "volt", "hays", "ctg", "tek systems", "teksystems", "on assignment", "motion recruitment",
    "actalent", "nelson", "collabera", "people ready", "appleone", "prolink", "medix", "experis"
}

INDUSTRY_KEYWORDS = [
    ("Automotive", ["automotive", "bmw", "tier 1", "powertrain", "stamping", "injection molding", "yanfeng", "magna", "faurecia", "forvia", "adient", "toyota", "honda"]),
    ("Aerospace & Defense", ["aerospace", "aeronautic", "defense", "avionics", "space", "satellite", "boeing", "northrup", "raytheon", "pratt & whitney", "lockheed"]),
    ("Energy & Utilities", ["power", "substation", "t&d", "transmission", "generation", "hydro", "solar", "wind", "pv", "battery", "bess", "nuclear", "gas", "pipeline"]),
    ("Industrial & Manufacturing", ["manufacturing", "plant", "facility", "industrial", "process", "lean", "six sigma", "kaizen", "automation", "plc", "robotics", "packaging"]),
    ("Pharma & Med Device", ["gmp", "cgmp", "pharma", "biotech", "validation", "sterile", "aseptic", "medical device"]),
    ("Semiconductor & Electronics", ["semiconductor", "wafer", "lithography", "pcb", "electronics", "rf", "analog", "asic", "fpga"]),
    ("Construction & EPC", ["epc", "capex", "construction", "greenfield", "brownfield", "commissioning", "turnaround", "project engineer"]),
]

DEFAULT_INCLUDED_TITLE_KEYWORDS = ["engineer", "engineering", "process", "manufacturing", "quality", "maintenance", "controls", "automation", "project"]

# ---------------------------
# Helpers
# ---------------------------

def normalize_text(s: t.Optional[str]) -> str:
    if not s:
        return ""
    return re.sub(r"\s+", " ", s).strip()


def is_staffing_agency(name: str) -> bool:
    n = normalize_text(name).lower()
    if not n:
        return False
    if any(k in n for k in AGENCY_KEYWORDS):
        return True
    if any(b in n for b in AGENCY_BLOCKLIST):
        return True
    return False


def guess_industry(title: str, company: str = "", description: str = "") -> str:
    blob = " ".join([title or "", company or "", description or ""]).lower()
    for industry, keys in INDUSTRY_KEYWORDS:
        for k in keys:
            if k in blob:
                return industry
    # Heuristic fallback by role keywords
    if any(k in blob for k in ["gmp", "pharma", "aseptic", "sterile", "validation"]):
        return "Pharma & Med Device"
    if any(k in blob for k in ["plc", "ladder", "hmi", "scada", "robot", "automation"]):
        return "Industrial & Manufacturing"
    if any(k in blob for k in ["substation", "transformer", "relay", "utility"]):
        return "Energy & Utilities"
    return "Unknown"


def extract_domain(url: str) -> str:
    try:
        ext = tldextract.extract(url)
        return ".".join([p for p in [ext.domain, ext.suffix] if p])
    except Exception:
        return ""

# ---------------------------
# Connectors
# ---------------------------

@dataclass
class Job:
    source: str
    company: str
    title: str
    location: str
    url: str
    posted_at: str = ""
    industry: str = ""
    description: str = ""

    def to_dict(self) -> dict:
        return {
            "source": self.source,
            "company": self.company,
            "title": self.title,
            "location": self.location,
            "posted_at": self.posted_at,
            "industry": self.industry,
            "url": self.url,
        }


# Greenhouse: https://boards-api.greenhouse.io/v1/boards/{token}/jobs
# Examples of tokens: "stripe", "spacex" (varies by company board)

def fetch_greenhouse_jobs(token: str) -> t.List[Job]:
    url = f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs"
    r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT)
    r.raise_for_status()
    data = r.json()
    jobs = []
    for j in data.get("jobs", []):
        title = normalize_text(j.get("title"))
        company = normalize_text(token)
        loc = normalize_text((j.get("location") or {}).get("name"))
        link = j.get("absolute_url")
        # Greenhouse returns limited fields; industry will be heuristic
        jobs.append(Job(source="greenhouse", company=company, title=title, location=loc, url=link))
    return jobs


# Lever: https://api.lever.co/v0/postings/{company}?mode=json

def fetch_lever_jobs(company_slug: str) -> t.List[Job]:
    url = f"https://api.lever.co/v0/postings/{company_slug}?mode=json"
    r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT)
    r.raise_for_status()
    data = r.json()
    jobs = []
    for j in data:
        title = normalize_text(j.get("text"))
        company = normalize_text(company_slug)
        cats = j.get("categories") or {}
        loc = normalize_text(cats.get("location") or cats.get("commitment") or "")
        link = j.get("hostedUrl") or j.get("applyUrl") or j.get("url")
        posted = normalize_text(j.get("createdAt") or "")
        jobs.append(Job(source="lever", company=company, title=title, location=loc, url=link, posted_at=posted))
    return jobs


# Adzuna: https://developer.adzuna.com/docs/search  (US site domain is 'us')

def fetch_adzuna_jobs(query: str = "engineer", where: str = "United States", max_days_old: int = 14, pages: int = 2) -> t.List[Job]:
    jobs: t.List[Job] = []
    if not (ADZUNA_APP_ID and ADZUNA_APP_KEY):
        return jobs
    country = "us"
    base = f"https://api.adzuna.com/v1/api/jobs/{country}/search/"
    headers = {"User-Agent": USER_AGENT}
    for page in range(1, pages + 1):
        params = {
            "app_id": ADZUNA_APP_ID,
            "app_key": ADZUNA_APP_KEY,
            "results_per_page": 50,
            "what": query,
            "where": where,
            "category": "engineering-jobs",
            "content-type": "application/json",
            "max_days_old": max_days_old,
        }
        url = base + str(page)
        r = requests.get(url, headers=headers, params=params, timeout=TIMEOUT)
        if r.status_code != 200:
            break
        data = r.json()
        for j in data.get("results", []):
            title = normalize_text(j.get("title"))
            company = normalize_text(((j.get("company") or {}).get("display_name")) or "")
            loc = normalize_text(((j.get("location") or {}).get("display_name")) or "")
            link = j.get("redirect_url") or j.get("adref") or ""
            posted = normalize_text(j.get("created"))
            desc = normalize_text(j.get("description"))
            jobs.append(Job(source="adzuna", company=company, title=title, location=loc, url=link, posted_at=posted, description=desc))
    return jobs


# USAJOBS: https://developer.usajobs.gov/api-reference/get-api-search

def fetch_usajobs_jobs(keyword: str = "Engineer", location: str = "") -> t.List[Job]:
    jobs: t.List[Job] = []
    if not (USAJOBS_USER_AGENT and USAJOBS_AUTH_KEY):
        return jobs
    url = "https://data.usajobs.gov/api/search"
    headers = {
        "User-Agent": USAJOBS_USER_AGENT,
        "Authorization-Key": USAJOBS_AUTH_KEY,
    }
    params = {"Keyword": keyword}
    if location:
        params["LocationName"] = location
    r = requests.get(url, headers=headers, params=params, timeout=TIMEOUT)
    if r.status_code != 200:
        return jobs
    data = r.json() or {}
    for item in (data.get("SearchResult", {}) or {}).get("SearchResultItems", []):
        pos = (item.get("MatchedObjectDescriptor") or {})
        title = normalize_text(pos.get("PositionTitle"))
        company = normalize_text(pos.get("OrganizationName"))
        locs = [normalize_text(l.get("LocationName")) for l in pos.get("PositionLocation", [])]
        loc = ", ".join([l for l in locs if l])
        url_j = normalize_text(pos.get("PositionURI"))
        posted = normalize_text(pos.get("PublicationStartDate"))
        jobs.append(Job(source="usajobs", company=company, title=title, location=loc, url=url_j, posted_at=posted))
    return jobs


# ---------------------------
# Data pipeline
# ---------------------------

def load_company_map(uploaded_df: t.Optional[pd.DataFrame]) -> pd.DataFrame:
    cols = ["company", "ats", "token", "domain", "industry"]
    if uploaded_df is None or uploaded_df.empty:
        return pd.DataFrame(columns=cols)
    # Normalize columns
    df = uploaded_df.copy()
    df.columns = [c.strip().lower() for c in df.columns]
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    df["company"] = df["company"].astype(str).str.strip()
    df["ats"] = df["ats"].astype(str).str.strip().str.lower()
    df["token"] = df["token"].astype(str).str.strip()
    df["domain"] = df["domain"].astype(str).str.strip()
    df["industry"] = df["industry"].astype(str).str.strip()
    return df[cols]


def gather_jobs(company_map: pd.DataFrame, include_sources: t.Set[str], query: str, where: str, max_days_old: int, pages: int) -> pd.DataFrame:
    all_jobs: t.List[Job] = []

    # Employer ATS sources (direct, non-agency)
    if not company_map.empty:
        gh_tokens = company_map[company_map["ats"] == "greenhouse"]["token"].dropna().unique().tolist()
        lv_tokens = company_map[company_map["ats"] == "lever"]["token"].dropna().unique().tolist()
        for tok in gh_tokens:
            try:
                all_jobs.extend(fetch_greenhouse_jobs(tok))
            except Exception:
                continue
        for tok in lv_tokens:
            try:
                all_jobs.extend(fetch_lever_jobs(tok))
            except Exception:
                continue

    # Aggregators (opt‑in)
    if "adzuna" in include_sources:
        try:
            all_jobs.extend(fetch_adzuna_jobs(query=query or "engineer", where=where or "United States", max_days_old=max_days_old, pages=pages))
        except Exception:
            pass

    if "usajobs" in include_sources:
        try:
            all_jobs.extend(fetch_usajobs_jobs(keyword=query or "Engineer", location=where))
        except Exception:
            pass

    # Normalize -> DataFrame
    rows = [j.to_dict() for j in all_jobs]
    df = pd.DataFrame(rows)
    if df.empty:
        return df

    # Guess industries where missing
    df["industry"] = df.apply(lambda r: r["industry"] or guess_industry(r.get("title", ""), r.get("company", "")), axis=1)

    # Clean
    df["company"] = df["company"].fillna("").apply(normalize_text)
    df["title"] = df["title"].fillna("").apply(normalize_text)
    df["location"] = df["location"].fillna("").apply(normalize_text)
    df["url"] = df["url"].fillna("")

    # Dedupe on URL + title + company
    df = df.drop_duplicates(subset=["url", "title", "company"], keep="first")

    return df


def apply_filters(df: pd.DataFrame, exclude_agencies: bool, include_kw: t.List[str], exclude_kw: t.List[str], location_filter: str) -> pd.DataFrame:
    if df.empty:
        return df

    dd = df.copy()

    if exclude_agencies:
        mask = ~dd["company"].str.lower().apply(is_staffing_agency)
        dd = dd[mask]

    # Include keywords in title
    if include_kw:
        incl_regex = re.compile("|".join([re.escape(k.lower()) for k in include_kw if k]), re.I)
        dd = dd[dd["title"].str.contains(incl_regex, na=False)]

    # Exclude keywords in title
    if exclude_kw:
        excl_regex = re.compile("|".join([re.escape(k.lower()) for k in exclude_kw if k]), re.I)
        dd = dd[~dd["title"].str.contains(excl_regex, na=False)]

    # Location filter (substring match)
    if location_filter:
        loc_re = re.compile(re.escape(location_filter), re.I)
        dd = dd[dd["location"].str.contains(loc_re, na=False)]

    # Sort by posted_at desc if available, else title
    dd = dd.sort_values(by=["posted_at", "company", "title"], ascending=[False, True, True], na_position="last")
    return dd


# ---------------------------
# Streamlit UI
# ---------------------------

st.set_page_config(page_title="BD Jobs Dashboard (Compliant)", layout="wide")
st.title("BD Jobs Dashboard — Engineering (Compliant, No Scraping)")
st.caption("Greenhouse, Lever, Adzuna, USAJOBS | Filters out staffing agencies | Exportable")

with st.sidebar:
    st.header("Data Sources & Filters")
    st.markdown("**Employer ATS (upload mapping)**")
    upload = st.file_uploader("Upload company mapping CSV (company, ats, token, domain, industry)", type=["csv"])
    uploaded_df = None
    if upload:
        try:
            uploaded_df = pd.read_csv(upload)
        except Exception:
            st.error("Couldn't parse the CSV. Ensure it has headers: company, ats, token, domain, industry")

    include_adzuna = st.checkbox("Include Adzuna (requires API key)", value=bool(ADZUNA_APP_ID and ADZUNA_APP_KEY))
    include_usajobs = st.checkbox("Include USAJOBS", value=False)

    st.divider()
    st.subheader("Search")
    query = st.text_input("Keywords to include", value=", ".join(DEFAULT_INCLUDED_TITLE_KEYWORDS))
    exclude = st.text_input("Keywords to exclude", value="recruiter, staffing, agency")
    location_filter = st.text_input("Location contains (optional)", value="")
    max_days_old = st.slider("Max days old (Adzuna)", min_value=1, max_value=60, value=21, step=1)
    pages = st.slider("Adzuna pages", min_value=1, max_value=10, value=3)

    st.divider()
    exclude_agencies = st.checkbox("Exclude staffing agencies", value=True)

    run = st.button("Refresh jobs now")

company_map = load_company_map(uploaded_df)

include_sources = set()
if include_adzuna:
    include_sources.add("adzuna")
if include_usajobs:
    include_sources.add("usajobs")

if run or "_cached_jobs" not in st.session_state:
    with st.spinner("Fetching jobs..."):
        df_all = gather_jobs(company_map, include_sources, query=query, where=location_filter, max_days_old=max_days_old, pages=pages)
        st.session_state["_cached_jobs"] = df_all

df_all = st.session_state.get("_cached_jobs", pd.DataFrame(columns=["source","company","title","location","posted_at","industry","url"]))

inc_kw = [k.strip() for k in (query or "").split(",") if k.strip()]
exc_kw = [k.strip() for k in (exclude or "").split(",") if k.strip()]

df = apply_filters(df_all, exclude_agencies=exclude_agencies, include_kw=inc_kw, exclude_kw=exc_kw, location_filter=location_filter)

# Metrics & visuals
c1, c2, c3, c4 = st.columns(4)
with c1:
    st.metric("Open roles", int(df.shape[0]))
with c2:
    st.metric("Hiring companies", int(df["company"].nunique()))
with c3:
    st.metric("Top locations", int(df["location"].nunique()))
with c4:
    st.metric("Sources", ", ".join(sorted(df["source"].unique().tolist())))

# Top 10 by industry
if not df.empty:
    st.subheader("Open roles by industry")
    counts = df.groupby("industry").size().sort_values(ascending=False).head(10)
    st.bar_chart(counts)

    st.subheader("Top hiring locations (city/state text)")
    loc_counts = df.groupby("location").size().sort_values(ascending=False).head(15)
    st.bar_chart(loc_counts)

st.subheader("Results (filtered)")
st.dataframe(df, use_container_width=True, hide_index=True)

# Download
st.download_button(
    label="Download CSV",
    data=df.to_csv(index=False).encode("utf-8"),
    file_name="bd_engineering_jobs.csv",
    mime="text/csv",
)

# Helpful notes / sample CSV template
with st.expander("CSV template & examples"):
    st.markdown(
        """
**Columns:** `company, ats, token, domain, industry`

**ATS values:**
- `greenhouse` → token is the board slug (e.g., `stripe`, `spacex`, `formlabs`)
- `lever` → token is the company slug (e.g., `rippling`, `anduril`, `loom`)

**Sample rows:**
```
company,ats,token,domain,industry
Acme Robotics,greenhouse,acmerobotics,acmerobotics.com,Industrial & Manufacturing
Stellar Aerospace,lever,stellaraero,stellaraero.com,Aerospace & Defense
```
        """
    )

st.caption("Built to prioritize **compliance** and **direct employer** sources. Extend responsibly.")
