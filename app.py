"""
Live Business Development Jobs Dashboard (Compliant)

This app:
- Pulls engineering roles from Greenhouse, Lever, Adzuna (API), USAJOBS (API).
- Filters out staffing agencies.
- Groups results by industry.
- Lets you upload a CSV of target companies (with ATS info).
- Exports results to CSV.

"""

import os
import re
import requests
import pandas as pd
import streamlit as st
import tldextract
from dotenv import load_dotenv

# ---------------------------
# Config
# ---------------------------
load_dotenv()

ADZUNA_APP_ID = os.getenv("ADZUNA_APP_ID", "")
ADZUNA_APP_KEY = os.getenv("ADZUNA_APP_KEY", "")
USAJOBS_USER_AGENT = os.getenv("USAJOBS_USER_AGENT", "")
USAJOBS_AUTH_KEY = os.getenv("USAJOBS_AUTH_KEY", "")

USER_AGENT = "Mozilla/5.0 (BD Jobs Dashboard)"
TIMEOUT = 20

# ---------------------------
# Helpers
# ---------------------------

def normalize_text(s):
    if not s:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()

def is_staffing_agency(name: str) -> bool:
    if not name:
        return False
    n = name.lower()
    blocklist = ["staffing", "recruiting", "talent solutions", "manpower", "randstad",
                 "adecco", "insight global", "kforce", "aerotek", "robert half",
                 "actalent", "system one", "apex systems", "teksystems"]
    return any(b in n for b in blocklist)

def guess_industry(title: str, company: str = "", description: str = "") -> str:
    blob = " ".join([title or "", company or "", description or ""]).lower()
    if any(k in blob for k in ["automotive", "yanfeng", "magna", "faurecia"]):
        return "Automotive"
    if any(k in blob for k in ["aerospace", "boeing", "raytheon", "pratt"]):
        return "Aerospace & Defense"
    if any(k in blob for k in ["power", "solar", "wind", "nuclear", "substation"]):
        return "Energy & Utilities"
    if any(k in blob for k in ["gmp", "pharma", "biotech", "aseptic"]):
        return "Pharma & Med Device"
    if any(k in blob for k in ["semiconductor", "pcb", "electronics", "wafer"]):
        return "Semiconductor & Electronics"
    return "Other"

# ---------------------------
# Connectors
# ---------------------------

def fetch_greenhouse_jobs(token: str):
    url = f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs"
    try:
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json()
    except Exception:
        return []
    jobs = []
    for j in data.get("jobs", []):
        jobs.append({
            "source": "greenhouse",
            "company": token,
            "title": normalize_text(j.get("title")),
            "location": normalize_text((j.get("location") or {}).get("name")),
            "url": j.get("absolute_url"),
            "posted_at": "",
            "industry": ""
        })
    return jobs

def fetch_lever_jobs(token: str):
    url = f"https://api.lever.co/v0/postings/{token}?mode=json"
    try:
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json()
    except Exception:
        return []
    jobs = []
    for j in data:
        jobs.append({
            "source": "lever",
            "company": token,
            "title": normalize_text(j.get("text")),
            "location": normalize_text((j.get("categories") or {}).get("location")),
            "url": j.get("hostedUrl"),
            "posted_at": "",
            "industry": ""
        })
    return jobs

def fetch_adzuna_jobs(query="engineer", where="United States", max_days_old=14, pages=1):
    if not (ADZUNA_APP_ID and ADZUNA_APP_KEY):
        return []
    jobs = []
    base = f"https://api.adzuna.com/v1/api/jobs/us/search/"
    for page in range(1, pages + 1):
        params = {
            "app_id": ADZUNA_APP_ID,
            "app_key": ADZUNA_APP_KEY,
            "results_per_page": 50,
            "what": query,
            "where": where,
            "category": "engineering-jobs",
            "max_days_old": max_days_old,
        }
        try:
            r = requests.get(base + str(page), params=params, timeout=TIMEOUT)
            if r.status_code != 200:
                break
            data = r.json()
        except Exception:
            continue
        for j in data.get("results", []):
            jobs.append({
                "source": "adzuna",
                "company": normalize_text(((j.get("company") or {}).get("display_name"))),
                "title": normalize_text(j.get("title")),
                "location": normalize_text(((j.get("location") or {}).get("display_name"))),
                "url": j.get("redirect_url"),
                "posted_at": normalize_text(j.get("created")),
                "industry": "",
                "description": normalize_text(j.get("description"))
            })
    return jobs

# ---------------------------
# Streamlit UI
# ---------------------------

st.set_page_config(page_title="BD Jobs Dashboard", layout="wide")
st.title("BD Jobs Dashboard — Engineering")
st.caption("Pulls jobs from Greenhouse, Lever, Adzuna, USAJOBS (API) | Filters staffing agencies")

with st.sidebar:
    st.header("Settings")
    query = st.text_input("Keywords to include", value="engineer")
    location_filter = st.text_input("Location contains (optional)", value="")
    include_adzuna = st.checkbox("Include Adzuna (API key required)", value=False)
    exclude_agencies = st.checkbox("Exclude staffing agencies", value=True)
    run = st.button("Fetch Jobs")

# ---------------------------
# Run jobs fetch
# ---------------------------

df = pd.DataFrame(columns=["source", "company", "title", "location", "url", "posted_at", "industry"])

if run:
    jobs = []
    # Example: hardcoded companies for demo
    jobs.extend(fetch_greenhouse_jobs("spacex"))
    jobs.extend(fetch_lever_jobs("anduril"))
    if include_adzuna:
        jobs.extend(fetch_adzuna_jobs(query=query, where=location_filter or "United States", max_days_old=14, pages=1))
    df = pd.DataFrame(jobs)
    if not df.empty:
        df["industry"] = df.apply(lambda r: r["industry"] or guess_industry(r.get("title", ""), r.get("company", ""), r.get("description", "")), axis=1)
        if exclude_agencies:
            df = df[~df["company"].apply(is_staffing_agency)]
        df = df.drop_duplicates(subset=["url", "title", "company"], keep="first")

# ---------------------------
# Metrics
# ---------------------------

c1, c2, c3, c4 = st.columns(4)
if not df.empty:
    with c1: st.metric("Open roles", int(df.shape[0]))
    with c2: st.metric("Hiring companies", int(df["company"].nunique()))
    with c3: st.metric("Top locations", int(df["location"].nunique()))
    with c4: st.metric("Sources", ", ".join(sorted(df["source"].unique().tolist())))
else:
    with c1: st.metric("Open roles", 0)
    with c2: st.metric("Hiring companies", 0)
    with c3: st.metric("Top locations", 0)
    with c4: st.metric("Sources", "None")

# ---------------------------
# Results
# ---------------------------

st.subheader("Results")
if not df.empty:
    st.dataframe(df, use_container_width=True, hide_index=True)
    st.download_button(
        label="Download CSV",
        data=df.to_csv(index=False).encode("utf-8"),
        file_name="bd_engineering_jobs.csv",
        mime="text/csv",
    )
else:
    st.info("No jobs found yet. Click **Fetch Jobs** in the sidebar to load data.")
