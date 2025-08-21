"""
BD Jobs Dashboard — Engineering Roles
Focus: Controls Engineer, Process Engineer, Maintenance Engineer,
        Industrial Engineer, Mechanical Engineer, Manufacturing Engineer
"""

import os
import re
import requests
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

# ---------------------------
# Config
# ---------------------------
load_dotenv()

ADZUNA_APP_ID = os.getenv("ADZUNA_APP_ID", "")
ADZUNA_APP_KEY = os.getenv("ADZUNA_APP_KEY", "")
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
    blocklist = [
        "staffing","recruiting","talent","solutions","manpower",
        "randstad","adecco","insight global","robert half","aerotek",
        "actalent","system one","apex systems","teksystems"
    ]
    return any(b in n for b in blocklist)

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
                "description": normalize_text(j.get("description"))
            })
    return jobs

# ---------------------------
# Streamlit UI
# ---------------------------

st.set_page_config(page_title="BD Jobs Dashboard", layout="wide")
st.title("BD Jobs Dashboard — Targeted Engineering Roles")
st.caption("Shows companies hiring Controls, Process, Maintenance, Industrial, Mechanical, and Manufacturing Engineers (via Adzuna API).")

with st.sidebar:
    st.header("Search Settings")
    location_filter = st.text_input("Location contains (optional)", value="United States")
    max_days_old = st.slider("Max days old", min_value=1, max_value=60, value=14)
    pages = st.slider("Pages (50 results each)", min_value=1, max_value=5, value=2)
    run = st.button("Fetch Jobs")

# ---------------------------
# Role list
# ---------------------------

roles = [
    "controls engineer",
    "process engineer",
    "maintenance engineer",
    "industrial engineer",
    "mechanical engineer",
    "manufacturing engineer"
]

# ---------------------------
# Fetch jobs
# ---------------------------

df = pd.DataFrame(columns=["company","title","location","posted_at","url"])

if run:
    jobs = []
    for role in roles:
        jobs.extend(fetch_adzuna_jobs(query=role, where=location_filter, max_days_old=max_days_old, pages=pages))
    df = pd.DataFrame(jobs)
    if not df.empty:
        df = df[~df["company"].apply(is_staffing_agency)]
        df = df.drop_duplicates(subset=["company","title","location"], keep="first")

# ---------------------------
# Metrics
# ---------------------------

c1, c2, c3 = st.columns(3)
if not df.empty:
    with c1: st.metric("Open roles", int(df.shape[0]))
    with c2: st.metric("Hiring companies", int(df["company"].nunique()))
    with c3: st.metric("Locations", int(df["location"].nunique()))
else:
    with c1: st.metric("Open roles", 0)
    with c2: st.metric("Hiring companies", 0)
    with c3: st.metric("Locations", 0)

# ---------------------------
# Results
# ---------------------------

st.subheader("Results")
if not df.empty:
    st.dataframe(df[["company","title","location","posted_at","url"]], use_container_width=True, hide_index=True)
    st.download_button(
        label="Download CSV",
        data=df.to_csv(index=False).encode("utf-8"),
        file_name="bd_engineering_roles.csv",
        mime="text/csv",
    )
else:
    st.info("No jobs found yet. Click **Fetch Jobs** in the sidebar to load data.")

