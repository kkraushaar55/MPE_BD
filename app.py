# app.py — Controls/Automation Jobs (ATS + Adzuna only) — Newest First

import os, re, requests, pandas as pd, streamlit as st
from dotenv import load_dotenv

from ats_providers import (
    fetch_greenhouse_jobs, fetch_lever_jobs, fetch_smartrecruiters_jobs,
    fetch_ashby_jobs, fetch_workable_jobs, fetch_workday_jobs,
)

# ---------- App setup ----------
load_dotenv()
st.set_page_config(page_title="US Controls & Industrial Automation", layout="wide")
st.title("US Controls & Industrial Automation — Engineers + Leadership (No Agencies)")
st.caption("Sources: ATS (Greenhouse/Lever/SmartRecruiters/Ashby/Workable/Workday) + Adzuna. Sorted newest first.")

# ---------- Secrets ----------
def _get_secret(name: str, default: str = "") -> str:
    try:
        if hasattr(st, "secrets") and name in st.secrets:
            return str(st.secrets[name])
    except Exception:
        pass
    return os.getenv(name, default)

ADZUNA_APP_ID = _get_secret("ADZUNA_APP_ID")
ADZUNA_APP_KEY = _get_secret("ADZUNA_APP_KEY")

USER_AGENT = "Mozilla/5.0 (BD Controls Dashboard)"
TIMEOUT = 20
SAFE_DAILY_BUDGET = 150
ADZUNA_CATEGORY = "engineering-jobs"

# ---------- Title targeting ----------
TITLE_REGEX = re.compile(
    r"""(?ix)\b(
        (controls?|automation)\s+engineer|
        (instrumentation\s*&?\s*controls?|i\s*&?\s*c)\s+engineer|
        electrical\s+controls?\s+engineer|
        plc\s+engineer|
        (controls?|automation)\s+(lead|supervisor|manager|director|head|chief)|
        (engineering\s+)?(manager|director)\s+of\s+(controls?|automation)
    )\b"""
)
def title_is_target(title): return bool(TITLE_REGEX.search(title or ""))

# ---------- Helpers ----------
def normalize_text(s): return re.sub(r"\s+", " ", str(s or "")).strip()
def _clean_val(v):
    try:
        s = str(v).strip()
        return "" if s.lower() in {"nan","none","nat"} else s
    except Exception:
        return ""

# ---------- Adzuna ----------
@st.cache_data(ttl=3600, show_spinner=False)
def _adzuna_page(query: str, where: str, max_days_old: int, page: int):
    base = "https://api.adzuna.com/v1/api/jobs/us/search/"
    params = {
        "app_id": ADZUNA_APP_ID, "app_key": ADZUNA_APP_KEY,
        "results_per_page": 50, "what": query, "where": where,
        "category": ADZUNA_CATEGORY, "max_days_old": max_days_old,
        "content-type": "application/json",
    }
    r = requests.get(base + str(page), params=params, timeout=TIMEOUT, headers={"User-Agent": USER_AGENT})
    r.raise_for_status()
    return (r.json() or {}).get("results", []) or []

def fetch_adzuna_controls(query: str, where: str, max_days_old: int, pages: int) -> list[dict]:
    if not (ADZUNA_APP_ID and ADZUNA_APP_KEY): return []
    out = []
    for p in range(1, pages+1):
        try:
            results = _adzuna_page(query, where, max_days_old, p)
        except Exception:
            break
        for j in results:
            loc = j.get("location") or {}
            out.append({
                "feed": "adzuna",
                "company": normalize_text(((j.get("company") or {}).get("display_name"))),
                "title": normalize_text(j.get("title")),
                "location": normalize_text(loc.get("display_name")),
                "posted_at": normalize_text(j.get("created")),
                "url": j.get("redirect_url"),
            })
    return out

# ---------- Sidebar ----------
with st.sidebar:
    st.header("Status")
    st.info("ATS: built-in")
    st.success(f"Adzuna: configured (…{ADZUNA_APP_ID[-2:]})") if (ADZUNA_APP_ID and ADZUNA_APP_KEY) else st.info("Adzuna: not configured")

    st.divider()
    st.header("Scope")
    include_leadership = st.checkbox("Include leadership (Lead/Supervisor/Manager/Director/Head)", value=True)

    st.divider()
    st.header("Adzuna")
    use_adzuna = st.checkbox("Use Adzuna feed", value=False, disabled=not (ADZUNA_APP_ID and ADZUNA_APP_KEY))
    adz_where = st.text_input("Adzuna location", value="United States", disabled=not use_adzuna)
    adz_max_days = st.slider("Adzuna max days old", 1, 60, 21, disabled=not use_adzuna)
    adz_pages = st.slider("Adzuna pages (x50 each)", 1, 12, 6, disabled=not use_adzuna)

    st.divider()
    st.header("Watchlist (ATS direct)")
    st.caption("companies.csv columns: company,ats,token,api_base,industry")
    include_watchlist = st.checkbox("Use companies.csv", value=True)

    st.divider()
    st.header("Output")
    lookback_days = st.slider("Only show jobs from the last N days", 1, 60, 14)
    top_n = st.slider("Show top N most recent", 10, 200, 50, step=10)

    run = st.button("Fetch Jobs")

# ---------- Fetch ----------
jobs: list[dict] = []
if run:
    # Adzuna
    if use_adzuna and ADZUNA_APP_ID and ADZUNA_APP_KEY:
        q_engineer = '(controls engineer OR automation engineer OR "instrumentation & controls engineer")'
        q_leader  = '(controls manager OR automation manager OR "director of controls" OR controls lead OR automation lead)'
        for q in [q_engineer] + ([q_leader] if include_leadership else []):
            jobs.extend(fetch_adzuna_controls(query=q, where=adz_where or "United States",
                                              max_days_old=adz_max_days, pages=adz_pages))

    # ATS (companies.csv)
    if include_watchlist and os.path.exists("companies.csv"):
        wl = pd.read_csv("companies.csv", dtype=str).fillna("")
        wl.columns = [c.strip().lower() for c in wl.columns]
        for _, row in wl.iterrows():
            ats      = _clean_val(row.get("ats")).lower()
            tok      = _clean_val(row.get("token"))
            api_base = _clean_val(row.get("api_base"))
            if ats == "greenhouse" and tok: jobs.extend(fetch_greenhouse_jobs(tok))
            elif ats == "lever" and tok: jobs.extend(fetch_lever_jobs(tok))
            elif ats == "smartrecruiters" and tok: jobs.extend(fetch_smartrecruiters_jobs(tok))
            elif ats == "ashby" and tok: jobs.extend(fetch_ashby_jobs(tok))
            elif ats == "workable" and tok: jobs.extend(fetch_workable_jobs(tok))
            elif ats in ("workday","workday_json") and api_base:
                wd_rows = fetch_workday_jobs(api_base=api_base, query="")
                if isinstance(wd_rows, list): jobs.extend(wd_rows)

# ---------- Filter + Display ----------
df = pd.DataFrame(jobs)
if not df.empty:
    df = df[df["title"].apply(title_is_target)]
    df["posted_at"] = pd.to_datetime(df["posted_at"], errors="coerce", utc=True)
    now_ts = pd.Timestamp.utcnow()
    df.loc[df["posted_at"].isna(), "posted_at"] = now_ts
    cutoff = now_ts - pd.Timedelta(days=lookback_days)
    df_recent = df[df["posted_at"] >= cutoff].copy().sort_values("posted_at", ascending=False)

    st.subheader(f"Most Recent (last {lookback_days} days)")
    cols = [c for c in ["company","title","location","posted_at","url","feed"] if c in df_recent.columns]
    st.dataframe(df_recent[cols].head(top_n), use_container_width=True, hide_index=True)
    st.download_button("Download CSV", df_recent[cols].to_csv(index=False).encode("utf-8"),
                       file_name="controls_automation_recent.csv", mime="text/csv")
else:
    st.info("No jobs found. Enable Adzuna or check companies.csv.")
