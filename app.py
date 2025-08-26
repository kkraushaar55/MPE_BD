import os
import re
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

from ats_providers import (
    fetch_greenhouse_jobs,
    fetch_lever_jobs,
    fetch_smartrecruiters_jobs,
    fetch_ashby_jobs,
    fetch_workable_jobs,
    fetch_workday_jobs,
)

# ---------------------------
# App setup
# ---------------------------
load_dotenv()
st.set_page_config(page_title="US Controls & Industrial Automation (ATS-only)", layout="wide")
st.title("US Controls & Industrial Automation — Engineers + Leadership (No Agencies)")
st.caption("Direct ATS feeds (Greenhouse, Lever, SmartRecruiters, Ashby, Workable, Workday). Sorted by most recent posted date.")

# ---------------------------
# Targeting & Filters
# ---------------------------
TITLE_REGEX = re.compile(
    r"""(?ix)\b(
        (controls?|automation)\s+engineer|
        (instrumentation\s*&?\s*controls?|i\s*&?\s*c)\s+engineer|
        electrical\s+controls?\s+engineer|
        plc\s+engineer|
        (controls?|automation|instrumentation\s*&?\s*controls?)\s+(lead|supervisor|manager|director|head|chief)|
        (engineering\s+)?(manager|director)\s+of\s+(controls?|automation)
    )\b"""
)

MFG_HINTS = {
    "manufacturing","industrial","plant","factory","oem","process","production","assembly","operations",
    "control system","automation system","plc","hmi","scada","dcs","ladder logic",
    "contrologix","compactlogix","studio 5000","rslogix","panelview","rockwell","allen-bradley",
    "siemens","tia portal","s7","beckhoff","codesys","mitsubishi","omron","yaskawa","fanuc","abb","kuka","ur robot","robot"
}

SOFT_AUTOMATION_NEG = {
    "sdet","qa","quality assurance","test automation","automation tester","selenium","cypress",
    "devops","ci/cd","docker","kubernetes","microservices","api automation","web automation"
}

DEFAULT_AGENCY_BLOCKLIST = {
    "adecco","randstad","manpower","experis","hays","robert half","kelly","aerotek","actalent",
    "kforce","insight global","beacon hill","asgn","volt","system one","cybercoders","jobot",
    "gpac","talentbridge","mindlance","aston carter","allegis","matrix resources","amerit",
    "vaco","michael page","trillium","harvey nash","signature consultants","atrium staffing",
    "cornerstone staffing","trc staffing","rht","aquent","lucid staffing","talentburst"
}

US_STATE_ABBR = {"AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA","KS","KY",
                 "LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND",
                 "OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY","DC"}
US_STATE_NAMES = {s.lower() for s in [
    "Alabama","Alaska","Arizona","Arkansas","California","Colorado","Connecticut","Delaware","Florida",
    "Georgia","Hawaii","Idaho","Illinois","Indiana","Iowa","Kansas","Kentucky","Louisiana","Maine",
    "Maryland","Massachusetts","Michigan","Minnesota","Mississippi","Missouri","Montana","Nebraska",
    "Nevada","New Hampshire","New Jersey","New Mexico","New York","North Carolina","North Dakota",
    "Ohio","Oklahoma","Oregon","Pennsylvania","Rhode Island","South Carolina","South Dakota","Tennessee",
    "Texas","Utah","Vermont","Virginia","Washington","West Virginia","Wisconsin","Wyoming","District of Columbia"
]}

def normalize_text(s): return re.sub(r"\s+", " ", str(s or "")).strip()
def title_is_target(title): return bool(TITLE_REGEX.search(title or ""))
def looks_software_automation(blob): return any(k in (blob or "").lower() for k in SOFT_AUTOMATION_NEG)
def looks_mfg_controls(blob): return any(k in (blob or "").lower() for k in MFG_HINTS)
def is_staffing_agency(name, extra=None):
    if not name: return False
    n = name.lower()
    bl = set(DEFAULT_AGENCY_BLOCKLIST) | ({x.strip().lower() for x in (extra or []) if x.strip()})
    return any(b in n for b in bl) or "staffing" in n or "recruit" in n or "agency" in n or "talent" in n
def is_us_location(display, area=None):
    combined = " ".join([normalize_text(display)] + ([" ".join(area)] if area else [])).lower()
    return ("united states" in combined or "usa" in combined or re.search(r"\bUS\b", combined) or
            any(s in combined for s in US_STATE_NAMES) or
            any(re.search(rf"\b{abbr}\b", combined) for abbr in US_STATE_ABBR))

# Clean CSV values safely
def _clean_val(v):
    try:
        s = str(v).strip()
        return "" if s.lower() in {"nan","none","nat"} else s
    except Exception:
        return ""

# ---------------------------
# Sidebar
# ---------------------------
with st.sidebar:
    st.header("Scope")
    include_leadership = st.checkbox("Include leadership titles", value=True)
    exclude_techs = st.checkbox("Exclude 'Technician' titles", value=True)

    st.divider()
    st.header("Watchlist (companies.csv)")
    include_watchlist = st.checkbox("Use companies.csv", value=True)
    if include_watchlist and not os.path.exists("companies.csv"):
        st.warning("companies.csv not found at repo root.")

    st.divider()
    st.header("Filters")
    us_only = st.checkbox("US only", value=True)
    exclude_agencies = st.checkbox("Exclude staffing agencies", value=True)
    extra_agencies = st.text_area("Extra agency names (comma-separated)",
                                  value="CyberCoders, Kelly Services, Insight Global, Robert Half")
    extra_agencies_set = {x.strip() for x in extra_agencies.split(",") if x.strip()}

    st.divider()
    st.header("Recency & Output")
    lookback_days = st.slider("Only show jobs from last N days", 1, 60, 14)
    top_n = st.slider("Show top N most recent", 10, 200, 50, step=10)

    run = st.button("Fetch Jobs")

# ---------------------------
# Fetch
# ---------------------------
jobs = []
if run and include_watchlist and os.path.exists("companies.csv"):
    wl = pd.read_csv("companies.csv", dtype=str).fillna("")
    wl.columns = [c.strip().lower() for c in wl.columns]

    required_cols = {"company","ats","token","api_base","industry"}
    if missing := (required_cols - set(wl.columns)):
        st.error(f"`companies.csv` missing columns: {sorted(missing)}")
    else:
        for _, row in wl.iterrows():
            ats      = _clean_val(row.get("ats")).lower()
            tok      = _clean_val(row.get("token"))
            api_base = _clean_val(row.get("api_base"))

            try:
                if ats == "greenhouse" and tok:
                    jobs.extend(fetch_greenhouse_jobs(tok))
                elif ats == "lever" and tok:
                    jobs.extend(fetch_lever_jobs(tok))
                elif ats == "smartrecruiters" and tok:
                    jobs.extend(fetch_smartrecruiters_jobs(tok))
                elif ats == "ashby" and tok:
                    jobs.extend(fetch_ashby_jobs(tok))
                elif ats == "workable" and tok:
                    jobs.extend(fetch_workable_jobs(tok))
                elif ats in ("workday","workday_json") and api_base:
                    jobs.extend(fetch_workday_jobs(api_base=api_base, query=""))
            except Exception as e:
                st.warning(f"Failed to fetch for ats={ats}, token='{tok}', api_base='{api_base}': {e}")

# ---------------------------
# Filter + Sort
# ---------------------------
df = pd.DataFrame(jobs)

if not df.empty:
    df = df[df["title"].apply(title_is_target)]
    if exclude_techs:
        df = df[~df["title"].str.contains(r"\btechnician\b", case=False, na=False)]
    df = df[~df.apply(lambda r: looks_software_automation(f"{r.get('title','')} {r.get('description','')}"), axis=1)]
    df = df[df.apply(lambda r: looks_mfg_controls(f"{r.get('title','')} {r.get('description','')}"), axis=1)]

    if us_only:
        df = df[df.apply(lambda r: is_us_location(r.get("location",""), r.get("location_area") or []), axis=1)]
    if exclude_agencies:
        df = df[~df["company"].apply(lambda x: is_staffing_agency(x, extra_agencies_set))]

    for c in ["company","title","location","posted_at","url","feed"]:
        if c in df.columns:
            df[c] = df[c].fillna("").astype(str).str.strip()
    df = df.drop_duplicates(subset=["company","title","location","url"], keep="first")

    df["posted_at"] = pd.to_datetime(df["posted_at"], errors="coerce", utc=True)
    now_ts = pd.Timestamp.utcnow()
    df.loc[df["posted_at"].isna(), "posted_at"] = now_ts

    cutoff = now_ts - pd.Timedelta(days=lookback_days)
    df_recent = df[df["posted_at"] >= cutoff].sort_values("posted_at", ascending=False, na_position="last")

    c1, c2, c3, c4 = st.columns(4)
    with c1: st.metric("Open roles", int(df_recent.shape[0]))
    with c2: st.metric("Hiring companies", int(df_recent["company"].nunique()))
    with c3: st.metric("Unique locations", int(df_recent["location"].nunique()))
    with c4: st.metric("Feeds", ", ".join(sorted(df_recent["feed"].unique())) if "feed" in df_recent.columns else "—")

    st.subheader(f"Most Recent (last {lookback_days} days)")
    view = df_recent[["company","title","location","posted_at","url","feed"]].head(top_n)
    st.dataframe(view, use_container_width=True, hide_index=True)
    st.download_button("Download CSV", view.to_csv(index=False).encode("utf-8"),
                       file_name="controls_automation_recent.csv", mime="text/csv")
else:
    if run:
        st.info("No jobs found. Check companies.csv and filters, then try again.")
    else:
        st.caption("Set filters and click ‘Fetch Jobs’.")

