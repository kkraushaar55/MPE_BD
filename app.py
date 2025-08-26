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
# Config / Setup
# ---------------------------
load_dotenv()
st.set_page_config(page_title="US Controls & Industrial Automation (ATS-only)", layout="wide")
st.title("US Controls & Industrial Automation — Engineers + Leadership (No Agencies)")
st.caption("Direct ATS feeds (Greenhouse, Lever, SmartRecruiters, Ashby, Workable, Workday). Sorted by most recent posted date. No Adzuna.")

# ---------------------------
# Targeting — titles we WANT
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

# Manufacturing/industrial controls signal
MFG_HINTS = {
    "manufacturing","industrial","plant","factory","oem","process","production","assembly","operations",
    "control system","control systems","automation system","automation systems","instrumentation",
    "plc","hmi","scada","dcs","ladder logic","iec 61131","iec-61131","contrologix","compactlogix","studio 5000",
    "rslogix","panelview","rockwell","allen-bradley","allen bradley","siemens","tia portal","s7",
    "beckhoff","codesys","mitsubishi","omron","yaskawa","fanuc","abb robot","kuka","ur robot","robot"
}

# Exclude software/test automation
SOFT_AUTOMATION_NEG = {
    "sdet","qa","quality assurance","test automation","automated testing","automation tester","qa automation",
    "selenium","cypress","playwright","appium","robot framework","jest","mocha","junit","pytest",
    "devops","ci/cd","cicd","pipeline","kubernetes","docker","microservices","api automation","web automation"
}

DEFAULT_AGENCY_BLOCKLIST = {
    "adecco","randstad","manpower","manpowergroup","experis","hays","robert half","kelly","kelly services","kellyocg",
    "aerotek","actalent","kforce","insight global","beacon hill","on assignment","asgn","volt","system one","people ready",
    "appleone","motion recruitment","nelson","collabera","yoh","prolink","medix","pds tech","teksystems","tek systems",
    "cybercoders","cyber coders","jobot","gpac","talentbridge","talent bridge","ettain","ettain group","diversant",
    "mindlance","aston carter","allegis","matrix resources","amerit","vaco","cyberthink","collabera digital",
    "michael page","pagegroup","page personnel","trillium","harvey nash","signature consultants","atrium staffing",
    "cornerstone staffing","trc staffing","rht","aquent","apple one","lucid staffing","talentburst","datanomics"
}

US_STATE_ABBR = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA","KS","KY","LA","ME",
    "MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA",
    "RI","SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY","DC"
}
US_STATE_NAMES = {
    "alabama","alaska","arizona","arkansas","california","colorado","connecticut","delaware","florida",
    "georgia","hawaii","idaho","illinois","indiana","iowa","kansas","kentucky","louisiana","maine",
    "maryland","massachusetts","michigan","minnesota","mississippi","missouri","montana","nebraska",
    "nevada","new hampshire","new jersey","new mexico","new york","north carolina","north dakota",
    "ohio","oklahoma","oregon","pennsylvania","rhode island","south carolina","south dakota","tennessee",
    "texas","utah","vermont","virginia","washington","west virginia","wisconsin","wyoming","district of columbia"
}

def normalize_text(s): return re.sub(r"\s+", " ", str(s or "")).strip()
def title_is_target(title: str) -> bool: return bool(TITLE_REGEX.search(title or ""))
def looks_software_automation(blob: str) -> bool:
    b = (blob or "").lower(); return any(k in b for k in SOFT_AUTOMATION_NEG)
def looks_mfg_controls(blob: str) -> bool:
    b = (blob or "").lower(); return any(k in b for k in MFG_HINTS)

def is_staffing_agency(name: str, extra: set|None=None) -> bool:
    if not name: return False
    n = name.lower()
    bl = set(DEFAULT_AGENCY_BLOCKLIST) | ({x.strip().lower() for x in (extra or []) if x.strip()})
    return any(b in n for b in bl) or "staffing" in n or "recruit" in n or "agency" in n or "talent" in n

def is_us_location(display: str, area: list|None=None) -> bool:
    combined = " ".join([normalize_text(display)] + ([" ".join(area)] if area else [])).lower()
    if "united states" in combined or "usa" in combined or re.search(r"\bUS\b", combined): return True
    if any(s in combined for s in US_STATE_NAMES): return True
    if any(re.search(rf"\b{abbr}\b", combined) for abbr in US_STATE_ABBR): return True
    return False

# ---------------------------
# Sidebar UI
# ---------------------------
with st.sidebar:
    st.header("Scope")
    include_leadership = st.checkbox("Include leadership (Lead/Supervisor/Manager/Director/Head)", value=True)
    exclude_techs = st.checkbox("Exclude 'Technician' titles", value=True)

    st.divider()
    st.header("Watchlist (ATS direct, zero quota)")
    st.caption("companies.csv columns: company,ats,token,api_base,industry")
    include_watchlist = st.checkbox("Use companies.csv", value=True)
    if include_watchlist and not os.path.exists("companies.csv"):
        st.warning("companies.csv not found at repo root.")

    st.divider()
    st.header("Filters")
    us_only = st.checkbox("US only", value=True)
    exclude_agencies = st.checkbox("Exclude staffing agencies", value=True)
    extra_agencies = st.text_area("Extra agency names (comma-separated)", value="CyberCoders, Kelly Services, Insight Global, Robert Half")
    extra_agencies_set = {x.strip() for x in extra_agencies.split(",") if x.strip()}

    st.divider()
    st.header("Recency & Output")
    lookback_days = st.slider("Only show jobs from the last N days", 1, 60, 14)
    top_n = st.slider("Show top N most recent", 10, 200, 50, step=10)

    run = st.button("Fetch Jobs")

# ---------------------------
# Fetch
# ---------------------------
jobs = []
if run:
    # 1) ATS Watchlist
    if include_watchlist and os.path.exists("companies.csv"):
        wl = pd.read_csv("companies.csv")
        wl.columns = [c.strip().lower() for c in wl.columns]
        for _, row in wl.iterrows():
            ats = (row.get("ats") or "").strip().lower()
            tok = (row.get("token") or "").strip()
            api_base = (row.get("api_base") or "").strip()

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

# ---------------------------
# Filter → Sort (most recent)
# ---------------------------
df = pd.DataFrame(jobs)
if not df.empty:
    # Title targeting
    df = df[df["title"].apply(title_is_target)]

    # Optional: exclude technician titles
    if exclude_techs:
        df = df[~df["title"].str.contains(r"\btechnician\b", case=False, na=False)]

    # Dump software/test automation
    df = df[~df.apply(lambda r: looks_software_automation(f'{r.get("title","")} {r.get("description","")}'), axis=1)]

    # Require manufacturing/industrial controls hints
    df = df[df.apply(lambda r: looks_mfg_controls(f'{r.get("title","")} {r.get("description","")}'), axis=1)]

    # US only
    if us_only:
        df = df[df.apply(lambda r: is_us_location(r.get("location",""), r.get("location_area") or []), axis=1)]

    # Agencies
    if exclude_agencies:
        df = df[~df["company"].apply(lambda x: is_staffing_agency(x, extra_agencies_set))]

    # Normalize columns
    for c in ["company","title","location","posted_at","url","feed"]:
        if c in df.columns:
            df[c] = df[c].fillna("").astype(str).str.strip()
    df = df.drop_duplicates(subset=["company","title","location","url"], keep="first")

    # Recency: parse date → filter window → sort newest
    df["posted_at"] = pd.to_datetime(df["posted_at"], errors="coerce", utc=True)
    cutoff = pd.Timestamp.utcnow() - pd.Timedelta(days=lookback_days)
    df_recent = df[df["posted_at"] >= cutoff].copy()
    df_recent = df_recent.sort_values("posted_at", ascending=False, na_position="last")

    # Metrics
    c1, c2, c3, c4 = st.columns(4)
    with c1: st.metric("Open roles (window)", int(df_recent.shape[0]))
    with c2: st.metric("Hiring companies", int(df_recent["company"].nunique()))
    with c3: st.metric("Unique locations", int(df_recent["location"].nunique()))
    with c4: st.metric("Feeds", ", ".join(sorted(df_recent["feed"].unique())) if "feed" in df_recent.columns else "—")

    # Output
    st.subheader(f"Most Recent (last {lookback_days} days)")
    view = df_recent[["company","title","location","posted_at","url","feed"]].head(top_n)
    st.dataframe(view, use_container_width=True, hide_index=True)
    st.download_button(
        label="Download CSV",
        data=view.to_csv(index=False).encode("utf-8"),
        file_name="controls_automation_recent.csv",
        mime="text/csv",
    )

else:
    st.info("No jobs found. Check companies.csv and filters, then try again.")
