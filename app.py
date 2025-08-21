"""
BD Jobs Dashboard — US Manufacturing Engineering & Managers (No Agencies)

What it does
- Searches Adzuna for targeted roles (engineers + manager variants) across any company
- Optional: also pull from a watchlist CSV (Greenhouse/Lever) to add more companies
- US-only filter, expanded staffing-agency blocklist, strict title filters
- Organized by company with counts + full results table
- Caches Adzuna responses for 1 hour to save your daily quota, and shows clear errors

Setup
- In Streamlit Cloud → Manage App → Settings → Secrets:
    ADZUNA_APP_ID = "..."
    ADZUNA_APP_KEY = "..."
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
# Role definitions (expanded)
# ---------------------------
CORE_ENGINEERING_ROLES = [
    # Core
    "controls engineer", "automation engineer",
    "process engineer",
    "maintenance engineer",
    "industrial engineer",
    "mechanical engineer",
    "manufacturing engineer",
    "plant engineer",
    "reliability engineer",
    "continuous improvement engineer",
    "quality engineer",
    # Expanded manufacturing roles
    "production engineer",
    "production process engineer",
    "manufacturing process engineer",
    "process development engineer",
    "manufacturing systems engineer",
    "industrialization engineer",
    "new product introduction engineer", "npi engineer",
    "tooling engineer", "mold tooling engineer", "die engineer",
    "weld engineer", "welding engineer",
    "equipment engineer",
    "facilities engineer",
    "plastics engineer", "injection molding engineer",
    "packaging engineer",
    "test engineer", "manufacturing test engineer",
    "mechatronics engineer",
    "methods engineer",
    "lean manufacturing engineer",
    "sustaining engineer",
]

MANAGER_VARIANTS = [
    # Managers
    "controls engineering manager", "automation manager", "controls manager",
    "process engineering manager",
    "maintenance manager", "maintenance engineering manager",
    "industrial engineering manager",
    "mechanical engineering manager",
    "manufacturing engineering manager", "manufacturing manager",
    "plant engineering manager", "plant manager",
    "reliability engineering manager", "reliability manager",
    "continuous improvement manager",
    "quality engineering manager", "quality manager",
    # Managers for expanded roles
    "production engineering manager", "production manager",
    "process development manager",
    "manufacturing systems manager",
    "industrialization manager",
    "npi manager", "new product introduction manager",
    "tooling manager", "molding manager", "weld engineering manager",
    "equipment engineering manager",
    "facilities engineering manager",
    "packaging engineering manager",
    "test engineering manager",
    "mechatronics manager",
    "methods engineering manager",
    "lean manufacturing manager",
    "sustaining engineering manager",
]

# Strict title regex (captures senior/principal/lead because base phrase appears)
TITLE_REGEX = re.compile(
    r"""(?ix)
    \b(
        # Engineers (core)
        (controls?|automation)\s+engineer|
        process\s+engineer|
        maintenance\s+engineer|
        industrial\s+engineer|
        mechanical\s+engineer|
        manufacturing\s+engineer|
        plant\s+engineer|
        reliability\s+engineer|
        continuous\s+improvement\s+engineer|
        quality\s+engineer|

        # Engineers (expanded)
        production(\s+process)?\s+engineer|
        manufacturing\s+process\s+engineer|
        process\s+development\s+engineer|
        manufacturing\s+systems?\s+engineer|
        industrialization\s+engineer|
        (new\s+product\s+introduction|npi)\s+engineer|
        tooling\s+engineer|
        (mold|mould|die)\s+engineer|
        weld(ing)?\s+engineer|
        equipment\s+engineer|
        facilities?\s+engineer|
        plastics?\s+engineer|
        injection\s+molding\s+engineer|
        packaging\s+engineer|
        test\s+engineer|
        manufacturing\s+test\s+engineer|
        mechatronics?\s+engineer|
        methods?\s+engineer|
        lean\s+manufacturing\s+engineer|
        sustaining\s+engineer|

        # Managers / manager-variants
        (controls?|automation)\s+(engineering\s+)?manager|
        process\s+(engineering\s+)?manager|
        maintenance(\s+engineering)?\s+manager|
        industrial\s+(engineering\s+)?manager|
        mechanical\s+(engineering\s+)?manager|
        manufacturing(\s+engineering)?\s+manager|
        plant(\s+engineering)?\s+manager|
        reliability(\s+engineering)?\s+manager|
        continuous\s+improvement\s+manager|
        quality(\s+engineering)?\s+manager|
        production(\s+process)?\s+(engineering\s+)?manager|
        process\s+development\s+(engineering\s+)?manager|
        manufacturing\s+systems?\s+(engineering\s+)?manager|
        industrialization\s+(engineering\s+)?manager|
        (new\s+product\s+introduction|npi)\s+(engineering\s+)?manager|
        tooling\s+(engineering\s+)?manager|
        (mold|mould|die)\s+(engineering\s+)?manager|
        weld(ing)?\s+(engineering\s+)?manager|
        equipment\s+(engineering\s+)?manager|
        facilities?\s+(engineering\s+)?manager|
        packaging\s+(engineering\s+)?manager|
        test\s+(engineering\s+)?manager|
        mechatronics?\s+(engineering\s+)?manager|
        methods?\s+(engineering\s+)?manager|
        lean\s+manufacturing\s+(engineering\s+)?manager|
        sustaining\s+(engineering\s+)?manager
    )\b
    """,
)

# ---------------------------
# Agency blocklist & US detection
# ---------------------------
DEFAULT_AGENCY_BLOCKLIST = {
    "adecco","randstad","manpower","manpowergroup","experis","hays","robert half","kelly","kelly services","kellyocg",
    "aerotek","actalent","kforce","insight global","beacon hill","on assignment","asgn","volt","system one","people ready",
    "appleone","motion recruitment","nelson","collabera","yoh","prolink","medix","pds tech","teksystems","tek systems",
    "cybercoders","cyber coders","jobot","gpac","talentbridge","talent bridge","ettain","ettain group","diversant",
    "mindlance","aston carter","allegis","matrix resources","amerit","vaco","cyberthink","mindseekers","collabera digital",
    "cornerstone staffing","roberthalf","apple one","aquent","patrice & associates","patrice and associates",
    "adecco staffing","randstad engineering","michael page","page personnel","pagegroup","tact","trillium","lucid staffing",
    "astoncarter","pinnacle group","harvey nash","rht","trc staffing","signature consultants","signature","atrium staffing",
    "suna","tri-s","tri s","talentpath","talentburst","datanomics"
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

def normalize_text(s: str | None) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()

def title_is_target(title: str) -> bool:
    return bool(TITLE_REGEX.search(title or ""))

def is_staffing_agency(name: str, extra: set[str] | None = None) -> bool:
    if not name:
        return False
    n = name.lower()
    bl = set(DEFAULT_AGENCY_BLOCKLIST)
    if extra:
        bl |= {x.strip().lower() for x in extra if x.strip()}
    return any(b in n for b in bl) or "staffing" in n or "recruit" in n or "agency" in n or "talent" in n

def is_us_location(display: str, area: list[str] | None) -> bool:
    combined = " ".join([normalize_text(display)] + ([" ".join(area)] if area else [])).lower()
    if "united states" in combined or "usa" in combined or re.search(r"\bUS\b", combined):
        return True
    if any(state in combined for state in US_STATE_NAMES):
        return True
    if any(re.search(rf"\b{abbr}\b", combined) for abbr in US_STATE_ABBR):
        return True
    return False

# ---------------------------
# Cached Adzuna page
# ---------------------------
@st.cache_data(ttl=3600, show_spinner=False)
def _adzuna_page(query: str, where: str, max_days_old: int, page: int):
    base = "https://api.adzuna.com/v1/api/jobs/us/search/"
    params = {
        "app_id": ADZUNA_APP_ID,
        "app_key": ADZUNA_APP_KEY,
        "results_per_page": 50,
        "what": query,
        "where": where,
        "category": "engineering-jobs",
        "max_days_old": max_days_old,
        "content-type": "application/json",
    }
    r = requests.get(base + str(page), params=params, timeout=TIMEOUT, headers={"User-Agent": USER_AGENT})
    r.raise_for_status()
    return r.json().get("results", [])

def fetch_adzuna_jobs(query: str, where: str, max_days_old: int, pages: int) -> list[dict]:
    if not (ADZUNA_APP_ID and ADZUNA_APP_KEY):
        st.error("Missing Adzuna keys. Add ADZUNA_APP_ID and ADZUNA_APP_KEY in Streamlit Secrets.")
        return []
    jobs = []
    for page in range(1, pages + 1):
        try:
            results = _adzuna_page(query, where, max_days_old, page)
        except requests.HTTPError as e:
            code = getattr(e.response, "status_code", "HTTP")
            st.error(f"Adzuna error (HTTP {code}). Reduce pages/roles or try later.")
            break
        except Exception:
            st.error("Adzuna request failed. Check keys/network or try again.")
            break
        for j in results:
            loc = j.get("location") or {}
            jobs.append({
                "feed": "adzuna",
                "company": normalize_text(((j.get("company") or {}).get("display_name"))),
                "title": normalize_text(j.get("title")),
                "location": normalize_text(loc.get("display_name")),
                "location_area": loc.get("area") or [],
                "posted_at": normalize_text(j.get("created")),
                "url": j.get("redirect_url"),
                "description": normalize_text(j.get("description")),
            })
    return jobs

# ---------------------------
# Optional watchlist (Greenhouse/Lever)
# ---------------------------
def fetch_greenhouse_jobs(token: str) -> list[dict]:
    url = f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs"
    try:
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json()
    except Exception:
        return []
    out = []
    for j in data.get("jobs", []):
        out.append({
            "feed": "greenhouse",
            "company": token,
            "title": normalize_text(j.get("title")),
            "location": normalize_text((j.get("location") or {}).get("name")),
            "location_area": [],
            "posted_at": "",
            "url": j.get("absolute_url"),
            "description": "",
        })
    return out

def fetch_lever_jobs(token: str) -> list[dict]:
    url = f"https://api.lever.co/v0/postings/{token}?mode=json"
    try:
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json()
    except Exception:
        return []
    out = []
    for j in data:
        cats = j.get("categories") or {}
        out.append({
            "feed": "lever",
            "company": token,
            "title": normalize_text(j.get("text")),
            "location": normalize_text(cats.get("location") or ""),
            "location_area": [],
            "posted_at": "",
            "url": j.get("hostedUrl") or j.get("applyUrl") or j.get("url"),
            "description": "",
        })
    return out

# ---------------------------
# UI
# ---------------------------
st.set_page_config(page_title="BD Jobs — Mfg Engineering & Managers", layout="wide")
st.title("US Manufacturing Engineering & Manager Hiring (No Agencies)")
st.caption("Adzuna + optional Watchlist (Greenhouse/Lever). Organized by company. Cached to protect your API quota.")

with st.sidebar:
    st.header("Roles & Scope")
    include_ci_quality = st.checkbox("Include CI & Quality families", value=True)

    role_pool = CORE_ENGINEERING_ROLES + (["continuous improvement engineer","quality engineer"] if include_ci_quality else [])
    roles_selected = st.multiselect("Engineer roles", options=sorted(set(role_pool)), default=sorted(set(role_pool)))

    include_managers = st.checkbox("Include manager variants", value=True)
    manager_selected = st.multiselect(
        "Manager titles",
        options=sorted(set(MANAGER_VARIANTS)),
        default=(sorted(set(MANAGER_VARIANTS)) if include_managers else [])
    )

    role_queries = sorted(set(roles_selected + manager_selected))

    st.divider()
    st.header("Search Filters")
    location_hint = st.text_input("Location filter hint", value="United States")
    us_only = st.checkbox("US only", value=True)
    max_days_old = st.slider("Max days old", 1, 60, 21)
    pages = st.slider("Adzuna pages (x50 / role)", 1, 15, 8)

    # Warn if you're about to blow the free daily limit (~250/day)
    estimated_requests = max(1, len(role_queries or (CORE_ENGINEERING_ROLES + MANAGER_VARIANTS))) * pages
    if estimated_requests > 200:
        st.warning(f"About to make ~{estimated_requests} Adzuna calls. Reduce 'Pages' or select fewer roles (free tier ≈250/day).")

    st.divider()
    st.header("Agency Filters")
    exclude_agencies = st.checkbox("Exclude staffing agencies", value=True)
    extra_agencies = st.text_area("Extra agency names (comma separated)",
                                  value="CyberCoders, Kelly Services, Aerotek, Insight Global, Robert Half")
    extra_agencies_set = {x.strip() for x in extra_agencies.split(",") if x.strip()}

    st.divider()
    st.header("Watchlist (optional)")
    include_watchlist = st.checkbox("Include companies.csv (Greenhouse/Lever)", value=False)
    st.caption("Add companies.csv at repo root with: company,ats,token,industry")

    run = st.button("Fetch Jobs")

# ---------------------------
# Fetch + Filter
# ---------------------------
df = pd.DataFrame(columns=["company","title","location","posted_at","url","feed"])

if run:
    jobs: list[dict] = []

    # 1) Adzuna (broad market)
    for q in (role_queries or CORE_ENGINEERING_ROLES + MANAGER_VARIANTS):
        jobs.extend(fetch_adzuna_jobs(query=q, where=location_hint or "United States",
                                      max_days_old=max_days_old, pages=pages))

    # 2) Optional: Watchlist CSV
    if include_watchlist and os.path.exists("companies.csv"):
        wl = pd.read_csv("companies.csv")
        wl.columns = [c.strip().lower() for c in wl.columns]
        for _, row in wl.iterrows():
            ats = (row.get("ats") or "").strip().lower()
            tok = (row.get("token") or "").strip()
            if not tok:
                continue
            if ats == "greenhouse":
                jobs.extend(fetch_greenhouse_jobs(tok))
            elif ats == "lever":
                jobs.extend(fetch_lever_jobs(tok))

    raw = pd.DataFrame(jobs)

    if not raw.empty:
        # strict titles
        raw = raw[raw["title"].apply(title_is_target)]

        # US filter
        if us_only:
            raw = raw[
                raw.apply(lambda r: is_us_location(r.get("location",""), r.get("location_area") or []), axis=1)
            ]

        # agency filter
        if exclude_agencies:
            raw = raw[~raw["company"].apply(lambda x: is_staffing_agency(x, extra_agencies_set))]

        # clean
        for col in ["company","title","location","posted_at","url","feed"]:
            if col in raw.columns:
                raw[col] = raw[col].fillna("").astype(str).str.strip()

        raw = raw.drop_duplicates(subset=["company","title","location","url"], keep="first")
        if not raw.empty:
            df = raw[["company","title","location","posted_at","url","feed"]].copy()

# ---------------------------
# Company-first view
# ---------------------------
c1, c2, c3, c4 = st.columns(4)
if not df.empty:
    with c1: st.metric("Open roles", int(df.shape[0]))
    with c2: st.metric("Hiring companies", int(df["company"].nunique()))
    with c3: st.metric("Unique locations", int(df["location"].nunique()))
    with c4: st.metric("Feeds", ", ".join(sorted(df["feed"].unique())) if "feed" in df.columns else "adzuna")

    st.subheader("Top Companies by Open Roles")
    top_companies = df.groupby("company").size().sort_values(ascending=False)
    st.bar_chart(top_companies.head(25))

    st.subheader("Company → Open Roles (Count)")
    comp

