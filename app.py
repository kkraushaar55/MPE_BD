"""
BD Jobs Dashboard — Controls & Industrial Automation (No Agencies, US-only)
Focus: Controls/Automation Engineering + Leadership in manufacturing/industrial
EXCLUDES: software/test automation (SDET/QA/dev/test frameworks)

Feeds:
- Adzuna (cached + budget-capped)
- Watchlist (Greenhouse/Lever) — zero quota
- Web Discovery (Bing → employer career pages → JSON-LD JobPosting)

Secrets (Streamlit → Manage App → Settings → Secrets)
  ADZUNA_APP_ID = "..."
  ADZUNA_APP_KEY = "..."
  BING_SEARCH_KEY = "..."     # optional but recommended for Web Discovery
# Optional override:
#  BING_ENDPOINT = "https://api.bing.microsoft.com/v7.0/search"
"""

import os
import re
import json
import time
import requests
import pandas as pd
import streamlit as st
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser
from dotenv import load_dotenv

# ---------------------------
# Config / Secrets (safe)
# ---------------------------
load_dotenv()

def _get_secret(name: str, default: str = "") -> str:
    # Prefer Streamlit Secrets, then env vars, then default
    try:
        if hasattr(st, "secrets") and name in st.secrets:
            return str(st.secrets[name])
    except Exception:
        pass
    return os.getenv(name, default)

ADZUNA_APP_ID = _get_secret("ADZUNA_APP_ID")
ADZUNA_APP_KEY = _get_secret("ADZUNA_APP_KEY")
BING_SEARCH_KEY = _get_secret("BING_SEARCH_KEY")
BING_ENDPOINT = _get_secret("BING_ENDPOINT") or "https://api.bing.microsoft.com/v7.0/search"

USER_AGENT = "Mozilla/5.0 (BD Controls BD Dashboard)"
TIMEOUT = 20
SAFE_DAILY_BUDGET = 150   # keep well under free-tier
ADZUNA_CATEGORY = "engineering-jobs"

# ---------------------------
# Targeting — Titles we WANT
# ---------------------------
# Controls/Automation engineering + leadership variants (industrial/manufacturing)
TITLE_REGEX = re.compile(
    r"""(?ix)\b(
        # Engineers
        (controls?|automation)\s+engineer|
        (instrumentation\s*&?\s*controls?|i\s*&?\s*c)\s+engineer|
        electrical\s+controls?\s+engineer|
        plc\s+engineer|
        # Leadership
        (controls?|automation|instrumentation\s*&?\s*controls?)\s+(lead|supervisor|manager|director|head|chief)|
        (engineering\s+)?(manager|director)\s+of\s+(controls?|automation)
    )\b"""
)

# ---------------------------
# Context filter — industrial/manufacturing controls only
# ---------------------------
MFG_HINTS = {
    "manufacturing","industrial","plant","factory","oem","process","production","assembly","operations",
    "control system","control systems","automation system","automation systems","instrumentation",
    "plc","hmi","scada","dcs","ladder logic","iec 61131","iec-61131","contrologix","compactlogix","studio 5000",
    "rslogix","panelview","rockwell","allen-bradley","allen bradley","siemens","tia portal","s7",
    "beckhoff","codesys","mitsubishi","omron","yaskawa","fanuc","abb robot","kuka","ur robot","robot"
}

# EXCLUDES — software/test automation / QA / DevOps automation
SOFT_AUTOMATION_NEG = {
    "sdet","qa","quality assurance","test automation","automated testing","automation tester","qa automation",
    "selenium","cypress","playwright","appium","robot framework","jest","mocha","junit","pytest",
    "devops","ci/cd","cicd","pipeline","kubernetes","docker","microservices","api automation","web automation"
}

def normalize_text(s):
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()

def title_is_target(title: str) -> bool:
    return bool(TITLE_REGEX.search(title or ""))

def looks_software_automation(blob: str) -> bool:
    b = (blob or "").lower()
    return any(k in b for k in SOFT_AUTOMATION_NEG)

def looks_mfg_controls(blob: str) -> bool:
    b = (blob or "").lower()
    return any(k in b for k in MFG_HINTS)

# ---------------------------
# Agency blocklist & US detection
# ---------------------------
DEFAULT_AGENCY_BLOCKLIST = {
    "adecco","randstad","manpower","manpowergroup","experis","hays","robert half","kelly","kelly services","kellyocg",
    "aerotek","actalent","kforce","insight global","beacon hill","on assignment","asgn","volt","system one","people ready",
    "appleone","motion recruitment","nelson","collabera","yoh","prolink","medix","pds tech","teksystems","tek systems",
    "cybercoders","cyber coders","jobot","gpac","talentbridge","talent bridge","ettain","ettain group","diversant",
    "mindlance","aston carter","allegis","matrix resources","amerit","vaco","cyberthink","mindseekers","collabera digital",
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

def is_staffing_agency(name: str, extra: set|None=None) -> bool:
    if not name:
        return False
    n = name.lower()
    bl = set(DEFAULT_AGENCY_BLOCKLIST)
    if extra:
        bl |= {x.strip().lower() for x in extra if x.strip()}
    return any(b in n for b in bl) or "staffing" in n or "recruit" in n or "agency" in n or "talent" in n

def is_us_location(display: str, area: list|None) -> bool:
    combined = " ".join([normalize_text(display)] + ([" ".join(area)] if area else [])).lower()
    if "united states" in combined or "usa" in combined or re.search(r"\bUS\b", combined):
        return True
    if any(s in combined for s in US_STATE_NAMES):
        return True
    if any(re.search(rf"\b{abbr}\b", combined) for abbr in US_STATE_ABBR):
        return True
    return False

# ---------------------------
# Respect robots.txt & avoid banned domains
# ---------------------------
BLOCKED_DOMAINS = {
    "linkedin.com","indeed.com","glassdoor.com","ziprecruiter.com","monster.com",
    "simplyhired.com","talent.com","snagajob.com","careerbuilder.com"
}
_robots_cache: dict[str, RobotFileParser] = {}

def can_fetch(url: str) -> bool:
    try:
        host = urlparse(url).netloc.lower()
        if any(host.endswith(d) for d in BLOCKED_DOMAINS):
            return False
        if host not in _robots_cache:
            rp = RobotFileParser()
            rp.set_url(f"https://{host}/robots.txt")
            try:
                rp.read()
            except Exception:
                _robots_cache[host] = rp
                return True
            _robots_cache[host] = rp
        return _robots_cache[host].can_fetch(USER_AGENT, url)
    except Exception:
        return False

# ---------------------------
# Adzuna (cached pages)
# ---------------------------
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
    return r.json().get("results", [])

def fetch_adzuna_controls(query: str, where: str, max_days_old: int, pages: int) -> list[dict]:
    if not (ADZUNA_APP_ID and ADZUNA_APP_KEY):
        return []
    jobs = []
    for p in range(1, pages+1):
        try:
            results = _adzuna_page(query, where, max_days_old, p)
        except requests.HTTPError as e:
            code = getattr(e.response, "status_code", "HTTP")
            st.error(f"Adzuna error (HTTP {code}). Reduce pages or try later.")
            if code in (401,403,429):
                st.session_state["_adzuna_quota_hit"] = True
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
# Watchlist (Greenhouse / Lever) — zero quota
# ---------------------------
def fetch_greenhouse_jobs(token: str) -> list[dict]:
    url = f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs"
    try:
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT); r.raise_for_status()
        data = r.json()
    except Exception:
        return []
    out = []
    for j in data.get("jobs", []):
        out.append({
            "feed":"greenhouse","company": token,
            "title": normalize_text(j.get("title")),
            "location": normalize_text((j.get("location") or {}).get("name")),
            "location_area": [], "posted_at": "", "url": j.get("absolute_url"), "description":""
        })
    return out

def fetch_lever_jobs(token: str) -> list[dict]:
    url = f"https://api.lever.co/v0/postings/{token}?mode=json"
    try:
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT); r.raise_for_status()
        data = r.json()
    except Exception:
        return []
    out = []
    for j in data:
        cats = j.get("categories") or {}
        out.append({
            "feed":"lever","company": token,
            "title": normalize_text(j.get("text")),
            "location": normalize_text(cats.get("location") or ""),
            "location_area": [], "posted_at": "",
            "url": j.get("hostedUrl") or j.get("applyUrl") or j.get("url"),
            "description":""
        })
    return out

# ---------------------------
# Web Discovery (Bing → employer pages → JSON-LD JobPosting)
# ---------------------------
def bing_search(q: str, count: int = 10, mkt: str = "en-US") -> list[dict]:
    if not BING_SEARCH_KEY:
        return []
    headers = {"Ocp-Apim-Subscription-Key": BING_SEARCH_KEY}
    params = {"q": q, "count": count, "mkt": mkt, "responseFilter":"Webpages"}
    try:
        r = requests.get(BING_ENDPOINT, headers=headers, params=params, timeout=TIMEOUT); r.raise_for_status()
        return (r.json().get("webPages") or {}).get("value", [])
    except Exception:
        return []

def discover_controls_pages(per_role: int = 8) -> list[str]:
    # Bias to PLC/SCADA terms and exclude aggregators
    queries = [
        '( "controls engineer" OR "automation engineer" ) ( PLC OR SCADA OR HMI OR DCS ) ( careers OR jobs ) site:*.com -site:linkedin.com -site:indeed.com -site:glassdoor.com -site:ziprecruiter.com -site:monster.com -site:simplyhired.com -site:talent.com -site:snagajob.com',
        '( "controls manager" OR "automation manager" OR "director of controls" ) ( PLC OR SCADA OR HMI OR DCS ) ( careers OR jobs ) site:*.com -site:linkedin.com -site:indeed.com -site:glassdoor.com -site:ziprecruiter.com -site:monster.com -site:simplyhired.com -site:talent.com -site:snagajob.com',
        '( "instrumentation & controls" OR "I&C engineer" ) ( manufacturing OR industrial OR plant ) ( careers OR jobs ) site:*.com -site:linkedin.com -site:indeed.com -site:glassdoor.com -site:ziprecruiter.com -site:monster.com -site:simplyhired.com -site:talent.com -site:snagajob.com',
    ]
    urls = []
    for q in queries:
        for item in bing_search(q, count=per_role*2):
            url = item.get("url")
            if not url:
                continue
            host = urlparse(url).netloc.lower()
            if any(host.endswith(d) for d in BLOCKED_DOMAINS):
                continue
            # Likely career paths
            if not re.search(r"/careers?|/jobs?|/join|/opportunit|/vacanc", url, re.I):
                continue
            urls.append(url)
            if len(urls) >= per_role:
                break
    return urls

def parse_jobposting_from_html(url: str) -> list[dict]:
    if not can_fetch(url):
        return []
    try:
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT)
        if r.status_code != 200:
            return []
        soup = BeautifulSoup(r.text, "lxml")
        out = []
        for s in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(s.string or "{}")
            except Exception:
                continue
            items = data if isinstance(data, list) else [data]
            for obj in items:
                if not isinstance(obj, dict):
                    continue
                t = obj.get("@type") or obj.get("type")
                if (isinstance(t, list) and not any(str(x).lower()=="jobposting" for x in t)) or (isinstance(t,str) and t.lower()!="jobposting"):
                    continue

                title = normalize_text(obj.get("title"))
                desc = normalize_text(obj.get("description") or "")
                if not title or not title_is_target(title):
                    continue
                blob = f"{title} {desc}"
                if looks_software_automation(blob):
                    continue
                if not looks_mfg_controls(blob):
                    continue

                org = obj.get("hiringOrganization") or {}
                company = normalize_text(org.get("name") or urlparse(url).netloc.split(":")[0])

                loc = obj.get("jobLocation") or {}
                if isinstance(loc, list) and loc:
                    loc = loc[0]
                address = (loc.get("address") or {}) if isinstance(loc, dict) else {}
                display_loc = ", ".join(x for x in [
                    normalize_text(address.get("addressLocality")),
                    normalize_text(address.get("addressRegion")),
                    normalize_text(address.get("addressCountry"))
                ] if x)

                apply_url = normalize_text(obj.get("url") or url)
                date_posted = normalize_text(obj.get("datePosted") or "")

                out.append({
                    "feed":"web","company": company, "title": title, "location": display_loc, "location_area": [],
                    "posted_at": date_posted, "url": apply_url if apply_url.startswith("http") else url, "description":""
                })
        return out
    except Exception:
        return []

def web_discovery(per_role: int = 8, per_domain_cap: int = 3) -> list[dict]:
    jobs, seen = [], {}
    for url in discover_controls_pages(per_role=per_role):
        host = urlparse(url).netloc.lower()
        if seen.get(host,0) >= per_domain_cap:
            continue
        postings = parse_jobposting_from_html(url)
        if postings:
            seen[host] = seen.get(host,0)+1
            jobs.extend(postings)
        time.sleep(0.2)
    return jobs

# ---------------------------
# UI
# ---------------------------
st.set_page_config(page_title="BD — Controls & Industrial Automation", layout="wide")
st.title("US Controls & Industrial Automation — Engineers + Leadership (No Agencies)")
st.caption("Adzuna + Watchlist (Greenhouse/Lever) + Web Discovery. Filters out software/test automation.")

with st.sidebar:
    st.header("Status")
    if ADZUNA_APP_ID and ADZUNA_APP_KEY:
        st.success(f"Adzuna: configured (ID …{ADZUNA_APP_ID[-2:]}, Key …{ADZUNA_APP_KEY[-4:]})")
    else:
        st.error("Adzuna: missing keys (set ADZUNA_APP_ID/ADZUNA_APP_KEY in Settings → Secrets).")
    if BING_SEARCH_KEY:
        st.success("Bing Web Search: configured")
    else:
        st.info("Bing Web Search: add BING_SEARCH_KEY in Secrets to enable Web Discovery.")

    st.header("Scope")
    include_leadership = st.checkbox("Include leadership (Lead/Supervisor/Manager/Director/Head)", value=True)
    exclude_techs = st.checkbox("Exclude 'Technician' titles", value=True)

    st.divider()
    st.header("Adzuna")
    use_adzuna = st.checkbox("Use Adzuna feed", value=True)
    location_hint = st.text_input("Adzuna location", value="United States")
    max_days_old = st.slider("Adzuna max days old", 1, 60, 21)
    pages = st.slider("Adzuna pages (x50 each)", 1, 12, 6)

    # request budget guard (two queries: engineers + leadership)
    role_query_count = 2 if include_leadership else 1
    est_calls = (pages if use_adzuna else 0) * role_query_count
    if est_calls > SAFE_DAILY_BUDGET:
        st.warning(f"~{est_calls} Adzuna calls > budget {SAFE_DAILY_BUDGET}. Reduce pages.")

    st.divider()
    st.header("Watchlist (zero quota)")
    include_watchlist = st.checkbox("Include companies.csv (Greenhouse/Lever)", value=False)
    st.caption("companies.csv headers: company,ats,token,industry  (ats ∈ {greenhouse, lever})")

    st.divider()
    st.header("Web Discovery (Bing)")
    use_web = st.checkbox("Use Web Discovery", value=True)
    per_role = st.slider("Employer pages to discover", 1, 20, 8)
    per_domain_cap = st.slider("Max pages per domain", 1, 10, 3)

    st.divider()
    st.header("Filters")
    us_only = st.checkbox("US only", value=True)
    exclude_agencies = st.checkbox("Exclude staffing agencies", value=True)
    extra_agencies = st.text_area("Extra agency names (comma-separated)", value="CyberCoders, Kelly Services, Insight Global, Robert Half")
    extra_agencies_set = {x.strip() for x in extra_agencies.split(",") if x.strip()}

    run = st.button("Fetch Jobs")

# ---------------------------
# Fetch + Filter
# ---------------------------
df = pd.DataFrame(columns=["company","title","location","posted_at","url","feed"])
if run:
    jobs = []

    # 1) Adzuna — two queries: engineers; leadership (if selected)
    if use_adzuna and ADZUNA_APP_ID and ADZUNA_APP_KEY:
        st.session_state["_adzuna_quota_hit"] = False
        q_engineer = '(controls engineer OR automation engineer OR "instrumentation & controls engineer" OR "i&c engineer")'
        q_leader  = '(controls manager OR automation manager OR "director of controls" OR "director of automation" OR controls lead OR automation lead)'
        queries = [q_engineer] + ([q_leader] if include_leadership else [])
        for q in queries:
            if st.session_state.get("_adzuna_quota_hit"):
                break
            jobs.extend(fetch_adzuna_controls(query=q, where=location_hint or "United States",
                                              max_days_old=max_days_old, pages=pages))

    # 2) Watchlist (ATS direct)
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

    # 3) Web Discovery
    if use_web and BING_SEARCH_KEY:
        jobs.extend(web_discovery(per_role=per_role, per_domain_cap=per_domain_cap))

    raw = pd.DataFrame(jobs)

    # ---------- Filtering pipeline ----------
    if not raw.empty:
        # Title must be controls/automation target
        raw = raw[raw["title"].apply(title_is_target)]

        # Optional: exclude Technician roles
        if exclude_techs:
            raw = raw[~raw["title"].str.contains(r"\btechnician\b", case=False, na=False)]

        # Drop software/test automation
        raw = raw[~raw.apply(lambda r: looks_software_automation(f'{r.get("title","")} {r.get("description","")}'), axis=1)]

        # Require manufacturing/industrial controls hints
        raw = raw[raw.apply(lambda r: looks_mfg_controls(f'{r.get("title","")} {r.get("description","")}'), axis=1)]

        # US-only
        if us_only:
            raw = raw[raw.apply(lambda r: is_us_location(r.get("location",""), r.get("location_area") or []), axis=1)]

        # Agencies
        if exclude_agencies:
            raw = raw[~raw["company"].apply(lambda x: is_staffing_agency(x, extra_agencies_set))]

        # Clean & dedupe
        for c in ["company","title","location","posted_at","url","feed"]:
            if c in raw.columns:
                raw[c] = raw[c].fillna("").astype(str).str.strip()
        raw = raw.drop_duplicates(subset=["company","title","location","url"], keep="first")

        if not raw.empty:
            df = raw[["company","title","location","posted_at","url","feed"]].copy()
            st.session_state["_last_controls"] = df.copy()
    elif st.session_state.get("_last_controls") is not None:
        df = st.session_state["_last_controls"].copy()
        st.info("Showing cached results (no new data fetched).")

# ---------------------------
# Company-first view
# ---------------------------
c1, c2, c3, c4 = st.columns(4)
if not df.empty:
    with c1: st.metric("Open roles", int(df.shape[0]))
    with c2: st.metric("Hiring companies", int(df["company"].nunique()))
    with c3: st.metric("Unique locations", int(df["location"].nunique()))
    with c4: st.metric("Feeds", ", ".join(sorted(df["feed"].unique())) if "feed" in df.columns else "—")

    st.subheader("Top Companies by Open Roles (Controls/Automation)")
    top_companies = df.groupby("company").size().sort_values(ascending=False)
    st.bar_chart(top_companies.head(25))

    st.subheader("Company → Open Roles (Count)")
    company_counts = top_companies.reset_index()
    company_counts.columns = ["company","open_roles"]
    st.dataframe(company_counts, use_container_width=True, hide_index=True)

    st.subheader("All Results")
    st.dataframe(df.sort_values(["company","title"]), use_container_width=True, hide_index=True)

    st.download_button(
        label="Download CSV",
        data=df.to_csv(index=False).encode("utf-8"),
        file_name="bd_us_controls_automation_no_software.csv",
        mime="text/csv",
    )
else:
    with c1: st.metric("Open roles", 0)
    with c2: st.metric("Hiring companies", 0)
    with c3: st.metric("Unique locations", 0)
    with c4: st.metric("Feeds", "—")
    st.info("No jobs found. Enable Web Discovery / Watchlist, reduce filters, or lower Adzuna pages.")
