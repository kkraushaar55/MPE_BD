# app.py — Controls/Automation Jobs (ATS + Adzuna + Web Discovery) — Newest First

import os, re, json, time, requests, pandas as pd, streamlit as st
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser
from dotenv import load_dotenv

from ats_providers import (
    fetch_greenhouse_jobs, fetch_lever_jobs, fetch_smartrecruiters_jobs,
    fetch_ashby_jobs, fetch_workable_jobs, fetch_workday_jobs,
)

# ---------- App setup & secrets ----------
load_dotenv()
st.set_page_config(page_title="US Controls & Industrial Automation", layout="wide")
st.title("US Controls & Industrial Automation — Engineers + Leadership (No Agencies)")
st.caption("Sources: ATS (Greenhouse/Lever/SmartRecruiters/Ashby/Workable/Workday) + Adzuna + Web Discovery.")

def _get_secret(name: str, default: str = "") -> str:
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

USER_AGENT = "Mozilla/5.0 (BD Controls Dashboard)"
TIMEOUT = 20
SAFE_DAILY_BUDGET = 150
ADZUNA_CATEGORY = "engineering-jobs"

# ---------- Targeting ----------
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
    "control system","control systems","automation system","automation systems","instrumentation",
    "plc","hmi","scada","dcs","ladder logic","iec 61131","iec-61131","contrologix","compactlogix","studio 5000",
    "rslogix","panelview","rockwell","allen-bradley","allen bradley","siemens","tia portal","s7",
    "beckhoff","codesys","mitsubishi","omron","yaskawa","fanuc","abb robot","kuka","ur robot","robot"
}

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

US_STATE_ABBR = {"AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA","KS","KY","LA","ME",
                 "MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA",
                 "RI","SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY","DC"}
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
        "category": "engineering-jobs", "max_days_old": max_days_old,
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
                "location_area": loc.get("area") or [],
                "posted_at": normalize_text(j.get("created")),
                "url": j.get("redirect_url"),
                "description": normalize_text(j.get("description")),
            })
    return out

# ---------- Web Discovery (Bing → employer pages → JSON-LD JobPosting) ----------
BLOCKED_DOMAINS = {
    "linkedin.com","indeed.com","glassdoor.com","ziprecruiter.com","monster.com",
    "simplyhired.com","talent.com","snagajob.com","careerbuilder.com"
}
_robots_cache: dict[str, RobotFileParser] = {}

def can_fetch(url: str) -> bool:
    try:
        host = urlparse(url).netloc.lower()
        if any(host.endswith(d) for d in BLOCKED_DOMAINS): return False
        if host not in _robots_cache:
            rp = RobotFileParser()
            rp.set_url(f"https://{host}/robots.txt")
            try: rp.read()
            except Exception:
                _robots_cache[host] = rp; return True
            _robots_cache[host] = rp
        return _robots_cache[host].can_fetch(USER_AGENT, url)
    except Exception:
        return False

def bing_search(q: str, count: int = 10, mkt: str = "en-US") -> list[dict]:
    if not BING_SEARCH_KEY: return []
    headers = {"Ocp-Apim-Subscription-Key": BING_SEARCH_KEY}
    params = {"q": q, "count": count, "mkt": mkt, "responseFilter":"Webpages"}
    try:
        r = requests.get(BING_ENDPOINT, headers=headers, params=params, timeout=TIMEOUT); r.raise_for_status()
        return (r.json().get("webPages") or {}).get("value", []) or []
    except Exception:
        return []

def discover_controls_pages(per_role: int = 8) -> list[str]:
    queries = [
        '( "controls engineer" OR "automation engineer" ) ( PLC OR SCADA OR HMI OR DCS ) ( careers OR jobs ) site:*.com -site:linkedin.com -site:indeed.com -site:glassdoor.com -site:ziprecruiter.com -site:monster.com -site:simplyhired.com -site:talent.com -site:snagajob.com',
        '( "controls manager" OR "automation manager" OR "director of controls" ) ( PLC OR SCADA OR HMI OR DCS ) ( careers OR jobs ) site:*.com -site:linkedin.com -site:indeed.com -site:glassdoor.com -site:ziprecruiter.com -site:monster.com -site:simplyhired.com -site:talent.com -site:snagajob.com',
        '( "instrumentation & controls" OR "I&C engineer" ) ( manufacturing OR industrial OR plant ) ( careers OR jobs ) site:*.com -site:linkedin.com -site:indeed.com -site:glassdoor.com -site:ziprecruiter.com -site:monster.com -site:simplyhired.com -site:talent.com -site:snagajob.com',
    ]
    urls = []
    for q in queries:
        for item in bing_search(q, count=per_role*2):
            url = item.get("url")
            if not url: continue
            host = urlparse(url).netloc.lower()
            if any(host.endswith(d) for d in BLOCKED_DOMAINS): continue
            if not re.search(r"/careers?|/jobs?|/join|/opportunit|/vacanc", url, re.I): continue
            urls.append(url)
            if len(urls) >= per_role: break
    return urls

def parse_jobposting_from_html(url: str) -> list[dict]:
    if not can_fetch(url): return []
    try:
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT)
        if r.status_code != 200: return []
        soup = BeautifulSoup(r.text, "lxml")
        out = []
        for s in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(s.string or "{}")
            except Exception:
                continue
            items = data if isinstance(data, list) else [data]
            for obj in items:
                if not isinstance(obj, dict): continue
                t = obj.get("@type") or obj.get("type")
                if (isinstance(t, list) and not any(str(x).lower()=="jobposting" for x in t)) or (isinstance(t,str) and t.lower()!="jobposting"):
                    continue

                title = normalize_text(obj.get("title"))
                desc = normalize_text(obj.get("description") or "")
                if not title or not title_is_target(title): continue
                blob = f"{title} {desc}"
                if looks_software_automation(blob): continue
                if not looks_mfg_controls(blob): continue

                org = obj.get("hiringOrganization") or {}
                company = normalize_text(org.get("name") or urlparse(url).netloc.split(":")[0])

                loc = obj.get("jobLocation") or {}
                if isinstance(loc, list) and loc: loc = loc[0]
                address = (loc.get("address") or {}) if isinstance(loc, dict) else {}
                display_loc = ", ".join(x for x in [
                    normalize_text(address.get("addressLocality")),
                    normalize_text(address.get("addressRegion")),
                    normalize_text(address.get("addressCountry")),
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
        if seen.get(host,0) >= per_domain_cap: continue
        postings = parse_jobposting_from_html(url)
        if postings:
            seen[host] = seen.get(host,0)+1
            jobs.extend(postings)
        time.sleep(0.2)
    return jobs

# ---------- Sidebar ----------
with st.sidebar:
    st.header("Status")
    st.info("ATS: built-in")
    st.success(f"Adzuna: configured (…{ADZUNA_APP_ID[-2:]})") if (ADZUNA_APP_ID and ADZUNA_APP_KEY) else st.info("Adzuna: not configured")
    st.success("Bing Web Search: configured") if BING_SEARCH_KEY else st.info("Bing Web: not configured")

    st.divider()
    st.header("Scope")
    include_leadership = st.checkbox("Include leadership (Lead/Supervisor/Manager/Director/Head)", value=True)
    exclude_techs = st.checkbox("Exclude 'Technician' titles", value=True)

    st.divider()
    st.header("Adzuna (optional)")
    use_adzuna = st.checkbox("Use Adzuna feed", value=False, disabled=not (ADZUNA_APP_ID and ADZUNA_APP_KEY))
    adz_where = st.text_input("Adzuna location", value="United States", disabled=not use_adzuna)
    adz_max_days = st.slider("Adzuna max days old", 1, 60, 21, disabled=not use_adzuna)
    adz_pages = st.slider("Adzuna pages (x50 each)", 1, 12, 6, disabled=not use_adzuna)
    role_query_count = 2 if include_leadership else 1
    est_calls = (adz_pages if use_adzuna else 0) * role_query_count
    if est_calls > SAFE_DAILY_BUDGET:
        st.warning(f"~{est_calls} Adzuna calls > budget {SAFE_DAILY_BUDGET}. Reduce pages.")

    st.divider()
    st.header("Watchlist (ATS direct)")
    st.caption("companies.csv columns: company,ats,token,api_base,industry")
    include_watchlist = st.checkbox("Use companies.csv", value=True)
    if include_watchlist and not os.path.exists("companies.csv"):
        st.warning("companies.csv not found at repo root.")

    st.divider()
    st.header("Web Discovery (optional)")
    use_web = st.checkbox("Use Web Discovery (Bing)", value=False, disabled=not BING_SEARCH_KEY)
    per_role = st.slider("Employer pages to discover", 1, 20, 8, disabled=not use_web)
    per_domain_cap = st.slider("Max pages per domain", 1, 10, 3, disabled=not use_web)

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

# ---------- Fetch (ATS + Adzuna + Web) ----------
jobs: list[dict] = []
if run:
    # Adzuna
    if use_adzuna and ADZUNA_APP_ID and ADZUNA_APP_KEY:
        q_engineer = '(controls engineer OR automation engineer OR "instrumentation & controls engineer" OR "i&c engineer")'
        q_leader  = '(controls manager OR automation manager OR "director of controls" OR "director of automation" OR controls lead OR automation lead)'
        for q in [q_engineer] + ([q_leader] if include_leadership else []):
            jobs.extend(fetch_adzuna_controls(query=q, where=adz_where or "United States",
                                              max_days_old=adz_max_days, pages=adz_pages))

    # ATS
    if include_watchlist and os.path.exists("companies.csv"):
        wl = pd.read_csv("companies.csv", dtype=str).fillna("")
        wl.columns = [c.strip().lower() for c in wl.columns]
        need = {"company","ats","token","api_base","industry"}
        if need - set(wl.columns):
            st.error("`companies.csv` missing required columns.")
        else:
            with st.expander("Loaded companies.csv (preview)"):
                st.dataframe(wl.head(25), use_container_width=True, hide_index=True)
            for _, row in wl.iterrows():
                ats      = _clean_val(row.get("ats")).lower()
                tok      = _clean_val(row.get("token"))
                api_base = _clean_val(row.get("api_base"))
                try:
                    if ats == "greenhouse" and tok: jobs.extend(fetch_greenhouse_jobs(tok))
                    elif ats == "lever" and tok: jobs.extend(fetch_lever_jobs(tok))
                    elif ats == "smartrecruiters" and tok: jobs.extend(fetch_smartrecruiters_jobs(tok))
                    elif ats == "ashby" and tok: jobs.extend(fetch_ashby_jobs(tok))
                    elif ats == "workable" and tok: jobs.extend(fetch_workable_jobs(tok))
                    elif ats in ("workday","workday_json") and api_base:
                        wd_rows = fetch_workday_jobs(api_base=api_base, query="")
                        if isinstance(wd_rows, list): jobs.extend(wd_rows)
                        else: st.warning(f"Workday adapter returned unexpected type for {api_base}; skipping.")
                except Exception as e:
                    st.warning(f"Fetch failed for ats={ats}, token='{tok}', api_base='{api_base}': {e}")

    # Web
    if use_web and BING_SEARCH_KEY:
        jobs.extend(web_discovery(per_role=per_role, per_domain_cap=per_domain_cap))

# ---------- Debug panel (before filters) ----------
df_raw = pd.DataFrame(jobs)
with st.expander("Debug: inbound data (before filters)"):
    cA, cB, cC = st.columns(3)
    with cA: st.metric("Total rows fetched", int(df_raw.shape[0]))
    with cB: st.metric("Feeds detected", len(df_raw["feed"].unique()) if "feed" in df_raw.columns else 0)
    with cC: st.metric("Companies (raw)", df_raw["company"].nunique() if "company" in df_raw.columns else 0)
    if not df_raw.empty:
        if "feed" in df_raw.columns:
            st.dataframe(df_raw["feed"].value_counts(dropna=False).to_frame("rows"), use_container_width=True)
        st.dataframe(df_raw.head(25), use_container_width=True, hide_index=True)
    else:
        st.info("No rows fetched from any source. Enable Adzuna/Web or fix companies.csv, then run again.")

bypass_filters = st.checkbox("Temporarily bypass filters (show raw newest-first)", value=False)
if bypass_filters and not df_raw.empty:
    view_dbg = df_raw.copy()
    if "posted_at" in view_dbg.columns:
        ts = pd.to_datetime(view_dbg["posted_at"], errors="coerce", utc=True)
        view_dbg.loc[ts.isna(), "posted_at"] = pd.Timestamp.utcnow()
        view_dbg["posted_at"] = pd.to_datetime(view_dbg["posted_at"], errors="coerce", utc=True)
        view_dbg = view_dbg.sort_values("posted_at", ascending=False, na_position="last")
    cols = [c for c in ["company","title","location","posted_at","url","feed"] if c in view_dbg.columns]
    st.subheader("Raw (no filters) — newest-first")
    st.dataframe(view_dbg[cols].head(top_n), use_container_width=True, hide_index=True)
    st.stop()

# ---------- Filter → newest-first ----------
df = df_raw.copy()

if not df.empty:
    df = df[df["title"].apply(title_is_target)]
    if exclude_techs:
        df = df[~df["title"].str.contains(r"\btechnician\b", case=False, na=False)]
    df = df[~df.apply(lambda r: looks_software_automation(f'{r.get("title","")} {r.get("description","")}'), axis=1)]
    df = df[df.apply(lambda r: looks_mfg_controls(f'{r.get("title","")} {r.get("description","")}'), axis=1)]
    if us_only:
        df = df[df.apply(lambda r: is_us_location(r.get("location",""), r.get("location_area") or []), axis=1)]
    if "company" in df.columns and exclude_agencies:
        df = df[~df["company"].apply(lambda x: is_staffing_agency(x, extra_agencies_set))]

    for c in ["company","title","location","posted_at","url","feed"]:
        if c in df.columns:
            df[c] = df[c].fillna("").astype(str).str.strip()
    df = df.drop_duplicates(subset=["company","title","location","url"], keep="first")

    df["posted_at"] = pd.to_datetime(df["posted_at"], errors="coerce", utc=True)
    now_ts = pd.Timestamp.utcnow()
    df.loc[df["posted_at"].isna(), "posted_at"] = now_ts
    cutoff = now_ts - pd.Timedelta(days=lookback_days)
    df_recent = df[df["posted_at"] >= cutoff].copy().sort_values("posted_at", ascending=False, na_position="last")

    c1, c2, c3, c4 = st.columns(4)
    with c1: st.metric("Open roles (window)", int(df_recent.shape[0]))
    with c2: st.metric("Hiring companies", int(df_recent["company"].nunique()))
    with c3: st.metric("Unique locations", int(df_recent["location"].nunique()))
    with c4: st.metric("Feeds", ", ".join(sorted(df_recent["feed"].unique())) if "feed" in df_recent.columns else "—")

    st.subheader(f"Most Recent (last {lookback_days} days)")
    cols = [c for c in ["company","title","location","posted_at","url","feed"] if c in df_recent.columns]
    st.dataframe(df_recent[cols].head(top_n), use_container_width=True, hide_index=True)
    st.download_button("Download CSV", df_recent[cols].to_csv(index=False).encode("utf-8"),
                       file_name="controls_automation_recent.csv", mime="text/csv")
else:
    if run:
        st.info("No jobs survived filters. Use the debug expander or bypass filters to diagnose.")
    else:
        st.caption("Set sources/filters and click ‘Fetch Jobs’.")
