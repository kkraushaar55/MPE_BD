"""
BD Jobs Dashboard — US Manufacturing Engineering & Managers (No Agencies)
Now with Web Discovery (Bing Web Search API → employer career pages → JSON-LD JobPosting)

What it does
- Adzuna API (cached, budget-capped) for broad coverage
- Watchlist (Greenhouse/Lever) — zero quota
- NEW: Web Discovery via Bing Web Search API → fetch employer career pages (NOT LinkedIn/Indeed/etc),
  parse schema.org JobPosting, filter US-only + no-agencies
- Organized by company; company counts + full results table

Secrets (Streamlit Cloud → Manage App → Settings → Secrets)
    ADZUNA_APP_ID = "..."
    ADZUNA_APP_KEY = "..."
    BING_SEARCH_KEY = "..."      # required for Web Discovery
    # Optional: BING_ENDPOINT = "https://api.bing.microsoft.com/v7.0/search" (default)

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
# Config / Secrets
# ---------------------------
load_dotenv()
ADZUNA_APP_ID = os.getenv("ADZUNA_APP_ID", "")
ADZUNA_APP_KEY = os.getenv("ADZUNA_APP_KEY", "")
BING_SEARCH_KEY = os.getenv("BING_SEARCH_KEY", "")
BING_ENDPOINT = os.getenv("BING_ENDPOINT", "https://api.bing.microsoft.com/v7.0/search")

USER_AGENT = "Mozilla/5.0 (BD Jobs Dashboard)"
TIMEOUT = 20
SAFE_DAILY_BUDGET = 200  # cap for Adzuna free tier

# ---------------------------
# Role definitions (expanded)
# ---------------------------
CORE_ENGINEERING_ROLES = [
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
    # Expanded mfg roles
    "production engineer", "production process engineer",
    "manufacturing process engineer", "process development engineer",
    "manufacturing systems engineer", "industrialization engineer",
    "new product introduction engineer", "npi engineer",
    "tooling engineer", "mold tooling engineer", "die engineer",
    "weld engineer", "welding engineer",
    "equipment engineer", "facilities engineer",
    "plastics engineer", "injection molding engineer",
    "packaging engineer", "test engineer", "manufacturing test engineer",
    "mechatronics engineer", "methods engineer",
    "lean manufacturing engineer", "sustaining engineer",
]
MANAGER_VARIANTS = [
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
    "production engineering manager", "production manager",
    "process development manager", "manufacturing systems manager",
    "industrialization manager", "npi manager", "new product introduction manager",
    "tooling manager", "molding manager", "weld engineering manager",
    "equipment engineering manager", "facilities engineering manager",
    "packaging engineering manager", "test engineering manager",
    "mechatronics manager", "methods engineering manager",
    "lean manufacturing manager", "sustaining engineering manager",
]

TITLE_REGEX = re.compile(
    r"""(?ix)\b(
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
    )\b"""
)

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

BLOCKED_DOMAINS = {
    # Never scrape these: banned by ToS or aggregators we avoid
    "linkedin.com","indeed.com","glassdoor.com","ziprecruiter.com","monster.com",
    "simplyhired.com","talent.com","snagajob.com","careerbuilder.com"
}

# ---------------------------
# Helpers
# ---------------------------
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

# robots.txt cache
_robots_cache: dict[str, RobotFileParser] = {}

def can_fetch(url: str) -> bool:
    try:
        netloc = urlparse(url).netloc
        domain = netloc.lower()
        if any(domain.endswith(bd) for bd in BLOCKED_DOMAINS):
            return False
        if domain not in _robots_cache:
            rp = RobotFileParser()
            rp.set_url(f"https://{domain}/robots.txt")
            try:
                rp.read()
            except Exception:
                # If robots can't be read, be conservative and allow only if not blocked domain
                _robots_cache[domain] = rp
                return True
            _robots_cache[domain] = rp
        return _robots_cache[domain].can_fetch(USER_AGENT, url)
    except Exception:
        return False

# ---------------------------
# Adzuna (cached pages, budget-capped)
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
        return []
    jobs = []
    for page in range(1, pages + 1):
        try:
            results = _adzuna_page(query, where, max_days_old, page)
        except requests.HTTPError as e:
            code = getattr(e.response, "status_code", "HTTP")
            st.error(f"Adzuna error (HTTP {code}). Reduce pages/roles or try later.")
            if code in (401, 403, 429):
                st.session_state["_adzuna_quota_hit"] = True
                break
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
# Watchlist (Greenhouse/Lever) — zero quota
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
# Web Discovery (Bing → employer pages → JSON-LD)
# ---------------------------
def bing_search(query: str, count: int = 10, mkt: str = "en-US") -> list[dict]:
    if not BING_SEARCH_KEY:
        return []
    headers = {"Ocp-Apim-Subscription-Key": BING_SEARCH_KEY}
    params = {"q": query, "count": count, "mkt": mkt, "responseFilter": "Webpages"}
    try:
        r = requests.get(BING_ENDPOINT, headers=headers, params=params, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json()
        return (data.get("webPages") or {}).get("value", [])
    except Exception:
        return []

def discover_job_pages_for_role(role: str, per_role: int = 8) -> list[str]:
    """
    Use Bing to find employer career pages mentioning the role.
    We exclude aggregator domains and obvious job boards we won't scrape.
    """
    query = f'"{role}" (careers OR jobs OR job) (apply OR hiring) site:*.com -site:linkedin.com -site:indeed.com -site:glassdoor.com -site:ziprecruiter.com -site:monster.com -site:simplyhired.com -site:talent.com -site:snagajob.com'
    results = bing_search(query, count=per_role * 2)
    urls = []
    for item in results:
        url = item.get("url")
        if not url:
            continue
        host = urlparse(url).netloc.lower()
        if any(host.endswith(bd) for bd in BLOCKED_DOMAINS):
            continue
        # Prefer likely careers subpaths
        if not re.search(r"/careers?|/jobs?|/join|/opportunit|/vacanc", url, re.I):
            continue
        urls.append(url)
        if len(urls) >= per_role:
            break
    return urls

def parse_jobposting_from_html(url: str) -> list[dict]:
    """
    Fetch a page, parse JSON-LD JobPosting entries.
    """
    if not can_fetch(url):
        return []
    try:
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT)
        if r.status_code != 200:
            return []
        soup = BeautifulSoup(r.text, "lxml")
        scripts = soup.find_all("script", type="application/ld+json")
        out = []
        for s in scripts:
            try:
                data = json.loads(s.string or "{}")
            except Exception:
                continue
            items = data if isinstance(data, list) else [data]
            for obj in items:
                if not isinstance(obj, dict):
                    continue
                t = obj.get("@type") or obj.get("type")
                if isinstance(t, list):
                    is_job = any(x.lower() == "jobposting" for x in map(str, t))
                else:
                    is_job = str(t).lower() == "jobposting"
                if not is_job:
                    continue

                title = normalize_text(obj.get("title"))
                if not title or not title_is_target(title):
                    continue

                org = obj.get("hiringOrganization") or {}
                company = normalize_text(org.get("name") or urlparse(url).netloc.split(":")[0])

                loc = obj.get("jobLocation") or {}
                # jobLocation can be dict or list
                if isinstance(loc, list) and loc:
                    loc = loc[0]
                address = (loc.get("address") or {}) if isinstance(loc, dict) else {}
                locality = normalize_text(address.get("addressLocality"))
                region = normalize_text(address.get("addressRegion"))
                country = normalize_text(address.get("addressCountry"))
                display_loc = ", ".join(x for x in [locality, region, country] if x)

                date_posted = normalize_text(obj.get("datePosted") or "")
                apply_url = normalize_text(obj.get("hiringOrganization", {}).get("sameAs") or obj.get("url") or url)

                out.append({
                    "feed": "web",
                    "company": company,
                    "title": title,
                    "location": display_loc or "",
                    "location_area": [],
                    "posted_at": date_posted,
                    "url": apply_url if apply_url.startswith("http") else url,
                    "description": "",
                })
        return out
    except Exception:
        return []

def web_discovery(role_queries: list[str], per_role: int = 8, per_domain_cap: int = 3) -> list[dict]:
    jobs = []
    seen_per_domain: dict[str, int] = {}
    for role in role_queries:
        for url in discover_job_pages_for_role(role, per_role=per_role):
            host = urlparse(url).netloc.lower()
            if seen_per_domain.get(host, 0) >= per_domain_cap:
                continue
            postings = parse_jobposting_from_html(url)
            if postings:
                seen_per_domain[host] = seen_per_domain.get(host, 0) + 1
                jobs.extend(postings)
            # small polite pause
            time.sleep(0.2)
    return jobs

# ---------------------------
# UI
# ---------------------------
st.set_page_config(page_title="BD Jobs — Mfg Engineering & Managers", layout="wide")
st.title("US Manufacturing Engineering & Manager Hiring (No Agencies)")
st.caption("Adzuna + optional Watchlist (Greenhouse/Lever) + Web Discovery (Bing → employer sites). Company-first view. Caching protects API quota.")

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
    st.header("Adzuna")
    use_adzuna = st.checkbox("Use Adzuna feed", value=True)
    location_hint = st.text_input("Location filter (Adzuna)", value="United States")
    us_only = st.checkbox("US only (all feeds)", value=True)
    max_days_old = st.slider("Max days old (Adzuna)", 1, 60, 21)
    pages = st.slider("Adzuna pages (x50 / role)", 1, 15, 6)
    estimated_requests = max(1, len(role_queries or (CORE_ENGINEERING_ROLES + MANAGER_VARIANTS))) * (pages if use_adzuna else 0)
    if estimated_requests > SAFE_DAILY_BUDGET:
        st.warning(f"~{estimated_requests} Adzuna calls; consider fewer roles/pages (budget {SAFE_DAILY_BUDGET}/day).")

    st.divider()
    st.header("Watchlist (zero quota)")
    include_watchlist = st.checkbox("Include companies.csv (Greenhouse/Lever)", value=False)
    st.caption("Add companies.csv at repo root with: company,ats,token,industry")

    st.divider()
    st.header("Web Discovery (Bing API)")
    use_web = st.checkbox("Use Web Discovery", value=True)
    per_role = st.slider("Pages to discover per role (search hits)", 1, 15, 6)
    per_domain_cap = st.slider("Max pages per domain", 1, 10, 3)
    if use_web and not BING_SEARCH_KEY:
        st.info("Add BING_SEARCH_KEY in Secrets to enable Web Discovery.")

    st.divider()
    st.header("Agency filter")
    exclude_agencies = st.checkbox("Exclude staffing agencies", value=True)
    extra_agencies = st.text_area("Extra agency names (comma separated)",
                                  value="CyberCoders, Kelly Services, Aerotek, Insight Global, Robert Half")
    extra_agencies_set = {x.strip() for x in extra_agencies.split(",") if x.strip()}

    run = st.button("Fetch Jobs")

# ---------------------------
# Fetch + Filter
# ---------------------------
df = pd.DataFrame(columns=["company","title","location","posted_at","url","feed"])

if run:
    jobs: list[dict] = []

    # 1) Adzuna (broad market, capped)
    if use_adzuna and ADZUNA_APP_ID and ADZUNA_APP_KEY:
        st.session_state["_adzuna_quota_hit"] = False
        for q in (role_queries or CORE_ENGINEERING_ROLES + MANAGER_VARIANTS):
            if st.session_state.get("_adzuna_quota_hit"):
                break
            jobs.extend(fetch_adzuna_jobs(query=q, where=location_hint or "United States",
                                          max_days_old=max_days_old, pages=pages))

    # 2) Watchlist CSV (ATS)
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

    # 3) Web Discovery (Bing → employer pages)
    if use_web and BING_SEARCH_KEY:
        jobs.extend(web_discovery(role_queries or (CORE_ENGINEERING_ROLES + MANAGER_VARIANTS),
                                  per_role=per_role, per_domain_cap=per_domain_cap))

    raw = pd.DataFrame(jobs)

    if not raw.empty:
        # strict titles
        raw = raw[raw["title"].apply(title_is_target)]

        # US filter (applied to all feeds)
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
            st.session_state["_last_results"] = df.copy()
    elif st.session_state.get("_last_results") is not None:
        df = st.session_state["_last_results"].copy()
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

    st.subheader("Top Companies by Open Roles")
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
        file_name="bd_us_mfg_eng_mgr_broad_no_agencies.csv",
        mime="text/csv",
    )
else:
    with c1: st.metric("Open roles", 0)
    with c2: st.metric("Hiring companies", 0)
    with c3: st.metric("Unique locations", 0)
    with c4: st.metric("Feeds", "—")
    st.info("No jobs found. Add BING_SEARCH_KEY for Web Discovery, enable Watchlist, or reduce filters, then Fetch.")

