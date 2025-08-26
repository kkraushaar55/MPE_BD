import requests

USER_AGENT = "Mozilla/5.0 (BD Controls ATS Dashboard)"
TIMEOUT = 20

def _nz(s):
    return " ".join(str(s or "").split())

def fetch_greenhouse_jobs(token: str) -> list[dict]:
    url = f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs"
    try:
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT); r.raise_for_status()
        data = r.json() or {}
    except Exception:
        return []
    out = []
    for j in data.get("jobs", []):
        out.append({
            "feed": "greenhouse",
            "company": _nz(token),
            "title": _nz(j.get("title")),
            "location": _nz((j.get("location") or {}).get("name")),
            "location_area": [],
            "posted_at": _nz(j.get("updated_at") or j.get("created_at") or ""),
            "url": j.get("absolute_url") or "",
            "description": ""
        })
    return out

def fetch_lever_jobs(token: str) -> list[dict]:
    url = f"https://api.lever.co/v0/postings/{token}?mode=json"
    try:
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT); r.raise_for_status()
        data = r.json() or []
    except Exception:
        return []
    out = []
    for j in data:
        cats = j.get("categories") or {}
        out.append({
            "feed": "lever",
            "company": _nz(token),
            "title": _nz(j.get("text")),
            "location": _nz(cats.get("location") or ""),
            "location_area": [],
            "posted_at": _nz(j.get("createdAt") or j.get("updatedAt") or ""),
            "url": j.get("hostedUrl") or j.get("applyUrl") or j.get("url") or "",
            "description": ""
        })
    return out

def fetch_smartrecruiters_jobs(company_slug: str) -> list[dict]:
    base = f"https://api.smartrecruiters.com/v1/companies/{company_slug}/postings"
    params = {"limit": 100}
    try:
        r = requests.get(base, params=params, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT); r.raise_for_status()
        data = r.json() or {}
    except Exception:
        return []
    out = []
    for p in data.get("content", []):
        loc = p.get("location") or {}
        city, region, country = _nz(loc.get("city")), _nz(loc.get("region")), _nz(loc.get("countryCode"))
        apply_url = (p.get("applyUrl") or (p.get("ref") or {}).get("applyUrl") or (p.get("ref") or {}).get("self")) or ""
        out.append({
            "feed": "smartrecruiters",
            "company": _nz(company_slug),
            "title": _nz(p.get("name")),
            "location": ", ".join(x for x in [city, region, country] if x),
            "location_area": [],
            "posted_at": _nz(p.get("releasedDate") or p.get("createdOn") or ""),
            "url": apply_url,
            "description": ""
        })
    return out

def fetch_ashby_jobs(org_slug: str) -> list[dict]:
    url = f"https://jobs.ashbyhq.com/api/non-user-entities/job-board/{org_slug}"
    try:
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT); r.raise_for_status()
        data = r.json() or {}
    except Exception:
        return []
    out = []
    company_name = _nz((data.get("jobBoard") or {}).get("companyName") or org_slug)
    for j in data.get("jobs", []):
        loc = j.get("location") or {}
        out.append({
            "feed": "ashby",
            "company": company_name,
            "title": _nz(j.get("title")),
            "location": _nz(loc.get("name") or loc.get("locationText") or ""),
            "location_area": [],
            "posted_at": _nz(j.get("publishedDate") or j.get("createdAt") or ""),
            "url": _nz(j.get("jobUrl") or j.get("applyUrl") or ""),
            "description": ""
        })
    return out

def fetch_workable_jobs(subdomain: str) -> list[dict]:
    url = f"https://{subdomain}.workable.com/api/v3/jobs"
    params = {"state": "published"}
    try:
        r = requests.get(url, params=params, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT); r.raise_for_status()
        data = r.json() or {}
    except Exception:
        return []
    out = []
    for j in data.get("jobs", []):
        loc = j.get("location") or {}
        display = ", ".join(x for x in [_nz(loc.get("city")), _nz(loc.get("region")), _nz(loc.get("country"))] if x)
        out.append({
            "feed": "workable",
            "company": _nz(subdomain),
            "title": _nz(j.get("title")),
            "location": display,
            "location_area": [],
            "posted_at": _nz(j.get("published_at") or j.get("created_at") or ""),
            "url": _nz(j.get("url") or j.get("application_url") or ""),
            "description": ""
        })
    return out

def fetch_workday_jobs(api_base: str, query: str = "", limit: int = 100) -> list[dict]:
    ""
