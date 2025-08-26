import requests

USER_AGENT = "Mozilla/5.0 (BD Controls ATS Dashboard)"
TIMEOUT = 20

def _nz(s):
    return " ".join(str(s or "").split())

def fetch_greenhouse_jobs(token: str) -> list[dict]:
    """token = boards.greenhouse.io/<token>"""
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
    """token = jobs.lever.co/<token>"""
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
    """slug = companies/<slug>/postings (e.g., 'eaton', 'abb', 'siemens')"""
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
    """org_slug from https://jobs.ashbyhq.com/<org_slug>"""
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
    """subdomain from https://<subdomain>.workable.com"""
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
        display = ", ".join(x for x in [
            _nz(loc.get("city")), _nz(loc.get("region")), _nz(loc.get("country"))
        ] if x)
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
    """
    api_base example:
      https://wd5.myworkdayjobs.com/wday/cxs/rockwellautomation/RA_Careers
    """
    url = api_base.rstrip("/") + "/jobs"
    payload = {"appliedFacets": {}, "limit": limit, "offset": 0, "searchText": query}
    try:
        r = requests.post(url, json=payload, headers={"User-Agent": USER_AGENT, "Content-Type": "application/json"}, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json() or {}
    except Exception:
        return []
    out = []
    for j in data.get("jobPostings", []):
        locs = j.get("locations") or []
        loc_display = ", ".join({ _nz(l.get("displayName") or "") for l in locs if l }) or _nz(j.get("locationsText") or "")
        apply_url = (j.get("externalPath") or j.get("externalUrl") or "") or api_base
        if apply_url.startswith("/"):
            root = api_base.split("/wday/")[0].rstrip("/")
            apply_url = root + apply_url
        out.append({
            "feed": "workday",
            "company": _nz(j.get("company") or api_base),
            "title": _nz(j.get("title")),
            "location": loc_display,
            "location_area": [],
            "posted_at": _nz(j.get("postedOn") or j.get("postedDate") or ""),
            "url": apply_url,
            "description": ""
        })
    return out
