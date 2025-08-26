import requests
from .utils import normalize_text, USER_AGENT, TIMEOUT  # or inline if you don't have utils.py

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
        title = normalize_text(p.get("name"))
        loc = p.get("location") or {}
        city = normalize_text(loc.get("city")); region = normalize_text(loc.get("region")); country = normalize_text(loc.get("countryCode"))
        posted = normalize_text(p.get("releasedDate") or p.get("createdOn") or "")
        apply_url = (p.get("applyUrl") or p.get("ref", {}).get("applyUrl") or p.get("ref", {}).get("self")) or ""
        out.append({"feed":"smartrecruiters","company":normalize_text(company_slug),"title":title,
                    "location":", ".join(x for x in [city,region,country] if x),"location_area":[],
                    "posted_at":posted,"url":apply_url,"description":""})
    return out

def fetch_ashby_jobs(org_slug: str) -> list[dict]:
    url = f"https://jobs.ashbyhq.com/api/non-user-entities/job-board/{org_slug}"
    try:
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT); r.raise_for_status()
        data = r.json() or {}
    except Exception:
        return []
    out = []
    company_name = normalize_text((data.get("jobBoard") or {}).get("companyName") or org_slug)
    for j in data.get("jobs", []):
        title = normalize_text(j.get("title"))
        loc = j.get("location") or {}
        display = normalize_text(loc.get("name") or loc.get("locationText") or "")
        posted = normalize_text(j.get("publishedDate") or j.get("createdAt") or "")
        apply_url = normalize_text(j.get("jobUrl") or j.get("applyUrl") or "")
        out.append({"feed":"ashby","company":company_name,"title":title,"location":display,
                    "location_area":[], "posted_at":posted, "url":apply_url, "description":""})
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
        title = normalize_text(j.get("title"))
        loc = j.get("location") or {}
        display = ", ".join(x for x in [normalize_text(loc.get("city")), normalize_text(loc.get("region")), normalize_text(loc.get("country"))] if x)
        posted = normalize_text(j.get("published_at") or j.get("created_at") or "")
        apply_url = normalize_text(j.get("url") or j.get("application_url") or "")
        out.append({"feed":"workable","company":normalize_text(subdomain),"title":title,"location":display,
                    "location_area":[], "posted_at":posted, "url":apply_url, "description":""})
    return out

def fetch_workday_jobs(api_base: str, query: str = "", limit: int = 100) -> list[dict]:
    url = api_base.rstrip("/") + "/jobs"
    payload = {"appliedFacets": {}, "limit": limit, "offset": 0, "searchText": query}
    try:
        r = requests.post(url, json=payload, headers={"User-Agent": USER_AGENT, "Content-Type":"application/json"}, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json() or {}
    except Exception:
        return []
    out = []
    for j in data.get("jobPostings", []):
        title = normalize_text(j.get("title"))
        locs = j.get("locations") or []
        loc_display = ", ".join({normalize_text(l.get("displayName") or "") for l in locs if l}) or normalize_text(j.get("locationsText") or "")
        posted = normalize_text(j.get("postedOn") or j.get("postedDate") or "")
        apply_url = normalize_text(j.get("externalPath") or j.get("externalUrl") or "")
        if apply_url and apply_url.startswith("/"):
            root = api_base.split("/wday/")[0]
            apply_url = root.rstrip("/") + apply_url
        out.append({"feed":"workday","company":normalize_text(j.get("company","") or api_base),
                    "title":title,"location":loc_display,"location_area":[], "posted_at":posted,
                    "url":apply_url or api_base, "description":""})
    return out
