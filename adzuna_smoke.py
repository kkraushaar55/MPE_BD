# adzuna_smoke.py — minimal connectivity test (no filters)

import os, requests, pandas as pd
from dotenv import load_dotenv

load_dotenv()

ADZUNA_APP_ID  = os.getenv("ADZUNA_APP_ID") or ""
ADZUNA_APP_KEY = os.getenv("ADZUNA_APP_KEY") or ""

print("ADZUNA_APP_ID  (last 2):", ADZUNA_APP_ID[-2:] if ADZUNA_APP_ID else "<missing>")
print("ADZUNA_APP_KEY (last 4):", ADZUNA_APP_KEY[-4:] if ADZUNA_APP_KEY else "<missing>")

if not (ADZUNA_APP_ID and ADZUNA_APP_KEY):
    print("❌ Missing Adzuna keys. Set them in Streamlit Secrets or .env")
    raise SystemExit(1)

base = "https://api.adzuna.com/v1/api/jobs/us/search/1"
params = {
    "app_id": ADZUNA_APP_ID,
    "app_key": ADZUNA_APP_KEY,
    "results_per_page": 50,
    "what": 'controls engineer OR automation engineer',
    "where": "United States",
    "category": "engineering-jobs",
    "max_days_old": 45,
    "content-type": "application/json",
}
headers = {"User-Agent": "adzu-smoke-test/1.0"}

try:
    r = requests.get(base, params=params, headers=headers, timeout=20)
    print("HTTP status:", r.status_code)
    r.raise_for_status()
    data = r.json() or {}
    results = data.get("results") or []
    print("Raw results:", len(results))
    if not results:
        print("⚠️  Zero results returned by Adzuna. Keys ok, but query/location/day window returned nothing.")
    else:
        df = pd.DataFrame([{
            "company": (res.get("company") or {}).get("display_name"),
            "title": res.get("title"),
            "location": (res.get("location") or {}).get("display_name"),
            "created": res.get("created"),
            "url": res.get("redirect_url"),
        } for res in results])
        print(df.head(10).to_string(index=False))
except Exception as e:
    print("❌ Request failed:", repr(e))
