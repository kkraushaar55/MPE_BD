# app.py — Controls/Automation jobs via Adzuna (clean + reliable)
# - Adzuna-only (no Bing, no ATS)
# - Small, targeted queries (batched) to avoid long URLs
# - Newest first, CSV export, solid debugging
# - Works locally and on Streamlit Cloud

import os
import re
import time
import requests
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

# --------- Setup ----------
load_dotenv()
st.set_page_config(page_title="Controls & Automation Jobs (Adzuna)", layout="wide")
st.title("🎯 Controls & Automation Jobs — Adzuna")
st.caption("Targets core Controls/Automation/Robotics roles. Sorted by newest first.")

# --------- Secrets ----------
def _get_secret(name: str, default: str = "") -> str:
    try:
        if hasattr(st, "secrets") and name in st.secrets:
            return str(st.secrets[name])
    except Exception:
        pass
    return os.getenv(name, default)

ADZUNA_APP_ID  = _get_secret("ADZUNA_APP_ID")
ADZUNA_APP_KEY = _get_secret("ADZUNA_APP_KEY")

USER_AGENT = "Mozilla/5.0 (Manpower Engineering — Controls/Automation Finder)"
TIMEOUT = 20
DEFAULT_COUNTRY = "us"  # Adzuna market
DEFAULT_CATEGORY = "engineering-jobs"  # can be toggled off

# --------- Target TERM GROUPS (short lists = safer queries) ----------
GROUPS = {
    "Core Titles": [
        "Controls Engineer", "Automation Engineer", "Electrical Controls Engineer",
        "Control Systems Engineer", "PLC Engineer", "Robotics Engineer",
        "Mechatronics Engineer", "SCADA Engineer", "Process Controls Engineer",
        "Instrumentation & Controls Engineer", "Motion Control Engineer",
        "Automation Technician", "Automation Specialist",
    ],
    "PLC / Vendors": [
        "Allen Bradley", "Rockwell Automation", "Siemens", "TIA Portal",
        "Mitsubishi", "Omron", "Schneider Electric", "GE Fanuc", "Beckhoff", "TwinCAT", "CODESYS",
    ],
    "Robotics": [
        "FANUC", "ABB Robot", "KUKA", "Yaskawa", "Motoman",
        "Universal Robots", "UR Robot", "Kawasaki Robotics", "Staubli", "Epson Robot",
    ],
    "SCADA / HMI": [
        "SCADA", "HMI", "Ignition", "Inductive Automation", "Wonderware", "AVEVA",
        "FactoryTalk View", "WinCC", "GE iFIX", "Cimplicity", "CitectSCADA",
    ],
    "Motion / Drives": [
        "Servo Drives", "VFD", "Variable Frequency Drives", "Motion Control",
        "Bosch Rexroth", "Yaskawa Drives",
    ],
    "Manufacturing / ICS": [
        "Industrial Automation", "Manufacturing Systems", "Industrial Control Systems",
        "DCS", "Machine Vision", "Sensors & Instrumentation", "System Integration",
        "Commissioning", "UL 508A", "PID Control",
    ],
}

# --------- Minimal sanity filter (keeps scope tight) ----------
TITLE_KEEP = re.compile(
    r"(?i)\b("
    r"controls?|automation|robotic|mechatronic|scada|plc|control systems?|process controls?|instrumentation|motion"
    r")\b"
)

def title_is_relevant(title: str) -> bool:
    if not title:
        return False
    return bool(TITLE_KEEP.search(title))

# --------- Adzuna helpers ----------
def _adzuna_search(country: str, page: int, what: str, where: str, max_days_old: int, use_category: bool):
    base = f"https://api.adzuna.com/v1/api/jobs/{country}/search/{page}"
    params = {
        "app_id": ADZUNA_APP_ID,
        "app_key": ADZUNA_APP_KEY,
        "results_per_page": 50,
        "what": what,
        "where": where,
        "max_days_old": max_days_old,
        "content-type": "application/json",
    }
    if use_category:
        params["category"] = DEFAULT_CATEGORY
    r = requests.get(base, params=params, timeout=TIMEOUT, headers={"User-Agent": USER_AGENT})
    r.raise_for_status()
    return (r.json() or {})

def fetch_group(country: str, where: str, max_days_old: int, pages: int, terms: list[str], use_category: bool) -> list[dict]:
    """Fetch one short OR-query group across N pages."""
    if not terms:
        return []
    # short OR query keeps URL small & avoids truncation
    query = "(" + " OR ".join([f'"{t}"' for t in terms]) + ")"
    out = []
    for p in range(1, pages + 1):
        try:
            data = _adzuna_search(country, p, query, where, max_days_old, use_category)
        except Exception as e:
            st.warning(f"Adzuna error (group='{terms[0]}…', page={p}): {e}")
            break
        results = data.get("results") or []
        for j in results:
            loc = j.get("location") or {}
            out.append({
                "feed": "adzuna",
                "company": ((j.get("company") or {}).get("display_name") or "")[:200],
                "title": (j.get("title") or "")[:300],
                "location": (loc.get("display_name") or "")[:200],
                "posted_at": j.get("created") or "",
                "url": j.get("redirect_url") or "",
                "description": (j.get("description") or "")[:2000],
            })
        # be polite
        time.sleep(0.15)
    return out

def fetch_all_selected(country: str, where: str, max_days_old: int, pages: int, selected_groups: list[str], use_category: bool) -> pd.DataFrame:
    if not (ADZUNA_APP_ID and ADZUNA_APP_KEY):
        return pd.DataFrame()
    rows = []
    for gname in selected_groups:
        rows.extend(fetch_group(country, where, max_days_old, pages, GROUPS[gname], use_category))
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    # basic clean + dedupe
    for c in ["company", "title", "location", "posted_at", "url"]:
        if c in df.columns:
            df[c] = df[c].fillna("").astype(str).str.strip()
    df = df.drop_duplicates(subset=["title", "company", "location", "url"], keep="first")
    return df

# --------- Sidebar ----------
with st.sidebar:
    st.header("Adzuna Status")
    if ADZUNA_APP_ID and ADZUNA_APP_KEY:
        st.success(f"Configured (ID …{ADZUNA_APP_ID[-2:]})")
    else:
        st.error("Missing ADZUNA_APP_ID / ADZUNA_APP_KEY in Secrets (.env or Streamlit).")

    st.divider()
    st.header("Search Scope")
    country = st.selectbox("Country market", ["us"], index=0)
    where = st.text_input("Location filter", value="United States")
    max_days_old = st.slider("Max days old", 1, 60, 45)
    pages = st.slider("Pages per group (x50 each)", 1, 12, 4)
    use_category = st.checkbox("Restrict to Adzuna 'engineering-jobs' category", value=True)

    st.divider()
    st.header("Target Groups")
    default_groups = ["Core Titles", "PLC / Vendors", "Robotics"]
    selected_groups = st.multiselect(
        "Pick the groups to query (short OR lists, batched)",
        options=list(GROUPS.keys()),
        default=default_groups,
    )

    st.divider()
    st.header("Display")
    top_n = st.slider("Show newest N", 10, 500, 150, step=10)

    st.divider()
    st.header("Diagnostics")
    if st.button("Adzuna smoke test (1 page: 'Controls Engineer')"):
        try:
            data = _adzuna_search(country, 1, '"Controls Engineer"', where, 45, use_category=True)
            count = len((data or {}).get("results") or [])
            st.write("HTTP OK. Rows:", count)
            if count:
                small = pd.DataFrame([{
                    "company": (r.get("company") or {}).get("display_name"),
                    "title": r.get("title"),
                    "location": (r.get("location") or {}).get("display_name"),
                    "posted_at": r.get("created"),
                    "url": r.get("redirect_url"),
                } for r in data["results"]]).head(25)
                st.dataframe(small, use_container_width=True, hide_index=True)
            else:
                st.warning("Zero rows. Increase max days or turn OFF category restriction.")
        except Exception as e:
            st.error(f"Smoke test failed: {e}")

    run = st.button("Fetch Jobs")

# --------- Run search ----------
if run:
    df = fetch_all_selected(country, where, max_days_old, pages, selected_groups, use_category)

    # raw debug
    with st.expander("Raw inbound (before relevance filter)"):
        st.metric("Rows fetched", 0 if df.empty else int(df.shape[0]))
        if not df.empty:
            if "posted_at" in df.columns:
                st.write(df[["company","title","location","posted_at","url"]].head(30))
        else:
            st.info("No rows from Adzuna. Try: increase 'Max days old', increase 'Pages', or uncheck category.")

    if not df.empty:
        # Relevance: keep likely Controls/Automation titles (light touch)
        df = df[df["title"].apply(title_is_relevant)]

        # Recency & sort
        df["posted_at"] = pd.to_datetime(df["posted_at"], errors="coerce", utc=True)
        now_ts = pd.Timestamp.utcnow()
        df.loc[df["posted_at"].isna(), "posted_at"] = now_ts
        df = df.sort_values("posted_at", ascending=False, na_position="last")

        # Display
        st.subheader(f"Newest {min(top_n, len(df))} roles (last {max_days_old} days)")
        view_cols = [c for c in ["company","title","location","posted_at","url"] if c in df.columns]
        st.dataframe(df[view_cols].head(top_n), use_container_width=True, hide_index=True)

        st.download_button(
            "Download CSV",
            df[view_cols].head(top_n).to_csv(index=False).encode("utf-8"),
            file_name="controls_automation_adzuna.csv",
            mime="text/csv",
