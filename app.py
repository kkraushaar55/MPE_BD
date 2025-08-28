# app.py — Adzuna-only Controls & Automation Job Finder

import os, re, requests, pandas as pd, streamlit as st
from dotenv import load_dotenv

# ---------------- Setup ----------------
load_dotenv()
st.set_page_config(page_title="Controls & Automation Jobs", layout="wide")
st.title("🎯 Controls & Automation Job Finder (Adzuna-only)")
st.caption("Targeting core controls, automation, robotics, and manufacturing roles via Adzuna API.")

# ---------------- Secrets ----------------
def _get_secret(name: str, default: str = "") -> str:
    try:
        if hasattr(st, "secrets") and name in st.secrets:
            return str(st.secrets[name])
    except Exception:
        pass
    return os.getenv(name, default)

ADZUNA_APP_ID  = _get_secret("ADZUNA_APP_ID")
ADZUNA_APP_KEY = _get_secret("ADZUNA_APP_KEY")

USER_AGENT = "Mozilla/5.0 (ControlsAutomationScraper)"
TIMEOUT = 20
ADZUNA_CATEGORY = "engineering-jobs"

# ---------------- Target Queries ----------------
# Core roles, platforms, robotics, manufacturing/automation keywords
TARGET_TERMS = [
    # 🎯 Core job titles
    "Controls Engineer", "Automation Engineer", "Electrical Controls Engineer",
    "Manufacturing Controls Engineer", "Industrial Controls Engineer",
    "Automation Controls Specialist", "Control Systems Engineer",
    "PLC Engineer", "Robotics Engineer", "Mechatronics Engineer",
    "SCADA Engineer", "Process Controls Engineer",
    "Instrumentation & Controls Engineer", "Motion Control Engineer",
    "Automation Technician", "Automation Specialist",

    # ⚡ Programs / Platforms
    "PLC Programming", "Allen Bradley", "Rockwell Automation", "Siemens",
    "Mitsubishi", "Omron", "Schneider Electric", "GE Fanuc", "Beckhoff", "TwinCAT",
    "B&R Automation", "ABB", "Robotics", "FANUC", "KUKA", "Yaskawa", "Universal Robots",
    "Kawasaki Robotics", "Staubli", "Epson", "SCADA", "HMI", "Wonderware", "Ignition",
    "FactoryTalk", "WinCC", "CitectSCADA",

    # 🏭 Manufacturing / Automation
    "Industrial Automation", "Manufacturing Systems", "ICS", "DCS",
    "Machine Vision", "Sensors", "Robotics Integration", "AGV", "Conveyors",
    "Pick and Place", "Material Handling", "Injection Molding Automation",
    "Assembly Line Automation", "Packaging Equipment Automation",
    "High Mix Low Volume", "Continuous Process Control",
    "Lean Manufacturing", "Six Sigma Automation",
    "Predictive Maintenance", "IIoT", "MES", "Digital Twin", "Smart Factory",

    # 🔧 Tools / Skills
    "Panel Design", "AutoCAD Electrical", "EPLAN", "Instrumentation",
    "PID Control", "Safety Systems", "Machine Safety", "UL 508A",
    "Debugging", "Commissioning", "System Integration",
    "Root Cause Analysis", "Preventive Maintenance",
]

# Collapse into one OR query string
ADZUNA_QUERY = "(" + " OR ".join([f'"{term}"' for term in TARGET_TERMS]) + ")"

# ---------------- Fetch from Adzuna ----------------
@st.cache_data(ttl=1800, show_spinner=False)
def fetch_adzuna(query: str, where: str, max_days_old: int, pages: int) -> list[dict]:
    if not (ADZUNA_APP_ID and ADZUNA_APP_KEY): 
        return []
    results_all = []
    base = "https://api.adzuna.com/v1/api/jobs/us/search/"
    for p in range(1, pages+1):
        params = {
            "app_id": ADZUNA_APP_ID, "app_key": ADZUNA_APP_KEY,
            "results_per_page": 50, "what": query, "where": where,
            "category": ADZUNA_CATEGORY, "max_days_old": max_days_old,
            "content-type": "application/json",
        }
        try:
            r = requests.get(base + str(p), params=params, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT)
            r.raise_for_status()
            data = r.json() or {}
            for j in data.get("results", []):
                loc = j.get("location") or {}
                results_all.append({
                    "company": (j.get("company") or {}).get("display_name"),
                    "title": j.get("title"),
                    "location": loc.get("display_name"),
                    "posted_at": j.get("created"),
                    "url": j.get("redirect_url"),
                    "description": j.get("description"),
                    "feed": "adzuna"
                })
        except Exception as e:
            st.warning(f"Adzuna error page {p}: {e}")
            break
    return results_all

# ---------------- Sidebar Controls ----------------
with st.sidebar:
    st.header("Adzuna Config")
    if ADZUNA_APP_ID and ADZUNA_APP_KEY:
        st.success(f"Adzuna: configured (ID …{ADZUNA_APP_ID[-2:]})")
    else:
        st.error("Adzuna: missing keys. Add ADZUNA_APP_ID and ADZUNA_APP_KEY in Secrets.")

    st.divider()
    location = st.text_input("Location", value="United States")
    max_days = st.slider("Max days old", 1, 60, 45)
    pages = st.slider("Pages (x50 each)", 1, 12, 6)

    st.divider()
    top_n = st.slider("Show top N newest", 10, 300, 100, step=10)

    run = st.button("Fetch Jobs")

# ---------------- Run ----------------
if run:
    jobs = fetch_adzuna(query=ADZUNA_QUERY, where=location, max_days_old=max_days, pages=pages)
    df = pd.DataFrame(jobs)

    if not df.empty:
        # Parse posted_at → datetime for sort
        df["posted_at"] = pd.to_datetime(df["posted_at"], errors="coerce", utc=True)
        now_ts = pd.Timestamp.utcnow()
        df.loc[df["posted_at"].isna(), "posted_at"] = now_ts
        df = df.sort_values("posted_at", ascending=False)

        st.subheader(f"Newest {top_n} Controls/Automation Roles (last {max_days} days)")
        st.dataframe(df[["company","title","location","posted_at","url"]].head(top_n), 
                     use_container_width=True, hide_index=True)

        st.download_button("Download CSV", df.to_csv(index=False).encode("utf-8"),
                           file_name="controls_automation_jobs.csv", mime="text/csv")
    else:
        st.warning("No jobs found. Try increasing max days, pages, or broadening location.")
