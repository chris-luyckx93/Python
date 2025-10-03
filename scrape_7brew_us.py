import csv
import datetime as dt
from typing import Dict, Any, List

import requests

URL = "https://7brew.com/data/locations.json"
OUT = "7brew_us_locations.csv"

# Excel serial date 1 == 1899-12-31, but Excel also treats 1900 as leap year.
# The easiest correct base for modern serials is 1899-12-30.
EXCEL_BASE = dt.date(1899, 12, 30)

def excel_serial_to_date(value: Any) -> str:
    """Convert an Excel serial (string or number) to YYYY-MM-DD; return '' if blank/invalid."""
    if value is None or str(value).strip() == "":
        return ""
    try:
        n = float(value)
        # Some rows might have decimals; floor is usually fine for opening-day
        d = EXCEL_BASE + dt.timedelta(days=int(n))
        return d.isoformat()
    except Exception:
        return ""

def fetch() -> Dict[str, Any]:
    r = requests.get(URL, timeout=30)
    r.raise_for_status()
    return r.json()

def norm_float(x: Any) -> str:
    try:
        return f"{float(str(x).strip()):.6f}"
    except Exception:
        return ""

def normalize(row: Dict[str, Any]) -> Dict[str, Any]:
    # Raw keys from their JSON
    stand_id   = row.get("Stand Id")
    frac_id    = row.get("Franchise Id")
    address    = row.get("Site Address")
    city       = row.get("City")
    state_code = row.get("State Code")
    state_name = row.get("State")
    zip_code   = row.get("Zip Code")
    country    = row.get("Country")
    phone      = row.get("Contact Phone Number")
    lat        = norm_float(row.get("Latitude"))
    lon        = norm_float(row.get("Longitude"))
    stage      = row.get("Project Stage")
    open_date  = excel_serial_to_date(row.get("Open Date"))
    g_review   = row.get("Google Review Link")
    notes      = row.get("Notes")
    photo_flag = row.get("Stand Photo")

    return {
        "stand_id": stand_id or "",
        "franchise_id": frac_id or "",
        "address": address or "",
        "city": city or "",
        "state_code": (state_code or "").upper(),
        "state": state_name or "",
        "postal_code": str(zip_code or "").strip(),
        "country": (country or "").upper(),
        "phone": phone or "",
        "lat": lat,
        "lon": lon,  # you asked for lon here instead of lng
        "project_stage": stage or "",
        "open_date": open_date,
        "google_review_link": g_review or "",
        "stand_photo": str(photo_flag or "").lower(),
        "notes": notes or "",
    }

def main():
    payload = fetch()
    data = payload.get("data") or []

    rows: List[Dict[str, Any]] = []
    seen = set()

    for item in data:
        if not isinstance(item, dict):
            continue

        # Only US
        country = str(item.get("Country") or "").strip().upper()
        if country not in {"US", "USA", "UNITED STATES"}:
            continue

        out = normalize(item)
        key = (out["stand_id"] or f"{out['address']}|{out['city']}|{out['state_code']}|{out['postal_code']}").lower()
        if key in seen:
            continue
        seen.add(key)
        rows.append(out)

    if not rows:
        print("No rows parsed from 7brew — check the endpoint or JSON keys.")
        return

    fieldnames = [
        "stand_id","franchise_id","address","city","state_code","state","postal_code","country",
        "phone","lat","lon","project_stage","open_date","google_review_link","stand_photo","notes"
    ]
    with open(OUT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

    print(f"Saved {len(rows)} US locations → {OUT}")

if __name__ == "__main__":
    main()
