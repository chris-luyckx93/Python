# scrape_dutchbros.py
import csv
import json
import sys
from typing import Any, Dict, List, Optional
import requests

# If your DevTools shows a different URL, paste it below.
# Commonly this is "https://www.dutchbros.com/stands.json"
STANDS_URL = "https://files.dutchbros.com/api-cache/stands.json"

# Friendly headers (some CDNs are picky)
HEADERS = {
    "accept": "application/json, text/plain, */*",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "referer": "https://www.dutchbros.com/locations",
}

def get_json(url: str) -> Any:
    """GET JSON with simple headers; raise for HTTP errors."""
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    # Some endpoints return JSON as text; requests should parse automatically,
    # but we’ll be robust:
    try:
        return r.json()
    except json.JSONDecodeError:
        return json.loads(r.text)

def first_nonempty(*vals):
    """Return the first non-empty value from a list of candidates."""
    for v in vals:
        if v not in (None, "", []):
            return v
    return None

def normalize_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    stand_address = first_nonempty(
        rec.get("stand_address"),
        rec.get("address"),
        rec.get("street"),
    )
    stand_address2 = first_nonempty(
        rec.get("stand_address2"),
        rec.get("address2"),
        rec.get("street2"),
    )

    lat = first_nonempty(
        rec.get("lat"), rec.get("latitude"),
        (rec.get("coordinates") or {}).get("lat"),
        (rec.get("location") or {}).get("lat"),
    )
    lon = first_nonempty(
        rec.get("lon"), rec.get("longitude"),
        (rec.get("coordinates") or {}).get("lon"),
        (rec.get("location") or {}).get("lon"),
    )

    hours = first_nonempty(rec.get("hours"), rec.get("hours_text"))
    schedule_array = rec.get("schedule_array")
    schedule_json = json.dumps(schedule_array) if schedule_array else None

    row = {
        "id": first_nonempty(rec.get("new_co_id"), rec.get("id")),
        "store_number": first_nonempty(rec.get("store_number"), rec.get("number")),
        "store_code": first_nonempty(rec.get("store_code"), rec.get("code")),
        "store_nickname": first_nonempty(rec.get("store_nickname"), rec.get("name")),
        "drivethru": first_nonempty(rec.get("drivethru"), rec.get("drive_thru")),
        "walkup_window": first_nonempty(rec.get("walkup_window"), rec.get("walk_up")),
        "line1": stand_address,
        "line2": stand_address2,
        "city": first_nonempty(rec.get("city")),
        "state": first_nonempty(rec.get("state"), rec.get("region")),
        "postalCode": first_nonempty(rec.get("zip_code"), rec.get("postalCode"), rec.get("zip")),
        "country": first_nonempty(rec.get("country"), "US"),
        "lat": lat,
        "lon": lon,  # <-- changed from "lng"
        "hours": hours,
        "schedule": schedule_json,
    }
    return row

def main():
    try:
        data = get_json(STANDS_URL)
    except Exception as e:
        print(f"Failed to fetch JSON from {STANDS_URL}:\n  {e}")
        sys.exit(1)

    # The endpoint is usually a list of stand dicts; sometimes it's wrapped.
    # Handle both shapes.
    if isinstance(data, dict):
        # Look for common wrapper keys
        for key in ("stands", "data", "items", "locations"):
            if isinstance(data.get(key), list):
                data = data[key]
                break

    if not isinstance(data, list):
        print("Unexpected JSON shape (not a list). Try opening DevTools → Network, "
              "click stands.json, right-click → Copy → Copy link address, and "
              "paste that into STANDS_URL.")
        sys.exit(1)

    rows: List[Dict[str, Any]] = []
    for rec in data:
        if not isinstance(rec, dict):
            continue
        row = normalize_record(rec)

        # Optional: keep **only** US (drop CA/MX if present)
        if row.get("country") and row["country"] != "US":
            continue

        rows.append(row)

    if not rows:
        print("No rows found after normalization.")
        sys.exit(1)

    # Output CSV
    out = "dutchbros_locations.csv"
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

    print(f"Saved {len(rows)} locations → {out}")

if __name__ == "__main__":
    main()
