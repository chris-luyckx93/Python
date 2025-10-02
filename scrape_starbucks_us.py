# scrape_starbucks_us.py
import os, time, csv, json, requests
from typing import Dict, Any, Iterable, Tuple, List, Set

BASE_URL = "https://www.starbucks.com/apiproxy/v1/locations"

# Optional: if you copied cookies out of DevTools (one long line), export them before running:
#   export SB_COOKIE="$(cat cookie.txt)"
SB_COOKIE = os.environ.get("SB_COOKIE", "").strip()

HEADERS = {
    "accept": "application/json",
    "accept-language": "en-US,en;q=0.9",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
    ),
    "x-requested-with": "XMLHttpRequest",
    # A plain store-locator referer keeps the endpoint happy
    "referer": "https://www.starbucks.com/store-locator",
}
if SB_COOKIE:
    HEADERS["cookie"] = SB_COOKIE

# =========  DENSE METRO SEEDS (fast, high-yield)  =========
# (lat, lng, label) – tweak or add if you want
SEEDS: List[Tuple[float, float, str]] = [
    (40.7128, -74.0060,  "nyc"),
    (34.0522, -118.2437, "la"),
    (41.8781, -87.6298,  "chicago"),
    (29.7604, -95.3698,  "houston"),
    (33.7490, -84.3880,  "atlanta"),
    (47.6062, -122.3321, "seattle"),
    (37.7749, -122.4194, "sf"),
    (32.7157, -117.1611, "sandiego"),
    (39.7392, -104.9903, "denver"),
    (25.7617, -80.1918,  "miami"),
    (38.9072, -77.0369,  "dc"),
    (42.3601, -71.0589,  "boston"),
    (36.1627, -86.7816,  "nashville"),
    (35.0844, -106.6504, "albuquerque"),
    (36.1699, -115.1398, "vegas"),
    (33.4484, -112.0740, "phoenix"),
    (39.9526, -75.1652,  "philly"),
    (32.7767, -96.7970,  "dallas"),
    (29.4241, -98.4936,  "sanantonio"),
    (30.2672, -97.7431,  "austin"),
    (39.7684, -86.1581,  "indianapolis"),
    (35.2271, -80.8431,  "charlotte"),
    (35.1495, -90.0490,  "memphis"),
    (45.5152, -122.6784, "portland"),
    (44.9778, -93.2650,  "minneapolis"),
]

# =========  LOWER-48 GRID (coarse & wide)  =========
def lower48_grid(step_deg: float = 0.5) -> Iterable[Tuple[float, float, str]]:
    """
    Coarse sweep over the continental US only (excludes Alaska/Hawaii/Puerto Rico).
    Increase 'step_deg' for faster/looser, decrease for slower/tighter.
    """
    min_lat, max_lat = 24.0, 49.6
    min_lng, max_lng = -125.5, -66.9
    lat = min_lat
    while lat <= max_lat + 1e-9:
        lng = min_lng
        while lng <= max_lng + 1e-9:
            yield (round(lat, 2), round(lng, 2), "lower48")
            lng += step_deg
        lat += step_deg

# =========  API CALL  =========
session = requests.Session()
session.headers.update(HEADERS)

def fetch_near(lat: float, lng: float, limit: int = 50) -> List[Dict[str, Any]]:
    """
    Starbucks endpoint generally likes: lat, lng, place (any string is fine),
    and limit. We avoid 'radius' because it often triggers 400s.
    """
    params = {
        "lat": f"{lat:.6f}",
        "lng": f"{lng:.6f}",
        "place": "United States",
        "limit": str(limit),
    }
    r = session.get(BASE_URL, params=params, timeout=20)
    if r.status_code != 200:
        # Bubble the error for logging upstream
        raise requests.HTTPError(f"{r.status_code} {r.text[:200]}")
    return r.json()

# =========  NORMALIZE & FILTER  =========
EXCLUDE_STATES = {"AK", "HI", "PR"}  # drop Alaska, Hawaii, Puerto Rico

def extract_rows(payload: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for item in payload:
        s = (item or {}).get("store") or {}
        addr = s.get("address") or {}
        tz = s.get("timeZone") or {}
        coords = s.get("coordinates") or {}
        state = addr.get("countrySubdivisionCode")
        # filter out AK/HI/PR
        if state in EXCLUDE_STATES:
            continue
        row = {
            "id": s.get("id"),
            "storeNumber": s.get("storeNumber"),
            "name": s.get("name"),
            "phone": s.get("phoneNumber"),
            "open": s.get("open"),
            "openStatusFormatted": s.get("openStatusFormatted"),
            "hoursStatusFormatted": s.get("hoursStatusFormatted"),
            "line1": addr.get("streetAddressLine1"),
            "line2": addr.get("streetAddressLine2"),
            "line3": addr.get("streetAddressLine3"),
            "city": addr.get("city"),
            "region": state,
            "postalCode": (addr.get("postalCode") or "")[:10],
            "country": addr.get("countryCode"),
            "lat": coords.get("latitude"),
            "lng": coords.get("longitude"),
            "timezone": tz.get("timeZoneId"),
            "slug": s.get("slug"),
            "ownershipType": s.get("ownershipTypeCode"),
        }
        out.append(row)
    return out

# =========  CRAWLER  =========
def crawl_us(step_deg: float = 0.5, rate_sec: float = 0.20) -> List[Dict[str, Any]]:
    seen: Set[str] = set()
    rows: List[Dict[str, Any]] = []

    def add_batch(batch: List[Dict[str, Any]]) -> int:
        new = 0
        for r in batch:
            k = str(r.get("id") or r.get("storeNumber"))
            if not k:
                continue
            if k in seen:
                continue
            seen.add(k)
            rows.append(r)
            new += 1
        return new

    # 1) Dense metros first
    for i, (lat, lng, label) in enumerate(SEEDS, 1):
        try:
            data = fetch_near(lat, lng, limit=50)
            added = add_batch(extract_rows(data))
            print(f"[seeds]    {i:04d}/{len(SEEDS):<4} [{label:<9}] @ {lat:6.2f},{lng:7.2f}  +{added:<2}  (total {len(rows)})")
        except Exception as e:
            print(f"[seeds]    {i:04d}/{len(SEEDS):<4} [{label:<9}] @ {lat:6.2f},{lng:7.2f}  ERROR: {e}")
        time.sleep(rate_sec)

    # 2) Continental grid (coarse)
    grid_pts = list(lower48_grid(step_deg=step_deg))
    n = len(grid_pts)
    for i, (lat, lng, label) in enumerate(grid_pts, 1):
        try:
            data = fetch_near(lat, lng, limit=50)
            added = add_batch(extract_rows(data))
            if added:
                print(f"[grid]     {i:04d}/{n:<5} [{label:<9}] @ {lat:6.2f},{lng:7.2f}  +{added:<2}  (total {len(rows)})")
            elif i % 250 == 0:
                # periodic heartbeat when nothing new found
                print(f"[grid]     {i:04d}/{n:<5} [{label:<9}] @ {lat:6.2f},{lng:7.2f}  +0  (total {len(rows)})")
        except Exception as e:
            print(f"[grid]     {i:04d}/{n:<5} [{label:<9}] @ {lat:6.2f},{lng:7.2f}  ERROR: {e}")
        time.sleep(rate_sec)

    return rows

# =========  MAIN  =========
if __name__ == "__main__":
    # You can make the grid coarser/faster (0.6–0.8) or finer/slower (0.4–0.3)
    rows = crawl_us(step_deg=0.35, rate_sec=0.18)

    if not rows:
        print("No rows collected. Try a larger step_deg (slower) or confirm headers/cookie.")
        raise SystemExit(1)

    # Save CSV
    out = "starbucks_us_locations.csv"
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

    print(f"Saved {len(rows)} rows → {out}")
