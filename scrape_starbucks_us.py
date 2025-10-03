# scrape_starbucks_us_expand.py
import os, time, csv, json, math, requests
from collections import deque

BASE_URL = "https://www.starbucks.com/apiproxy/v1/locations"

SB_COOKIE = os.environ.get("SB_COOKIE", "").strip()
HEADERS = {
    "accept": "application/json",
    "accept-language": "en-US,en;q=0.9",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
    ),
    "x-requested-with": "XMLHttpRequest",
    "referer": "https://www.starbucks.com/store-locator",
}
if SB_COOKIE:
    HEADERS["cookie"] = SB_COOKIE

# ——————————————————————————————
# Smart seeds = state capitals + a few dense metros
# (lat, lon)
STATE_CAPS = [
    (32.3777, -86.3000), # AL Montgomery
    (58.3019, -134.4197),# AK Juneau (we will exclude AK later)
    (33.4484, -112.0740),# AZ Phoenix
    (34.7465, -92.2896), # AR Little Rock
    (38.5750, -121.4900),# CA Sacramento
    (39.7392, -104.9903),# CO Denver
    (41.7640, -72.6820), # CT Hartford
    (39.1582, -75.5244), # DE Dover
    (30.4383, -84.2807), # FL Tallahassee
    (33.7490, -84.3880), # GA Atlanta
    (21.3099, -157.8581),# HI Honolulu (we will exclude HI)
    (43.6178, -116.2156),# ID Boise
    (39.7980, -89.6440), # IL Springfield
    (39.7684, -86.1581), # IN Indianapolis
    (41.5911, -93.6037), # IA Des Moines
    (39.0473, -95.6752), # KS Topeka
    (38.2009, -84.8733), # KY Frankfort
    (30.4571, -91.1874), # LA Baton Rouge
    (44.3106, -69.7806), # ME Augusta
    (38.9784, -76.4922), # MD Annapolis
    (42.3601, -71.0589), # MA Boston
    (42.7335, -84.5555), # MI Lansing
    (44.9551, -93.1022), # MN St Paul
    (32.2988, -90.1848), # MS Jackson
    (38.5767, -92.1735), # MO Jefferson City
    (46.5857, -112.0184),# MT Helena
    (40.8136, -96.7026), # NE Lincoln
    (39.1638, -119.7674),# NV Carson City
    (43.2072, -71.5376), # NH Concord
    (40.2206, -74.7597), # NJ Trenton
    (35.6870, -105.9378),# NM Santa Fe
    (42.6526, -73.7562), # NY Albany
    (35.7796, -78.6382), # NC Raleigh
    (46.8209, -100.7837),# ND Bismarck
    (39.9623, -83.0007), # OH Columbus
    (35.4676, -97.5164), # OK Oklahoma City
    (44.9429, -123.0351),# OR Salem
    (40.2698, -76.8756), # PA Harrisburg
    (41.8236, -71.4222), # RI Providence
    (34.0007, -81.0348), # SC Columbia
    (44.3668, -100.3538),# SD Pierre
    (36.1627, -86.7816), # TN Nashville
    (30.2747, -97.7404), # TX Austin
    (40.7608, -111.8910),# UT Salt Lake City
    (44.2601, -72.5754), # VT Montpelier
    (37.5407, -77.4360), # VA Richmond
    (47.0379, -122.9007),# WA Olympia
    (38.3362, -81.6123), # WV Charleston
    (43.0747, -89.3842), # WI Madison
    (41.1400, -104.8202),# WY Cheyenne
]
BIG_METROS = [
    (40.7128, -74.0060),   # NYC
    (34.0522, -118.2437),  # Los Angeles
    (41.8781, -87.6298),   # Chicago
    (47.6062, -122.3321),  # Seattle
    (37.7749, -122.4194),  # San Francisco
    (32.7157, -117.1611),  # San Diego
    (25.7617, -80.1918),   # Miami
    (29.7604, -95.3698),   # Houston
    (33.4484, -112.0740),  # Phoenix
    (39.9526, -75.1652),   # Philadelphia
    (38.9072, -77.0369),   # DC
]

EXCLUDE_STATES = {"AK", "HI", "PR"}  # if you want to drop these

session = requests.Session()
session.headers.update(HEADERS)

def fetch_near(lat, lon, limit=50):
    params = {
        "lat": f"{lat:.6f}",
        "lng": f"{lon:.6f}",
        "place": "United States",
        "limit": str(limit),
    }
    r = session.get(BASE_URL, params=params, timeout=20)
    if r.status_code != 200:
        raise requests.HTTPError(f"{r.status_code} {r.text[:200]}")
    return r.json()

def extract_rows(batch):
    out = []
    for item in batch or []:
        s = (item or {}).get("store") or {}
        addr = s.get("address") or {}
        tz = s.get("timeZone") or {}
        coords = s.get("coordinates") or {}
        state = addr.get("countrySubdivisionCode")
        country = addr.get("countryCode")

        # keep only U.S.; optionally drop AK/HI/PR
        if country != "US":
            continue
        if state in EXCLUDE_STATES:
            continue

        out.append({
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
            "country": country,
            "lat": coords.get("latitude"),
            "lon": coords.get("longitude"),
            "timezone": tz.get("timeZoneId"),
            "slug": s.get("slug"),
            "ownershipType": s.get("ownershipTypeCode"),
        })
    return out

# Make a coarse “cell id” so we don't re-query the same area forever.
def cell_key(lat, lon, gran=0.20):
    return (round(lat / gran) * gran, round(lon / gran) * gran)

# Generate a small ring of offsets (~10–25 km) around a point
def ring_offsets_km():
    # ~0.15 degrees ≈ 15–17km depending on latitude
    ds = [0.12, 0.18]  # inner and outer ring
    dirs = [(1,0),(-1,0),(0,1),(0,-1),(1,1),(-1,1),(1,-1),(-1,-1)]
    for d in ds:
        for dx, dy in dirs:
            yield (dx * d, dy * d)

def crawl(rate_sec=0.15):
    rows, seen_ids = [], set()
    visited_cells = set()
    q = deque()

    # enqueue seeds
    for lat, lon in (STATE_CAPS + BIG_METROS):
        q.append((lat, lon))

    def add_batch(batch):
        new = 0
        for r in batch:
            k = str(r.get("id") or r.get("storeNumber"))
            if not k or k in seen_ids:
                continue
            seen_ids.add(k)
            rows.append(r)
            new += 1
        return new

    i = 0
    while q:
        lat, lon = q.popleft()
        ckey = cell_key(lat, lon, gran=0.20)
        if ckey in visited_cells:
            continue
        visited_cells.add(ckey)

        try:
            data = fetch_near(lat, lon, limit=50)
            batch = extract_rows(data)
            added = add_batch(batch)
            i += 1
            print(f"[{i:05d}] @ {lat:6.2f},{lon:7.2f}  +{added:<2}  (total {len(rows)})")

            # expand around any newly discovered stores
            if added:
                for store in batch:
                    slat = store.get("lat")
                    slon = store.get("lon")
                    if slat is None or slon is None:
                        continue
                    for dx, dy in ring_offsets_km():
                        q.append((slat + dy, slon + dx))

        except Exception as e:
            i += 1
            print(f"[{i:05d}] @ {lat:6.2f},{lon:7.2f}  ERROR {e}")

        time.sleep(rate_sec)

    return rows

if __name__ == "__main__":
    rows = crawl(rate_sec=0.15)
    if not rows:
        print("No rows collected.")
        raise SystemExit(1)

    # Save
    out = "starbucks_us_locations_expand.csv"
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print(f"Saved {len(rows)} rows → {out}")
