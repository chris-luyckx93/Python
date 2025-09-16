# scrape_raising_canes.py
import os, time, csv, json, requests

# Base Yext endpoint (from your DevTools URL)
BASE_URL = os.environ.get("RC_BASE_URL", "https://prod-cdn.us.yextapis.com/v2/accounts/me/search/vertical/query")

# Required params
DEFAULT = {
    "api_key":        os.environ["RC_API_KEY"],              # <— must be set
    "experienceKey":  os.environ.get("RC_EXPERIENCE","locator"),
    "verticalKey":    os.environ.get("RC_VERTICAL","locations"),
    "v":              os.environ.get("RC_VERSION_DATE","20220511"),
    "version":        os.environ.get("RC_ENV_VERSION","PRODUCTION"),
    "locale":         os.environ.get("RC_LOCALE","en"),
    "source":         "STANDARD",
    "sessionTrackingEnabled": "true",
    "skipSpellCheck": "true",
    "retrieveFacets": "false",
    "limit":          "50",
    "sortBys":        "[]",
    "input":          "",   # we use 'filters' for geo search
}

HEADERS = {"accept":"application/json","user-agent":"Mozilla/5.0"}

def query_near(lat, lng, radius_m=400_000, limit=50):
    params = DEFAULT.copy()
    params["limit"] = str(limit)
    # Yext expects a JSON-encoded 'filters' param for a geo "near" search:
    near = {"builtin.location":{"$near":{"lat":lat, "lng":lng, "radius": int(radius_m)}}}
    params["filters"] = json.dumps(near, separators=(",",":"))
    r = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=20)
    r.raise_for_status()
    return r.json()

def us_grid(step_deg=2.5):
    # simple grid over the contiguous US
    min_lat, max_lat = 24.5, 49.5
    min_lng, max_lng = -124.8, -66.9
    lat = min_lat
    while lat <= max_lat:
        lng = min_lng
        while lng <= max_lng:
            yield round(lat,3), round(lng,3)
            lng += step_deg
        lat += step_deg

def extract_rows(payload: dict):
    resp = payload.get("response") or {}
    results = resp.get("results") or []
    rows = []
    for r in results:
        d = r.get("data",{}) or {}
        addr = d.get("address",{}) or {}
        coord = d.get("yextDisplayCoordinate") or d.get("displayCoordinate") or {}
        loc_id = d.get("id") or d.get("uid") or f"{d.get('name','')}|{addr.get('line1','')}|{addr.get('postalCode','')}"
        rows.append({
            "brand": "Raising Cane's",
            "location_id": loc_id,
            "name": d.get("name"),
            "line1": addr.get("line1"),
            "city": addr.get("city"),
            "region": addr.get("region") or addr.get("state"),
            "postalCode": (addr.get("postalCode") or "")[:5],
            "countryCode": addr.get("countryCode"),
            "lat": coord.get("latitude"),
            "lng": coord.get("longitude"),
            "phone": d.get("mainPhone"),
            "website": d.get("websiteUrl"),
        })
    return rows

def crawl_all(rate_sec=0.6, step_deg=2.5, radius_m=350_000):
    seen, all_rows = set(), []
    for i,(lat,lng) in enumerate(us_grid(step_deg)):
        try:
            data = query_near(lat,lng,radius_m=radius_m,limit=int(DEFAULT["limit"]))
            new = 0
            for row in extract_rows(data):
                if row["location_id"] in seen: 
                    continue
                seen.add(row["location_id"]); all_rows.append(row); new += 1
            print(f"{i:03d} @ {lat},{lng} → +{new} (total {len(all_rows)})")
        except Exception as e:
            print(f"Error @ {lat},{lng}: {e}")
        time.sleep(rate_sec)   # be polite
    return all_rows

if __name__ == "__main__":
    rows = crawl_all()
    if rows:
        out = "raising_canes_locations.csv"
        with open(out,"w",newline="",encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader(); w.writerows(rows)
        print(f"Saved {len(rows)} locations → {out}")
    else:
        print("No locations found — recheck env vars (api_key, v, version, experienceKey, verticalKey).")
