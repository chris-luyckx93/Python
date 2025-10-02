# scrape_starbucks_us.py
import os, csv, time, requests
from typing import Dict, Any, List, Set, Tuple

COOKIE_FILE = "cookie.txt"
BASE_URL = "https://www.starbucks.com/apiproxy/v1/locations"

# ---------- load cookie ----------
if not os.path.exists(COOKIE_FILE):
    raise RuntimeError("cookie.txt not found. Paste the full `cookie:` header value into cookie.txt (single line).")

with open(COOKIE_FILE, "r", encoding="utf-8") as f:
    SB_COOKIE = f.read().strip()

if not SB_COOKIE:
    raise RuntimeError("cookie.txt is empty. Paste the full cookie header value into it (single line).")

# ---------- headers (very close to browser) ----------
COMMON_HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "en-US,en;q=0.9",
    "referer": "https://www.starbucks.com/store-locator",
    "origin": "https://www.starbucks.com",
    "user-agent": "Mozilla/5.0",
    "x-requested-with": "XMLHttpRequest",
    "sec-fetch-site": "same-origin",
    "sec-fetch-mode": "cors",
    "sec-fetch-dest": "empty",
    "cookie": SB_COOKIE,        # <- copied from your browser
}

def us_grid(step_deg: float = 1.5) -> List[Tuple[float, float]]:
    min_lat, max_lat = 24.5, 49.5
    min_lng, max_lng = -124.8, -66.9
    pts = []
    lat = min_lat
    while lat <= max_lat + 1e-9:
        lng = min_lng
        while lng <= max_lng + 1e-9:
            pts.append((round(lat,3), round(lng,3)))
            lng += step_deg
        lat += step_deg
    return pts

def query_near(lat: float, lng: float, session: requests.Session):
    params = {
        "lat": f"{lat:.6f}",
        "lng": f"{lng:.6f}",
        # any string works for 'place'; we'll keep it simple
        "place": "United States",
        # you can experiment with 'limit' or 'radius', but they’re not required
    }
    r = session.get(BASE_URL, params=params, headers=COMMON_HEADERS, timeout=25)
    r.raise_for_status()
    data = r.json()
    return data if isinstance(data, list) else []

def normalize_row(item: Dict[str, Any]) -> Dict[str, Any]:
    s = item.get("store", {}) or {}
    a = s.get("address", {}) or {}
    c = s.get("coordinates", {}) or {}
    return {
        "brand": "Starbucks",
        "store_id": s.get("id") or s.get("storeNumber") or "",
        "store_number": s.get("storeNumber") or "",
        "name": s.get("name") or "",
        "ownership": s.get("ownershipTypeCode") or "",
        "phone": s.get("phoneNumber") or "",
        "line1": a.get("streetAddressLine1") or "",
        "line2": a.get("streetAddressLine2") or "",
        "line3": a.get("streetAddressLine3") or "",
        "city": a.get("city") or "",
        "region": a.get("countrySubdivisionCode") or "",
        "postalCode": (a.get("postalCode") or "")[:10],
        "countryCode": a.get("countryCode") or "",
        "lat": c.get("latitude"),
        "lng": c.get("longitude"),
        "open": s.get("open"),
        "openStatusFormatted": s.get("openStatusFormatted") or "",
        "hoursStatusFormatted": s.get("hoursStatusFormatted") or "",
        "slug": s.get("slug") or "",
    }

def sanity_check() -> bool:
    """Make one known-good call (NYC). If this fails, your cookie/headers are bad."""
    test_lat, test_lng = 40.7127753, -74.0059728
    with requests.Session() as session:
        try:
            data = query_near(test_lat, test_lng, session)
            ok = bool(data)
            print(f"Sanity check at NYC → {'OK' if ok else 'NO DATA'} ({len(data)} items).")
            return ok
        except requests.HTTPError as e:
            txt = ""
            try:
                txt = e.response.text[:200]
            except Exception:
                pass
            print(f"Sanity check failed: HTTP {getattr(e.response, 'status_code', '?')} → {txt}")
            return False
        except Exception as e:
            print(f"Sanity check error: {e}")
            return False

def crawl_all(step_deg: float = 1.5, rate_sec: float = 0.35):
    if not sanity_check():
        print("\nYour cookie/headers are not being accepted. "
              "Refresh the store locator, copy the full `cookie:` header again into cookie.txt, "
              "save, and re-run.")
        return []

    session = requests.Session()
    rows, seen = [], set()
    pts = us_grid(step_deg=step_deg)

    for i,(lat,lng) in enumerate(pts, 1):
        try:
            items = query_near(lat, lng, session)
            added = 0
            for it in items:
                row = normalize_row(it)
                sid = str(row["store_id"])
                if not sid or sid in seen:
                    continue
                seen.add(sid)
                rows.append(row)
                added += 1
            print(f"{i:03d}/{len(pts)}  @ {lat:5.1f},{lng:6.1f}  +{added}  (total {len(rows)})")
        except requests.HTTPError as e:
            msg = ""
            try:
                msg = e.response.text[:160]
            except Exception:
                msg = str(e)
            print(f"HTTP {getattr(e.response, 'status_code','?')} @ {lat},{lng} → {msg}")
        except Exception as e:
            print(f"Error @ {lat},{lng}: {e}")
        time.sleep(rate_sec)
    return rows

def save_csv(rows: List[Dict[str, Any]], out_path: str):
    if not rows:
        print("No rows to save.")
        return
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        cols = list(rows[0].keys())
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)
    print(f"Saved {len(rows)} rows → {out_path}")

if __name__ == "__main__":
    all_rows = crawl_all(step_deg=1.5, rate_sec=0.35)
    if all_rows:
        save_csv(all_rows, "starbucks_us_locations.csv")
