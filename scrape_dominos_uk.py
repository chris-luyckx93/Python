import json
import time
import csv
import requests

BASE_URL = "https://www.dominos.co.uk/api/stores/v1/stores"

# Headers that mimic a normal browser request.
# These are similar to what worked for you already.
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/129.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-GB,en;q=0.9",
    "Origin": "https://www.dominos.co.uk",
    "Referer": "https://www.dominos.co.uk/",
    "X-Requested-With": "XMLHttpRequest",
}


def make_location_token(postcode, lat, lon):
    """
    Build the locationToken value used by the Domino's API:
    'UK-PC:' + JSON-encoded {postCode, latitude, longitude}
    """
    payload = {
        "postCode": postcode,
        "latitude": str(lat),
        "longitude": str(lon),
    }
    return "UK-PC:" + json.dumps(payload, separators=(",", ":"))


def fetch_stores_for_location(postcode, lat, lon, radius=30, limit=200):
    """
    Call the Domino's API for a single location and return a list of raw store dicts.
    """
    token = make_location_token(postcode, lat, lon)
    params = {
        "locationToken": token,
        "radius": radius,
        "limit": limit,
    }

    resp = requests.get(BASE_URL, params=params, headers=HEADERS)
    resp.raise_for_status()

    data = resp.json().get("data", {}) or {}

    stores = []

    # 1) Local (primary) store
    local_store = data.get("localStore")
    if isinstance(local_store, dict):
        stores.append(local_store)

    # 2) Any list of stores in the data (covers nearbyStores, storesInRadius, etc.)
    for key, value in data.items():
        if key == "localStore":
            continue
        if isinstance(value, list):
            stores.extend(value)

    print(
        f"  API keys for this location: {list(data.keys())}, "
        f"fetched {len(stores)} raw stores"
    )

    return stores


def normalize_store(store):
    """
    Flatten the nested store JSON into a simple dict with the fields we care about.
    """
    loc = store.get("location", {}) or {}
    coords = loc.get("coordinates", {}) or {}
    addr = loc.get("address", {}) or {}
    contacts = store.get("contacts", {}) or {}
    region = store.get("region", {}) or {}
    delivery_charge = (store.get("deliveryCharge") or {}).get("charge", {}) or {}

    return {
        "id": store.get("id"),
        "name": store.get("name"),
        "phone": contacts.get("phone"),
        "customerConcernEmail": contacts.get("customerConcernEmail"),
        "latitude": coords.get("latitude"),
        "longitude": coords.get("longitude"),
        "postcode": addr.get("postcode"),
        "town": addr.get("town"),
        "address_line1": addr.get("line1"),
        "address_line2": addr.get("line2"),
        "country": addr.get("country"),
        "status": store.get("status"),
        "isOpen": store.get("isOpen"),
        "region_id": region.get("id"),
        "region_name": region.get("name"),
        "delivery_charge": delivery_charge.get("amount"),
    }


def frange(start, stop, step):
    """
    Float range helper: yields start, start+step, ..., <= stop (within a tiny epsilon).
    """
    x = start
    # use a small epsilon to avoid floating-point issues
    while x <= stop + 1e-9:
        yield round(x, 6)
        x += step


def generate_uk_grid(step_deg=0.4):
    """
    Generate a coarse lat/lon grid over the UK.
    Each grid point is a 'seed' location for the API.

    step_deg = 0.4 gives ~25 x ~25 = ~625 seed points.
    """
    seeds = []
    # Very rough UK bounding box
    min_lat, max_lat = 49.9, 60.0
    min_lon, max_lon = -8.0, 2.0

    for lat in frange(min_lat, max_lat, step_deg):
        for lon in frange(min_lon, max_lon, step_deg):
            seeds.append(
                {
                    # Any valid UK postcode; lat/lon does the real work.
                    "postcode": "SW5 0PA",
                    "lat": lat,
                    "lon": lon,
                }
            )
    return seeds


def collect_all_stores(
    seed_locations,
    radius=30,
    limit=200,
    sleep_secs=1.0,
    max_no_new=200,
):
    """
    Call the API for multiple seed locations and dedupe stores by ID.

    max_no_new: stop early after this many consecutive seeds that add no new stores.
    """
    all_stores = {}  # key = store id, value = normalized store
    no_new_counter = 0

    total_seeds = len(seed_locations)

    for idx, loc in enumerate(seed_locations, start=1):
        postcode = loc["postcode"]
        lat = loc["lat"]
        lon = loc["lon"]
        print(f"[{idx}/{total_seeds}] Fetching for {postcode} ({lat}, {lon})...")

        try:
            stores_raw = fetch_stores_for_location(
                postcode, lat, lon, radius=radius, limit=limit
            )
        except requests.HTTPError as e:
            print(f"  Error fetching {postcode} / {lat},{lon}: {e}")
            continue
        except requests.RequestException as e:
            print(f"  Request error for {postcode} / {lat},{lon}: {e}")
            continue

        before = len(all_stores)

        for s in stores_raw:
            sid = s.get("id")
            if not sid:
                continue
            all_stores[sid] = normalize_store(s)

        after = len(all_stores)
        newly_added = after - before
        print(f"  -> {newly_added} new, {after} total unique stores")

        if newly_added == 0:
            no_new_counter += 1
        else:
            no_new_counter = 0

        if max_no_new is not None and no_new_counter >= max_no_new:
            print("No new stores for a while – stopping early.")
            break

        # Be gentle with their API
        time.sleep(sleep_secs)

    return list(all_stores.values())


def save_stores_to_csv(stores, filename="dominos_uk_stores.csv"):
    if not stores:
        print("No stores to save.")
        return

    fieldnames = list(stores[0].keys())
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for s in stores:
            writer.writerow(s)

    print(f"Saved {len(stores)} stores to {filename}")


if __name__ == "__main__":
    # 1) Build a grid of seed locations across the UK
    seed_locations = generate_uk_grid(step_deg=0.4)
    print(f"Using {len(seed_locations)} seed locations")

    # 2) Collect stores
    stores = collect_all_stores(
        seed_locations,
        radius=30,       # search radius in km
        limit=200,       # max stores per call (API may cap it lower)
        sleep_secs=1.0,  # pause between requests
        max_no_new=200,  # stop after 200 consecutive seeds with no new stores
    )

    print(f"\nCollected {len(stores)} unique stores in total.\n")

    # 3) Save to CSV
    save_stores_to_csv(stores, filename="dominos_uk_stores.csv")
