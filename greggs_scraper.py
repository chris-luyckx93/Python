"""
greggs_scraper.py
Downloads all Greggs store locations (lat/lon + metadata) via their store finder API.
Strategy: Grid search across UK bounding box, deduplicate by shopCode.
"""

import requests
import pandas as pd
import numpy as np
import json
import time
import os
from datetime import datetime

# ── Config ────────────────────────────────────────────────────────────────────
API_URL = "https://production-digital.greggs.co.uk/api/v1.0/shops"
RADIUS_M = 25_000          # 25km radius per query point
LAT_STEP = 0.32            # ~35km between grid rows
LON_STEP = 0.50            # ~35km between grid cols (UK ~55°N avg)
REQUEST_DELAY = 0.3        # seconds between requests (be respectful)
CHECKPOINT_FILE = "greggs_checkpoint.json"
OUTPUT_CSV = "greggs_stores.csv"
MAX_RETRIES = 3

# UK bounding box
LAT_MIN, LAT_MAX = 49.9, 61.0
LON_MIN, LON_MAX = -8.2, 1.9

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
}

# ── Grid generation ────────────────────────────────────────────────────────────
def build_grid() -> list[tuple[float, float]]:
    """Generate lat/lon grid points covering the UK."""
    lats = np.arange(LAT_MIN, LAT_MAX + LAT_STEP, LAT_STEP)
    lons = np.arange(LON_MIN, LON_MAX + LON_STEP, LON_STEP)
    return [(round(lat, 5), round(lon, 5)) for lat in lats for lon in lons]


# ── API fetch ─────────────────────────────────────────────────────────────────
def fetch_stores(lat: float, lon: float) -> list[dict]:
    """Query the Greggs API for stores near a given lat/lon."""
    params = {
        "latitude": lat,
        "longitude": lon,
        "distanceInMeters": RADIUS_M,
    }
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(API_URL, params=params, headers=HEADERS, timeout=15)
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 429:
                wait = 5 * attempt
                print(f"  ⚠️  Rate limited (429). Waiting {wait}s...")
                time.sleep(wait)
            else:
                print(f"  ⚠️  HTTP {resp.status_code} at ({lat}, {lon}). Attempt {attempt}/{MAX_RETRIES}")
                time.sleep(2)
        except requests.RequestException as e:
            print(f"  ❌ Request error at ({lat}, {lon}): {e}. Attempt {attempt}/{MAX_RETRIES}")
            time.sleep(3)
    return []


# ── Flatten store record ──────────────────────────────────────────────────────
def flatten_store(store: dict) -> dict:
    """Extract key fields from a raw store JSON record."""
    addr = store.get("address", {})
    
    # Determine available channels
    channels = [c["channelType"] for c in store.get("channels", []) if c.get("isAvailable")]
    
    return {
        "shopCode":         store.get("shopCode"),
        "shopName":         store.get("shopName"),
        "latitude":         addr.get("latitude"),
        "longitude":        addr.get("longitude"),
        "streetName":       addr.get("streetName"),
        "city":             addr.get("city"),
        "postCode":         addr.get("postCode"),
        "country":          addr.get("country"),
        "phoneNumber":      addr.get("phoneNumber"),
        "isPublished":      store.get("isPublished"),
        "isClosed":         store.get("isClosed"),
        "isFranchise":      store.get("isFranchiseShop"),
        "franchisePartner": store.get("franchisePartner"),
        "driveThrough":     store.get("driveThrough"),
        "seating":          store.get("seating"),
        "parking":          store.get("parking"),
        "kiosk":            store.get("kiosk"),
        "wifi":             store.get("wifi"),
        "digitalOnly":      store.get("digitalOnly"),
        "supplySite":       store.get("supplySite"),
        "openingDate":      store.get("openingDate"),
        "channels":         "|".join(channels),
        "hasJustEat":       "JustEat" in channels,
        "hasUberEats":      "UberEatsDelivery" in channels,
        "hasClickCollect":  "ClickAndCollect" in channels,
    }


# ── Checkpoint helpers ────────────────────────────────────────────────────────
def load_checkpoint() -> tuple[dict, set]:
    """Load previously saved stores and completed grid points."""
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE) as f:
            data = json.load(f)
        stores = {k: v for k, v in data.get("stores", {}).items()}
        completed = set(tuple(p) for p in data.get("completed_points", []))
        print(f"📂 Resumed from checkpoint: {len(stores)} stores, {len(completed)} points done.")
        return stores, completed
    return {}, set()


def save_checkpoint(stores: dict, completed: set):
    """Persist current state to JSON."""
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump({
            "stores": stores,
            "completed_points": [list(p) for p in completed],
            "saved_at": datetime.utcnow().isoformat(),
        }, f)


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    grid = build_grid()
    total_points = len(grid)
    print(f"🗺️  Grid: {total_points} points | Radius: {RADIUS_M/1000:.0f}km | Delay: {REQUEST_DELAY}s")
    print(f"⏱️  Estimated time: ~{total_points * REQUEST_DELAY / 60:.1f} min\n")

    stores, completed = load_checkpoint()

    try:
        for i, (lat, lon) in enumerate(grid, 1):
            if (lat, lon) in completed:
                continue

            results = fetch_stores(lat, lon)
            new_count = 0

            for store in results:
                code = store.get("shopCode")
                if code and code not in stores:
                    stores[code] = flatten_store(store)
                    new_count += 1

            completed.add((lat, lon))

            # Progress log
            print(f"[{i:>4}/{total_points}] ({lat:>8.4f}, {lon:>7.4f}) "
                  f"→ {len(results):>3} returned, {new_count:>3} new | "
                  f"Total unique: {len(stores):>4}")

            # Checkpoint every 50 calls
            if i % 50 == 0:
                save_checkpoint(stores, completed)
                print(f"  💾 Checkpoint saved ({len(stores)} stores so far)\n")

            time.sleep(REQUEST_DELAY)

    except KeyboardInterrupt:
        print("\n⚠️  Interrupted! Saving checkpoint...")
        save_checkpoint(stores, completed)

    # ── Save final CSV ────────────────────────────────────────────────────────
    df = pd.DataFrame(list(stores.values()))
    df = df.sort_values("shopCode").reset_index(drop=True)
    
    # Filter out any records missing coordinates (shouldn't happen, but safety net)
    df = df.dropna(subset=["latitude", "longitude"])
    
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"\n✅ Done! {len(df)} unique stores saved to '{OUTPUT_CSV}'")
    print(f"   Columns: {list(df.columns)}")

    # Clean up checkpoint on success
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)
        print("   Checkpoint file removed.")

    return df


if __name__ == "__main__":
    df = main()
    print(df.head())
