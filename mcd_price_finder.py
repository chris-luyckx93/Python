import csv
import json
import re
from pathlib import Path

import requests

STORE_ID = "837625"   # McDonald's Islandia, NY in your example
OUT_CSV  = Path("mcdonalds_menu_prices.csv")

# --- Helpers ---------------------------------------------------------------

MONEY_RE = re.compile(r"[-+]?\$?\s*([0-9]+(?:\.[0-9]{1,2})?)")

def to_float(maybe_price_str):
    if not maybe_price_str:
        return None
    m = MONEY_RE.search(maybe_price_str)
    return float(m.group(1)) if m else None

def add_row(rows, category, item_name, variant_name, display_price, strike_price=None, rating=None, item_id=None):
    rows.append({
        "category": category or "",
        "item_id": item_id or "",
        "item_name": item_name or "",
        "variant": variant_name or "",
        "display_price": display_price or "",
        "price_value": to_float(display_price),
        "display_strike_price": strike_price or "",
        "rating": rating or ""
    })

def walk_items_from_section(section, rows, category_hint=None):
    """
    Handles a variety of section shapes we've seen on DoorDash-powered menus:
    - item_carousel: { items: [ { name, displayPrice, ... } ] }
    - category: { name, items: [ ... ] }
    - nested sections under "sections" or "children"
    - variant/size options under "optionGroups"/"options" (when present)
    """
    if not isinstance(section, dict):
        return

    sec_type = section.get("type") or section.get("__typename")
    category_name = section.get("name") or category_hint

    # 1) Flat carousels with items
    if "items" in section and isinstance(section["items"], list):
        for it in section["items"]:
            if not isinstance(it, dict):
                continue
            name = it.get("name")
            price = it.get("displayPrice")
            strike = it.get("displayStrikethroughPrice")
            rating = it.get("ratingDisplayString")
            item_id = it.get("id")

            # Base item row
            if name and (price or it.get("price")):
                add_row(rows, category_name, name, "", price or str(it.get("price")), strike, rating, item_id)

            # 2) If there are variants (common on some stores), record each
            # These show up under different keys; try a few common shapes:
            # item -> optionGroups -> options -> { name, displayPrice }
            for og_key in ("optionGroups", "option_groups", "groups"):
                if isinstance(it.get(og_key), list):
                    for group in it[og_key]:
                        opts = group.get("options") if isinstance(group, dict) else None
                        if isinstance(opts, list):
                            for opt in opts:
                                vname = opt.get("name")
                                vprice = opt.get("displayPrice") or opt.get("priceDisplay")
                                if vname and vprice:
                                    add_row(rows, category_name, name, vname, vprice, None, rating, item_id)

    # 3) Some feeds have "children" or "sections" that contain more items
    for child_key in ("children", "sections", "cards"):
        if isinstance(section.get(child_key), list):
            for child in section[child_key]:
                walk_items_from_section(child, rows, category_name)

def extract_all_items(feed_json):
    """
    Looks for the storepageFeed payload and iterates through sections/tiles.
    """
    rows = []
    data = feed_json.get("data", {})
    spp = data.get("storepageFeed") or data.get("storePageFeed") or {}

    # Many pages expose items under "storeSections" or "tiles" or "feed"
    for root_key in ("storeSections", "tiles", "feed", "sections", "cards"):
        root = spp.get(root_key)
        if isinstance(root, list):
            for section in root:
                walk_items_from_section(section, rows)
        elif isinstance(root, dict):
            walk_items_from_section(root, rows)

    # As a safety net, walk any dict that looks like a section
    if not rows:
        for v in spp.values():
            if isinstance(v, (list, dict)):
                walk_items_from_section(v, rows)

    return rows

# --- Networking ------------------------------------------------------------

def fetch_storepage_feed(store_id):
    """
    Replays the GraphQL call your browser makes.
    If the site ever tightens headers, copy the request as cURL in DevTools and
    port over headers (notably user-agent, dd-geo headers, etc).
    """
    url = "https://mcdonalds.order.online/graphql/storepageFeed?operation=storepageFeed"
    headers = {
        "content-type": "application/json",
        "accept": "*/*",
        "origin": "https://mcdonalds.order.online",
        "referer": f"https://mcdonalds.order.online/store/{store_id}?delivery=true&hideModal=true",
        "user-agent": "Mozilla/5.0",
    }

    # The GraphQL payload structure matches what the app sends.
    # The exact query isn't required because DoorDash’s endpoint accepts a persisted query by operation name,
    # as long as variables are shaped correctly. These usually include storeId and a few flags.
    # Below variables are minimal but typically work; if you see errors, copy variables from DevTools.
    payload = {
        "operationName": "storepageFeed",
        "variables": {
            "storeId": str(store_id),
            # These flags are commonly present; they can help tailor the feed:
            "isDeviceMobile": False,
            "isFromDeepLink": False,
            "isStorefront": True,
            "shouldFetchFeedV2": True
        },
        "query": None  # persisted queries don't require inline query text
    }

    r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=30)
    r.raise_for_status()
    return r.json()

def main():
    feed = fetch_storepage_feed(STORE_ID)
    rows = extract_all_items(feed)

    # De-dup by (item_name, variant) to keep CSV tidy
    seen = set()
    deduped = []
    for row in rows:
        key = (row["item_name"], row["variant"], row["category"])
        if key not in seen:
            seen.add(key)
            deduped.append(row)

    # Sort nicely
    deduped.sort(key=lambda r: (r["category"].lower(), r["item_name"].lower(), r["variant"].lower()))

    # Write CSV
    fieldnames = [
        "category",
        "item_id",
        "item_name",
        "variant",
        "display_price",
        "price_value",
        "display_strike_price",
        "rating",
    ]
    with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(deduped)

    print(f"Wrote {len(deduped)} rows to {OUT_CSV.resolve()}")

if __name__ == "__main__":
    main()
