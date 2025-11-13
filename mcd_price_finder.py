from playwright.sync_api import sync_playwright
import json, csv, re
from pathlib import Path
from datetime import datetime

STORE_ID = "837625"
STORE_URL = f"https://mcdonalds.order.online/store/{STORE_ID}?delivery=true&hideModal=true"
OUT_JSON = Path("storepageFeed.json")
OUT_CSV  = Path("mcdonalds_menu_prices.csv")

MONEY_RE = re.compile(r"[-+]?\$?\s*([0-9]+(?:\.[0-9]{1,2})?)")

def to_float(s):
    if not s:
        return None
    m = MONEY_RE.search(s)
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
    if not isinstance(section, dict):
        return

    category_name = section.get("name") or category_hint

    if "items" in section and isinstance(section["items"], list):
        for it in section["items"]:
            if not isinstance(it, dict):
                continue
            name   = it.get("name")
            price  = it.get("displayPrice") or it.get("priceDisplay") or it.get("price")
            strike = it.get("displayStrikethroughPrice")
            rating = it.get("ratingDisplayString")
            item_id= it.get("id")

            if name and price:
                add_row(rows, category_name, name, "", str(price), strike, rating, item_id)

            # capture variant-like options if present
            for og_key in ("optionGroups", "option_groups", "groups"):
                if isinstance(it.get(og_key), list):
                    for group in it[og_key]:
                        opts = group.get("options") if isinstance(group, dict) else None
                        if isinstance(opts, list):
                            for opt in opts:
                                vname  = opt.get("name")
                                vprice = opt.get("displayPrice") or opt.get("priceDisplay") or opt.get("price")
                                if vname and vprice:
                                    add_row(rows, category_name, name, vname, str(vprice), None, rating, item_id)

    for child_key in ("children", "sections", "cards"):
        if isinstance(section.get(child_key), list):
            for child in section[child_key]:
                walk_items_from_section(child, rows, category_name)

def extract_all_items(feed_json):
    rows = []
    data = feed_json.get("data", {})
    spp  = data.get("storepageFeed") or data.get("storePageFeed") or {}

    for root_key in ("storeSections", "tiles", "feed", "sections", "cards"):
        root = spp.get(root_key)
        if isinstance(root, list):
            for section in root:
                walk_items_from_section(section, rows)
        elif isinstance(root, dict):
            walk_items_from_section(root, rows)

    if not rows:
        for v in spp.values():
            if isinstance(v, (list, dict)):
                walk_items_from_section(v, rows)
    return rows

def main():
    captured = {}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, args=["--disable-blink-features=AutomationControlled"])
        context = p.chromium.launch_persistent_context(
            user_data_dir=".playwright_profile",  # persist cookies/tokens between runs
            headless=False,
            viewport={"width": 1280, "height": 900},
            locale="en-US",
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"),
        )
        page = context.pages[0] if context.pages else context.new_page()

        def on_response(resp):
            url = resp.url
            if "graphql/storepageFeed" in url and resp.request.method == "POST":
                try:
                    captured["json"] = resp.json()
                except Exception:
                    pass

        page.on("response", on_response)

        page.goto(STORE_URL, wait_until="domcontentloaded")
        # Give time for any bot challenge + app bootstrapping to fire the GraphQL request
        page.wait_for_timeout(8000)

        # If not captured yet, try a reload which often refires the feed request
        if "json" not in captured:
            page.reload(wait_until="networkidle")
            page.wait_for_timeout(4000)

        # Extra nudge: scroll to bottom to trigger lazy loads
        if "json" not in captured:
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(3000)

        if "json" not in captured:
            raise RuntimeError("Did not capture storepageFeed response. The page may have changed its load flow.")

        feed = captured["json"]

        # Save raw JSON for inspection
        OUT_JSON.write_text(json.dumps(feed, indent=2), encoding="utf-8")

        # Parse items -> CSV
        rows = extract_all_items(feed)

        # De-dup and sort
        seen, deduped = set(), []
        for r in rows:
            key = (r["category"], r["item_name"], r["variant"])
            if key not in seen:
                seen.add(key)
                deduped.append(r)

        deduped.sort(key=lambda r: (r["category"].lower(), r["item_name"].lower(), r["variant"].lower()))

        with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(deduped[0].keys()) if deduped else
                                    ["category","item_id","item_name","variant","display_price","price_value","display_strike_price","rating"])
            writer.writeheader()
            writer.writerows(deduped)

        print(f"Captured {len(deduped)} rows -> {OUT_CSV.resolve()}")
        print(f"Also saved raw JSON -> {OUT_JSON.resolve()}")

        # keep the context around so future runs reuse cookies/tokens
        # (Close pages only)
        for pg in context.pages:
            try: pg.close()
            except: pass
        browser.close()

if __name__ == "__main__":
    main()
