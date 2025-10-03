# scrape_7brew_us.py
import csv, json, time, re
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup

BASE = "https://7brew.com"
LIST_URL = "https://7brew.com/find-a-7-brew/"

HEADERS = {
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

session = requests.Session()
session.headers.update(HEADERS)

def get_soup(url: str) -> BeautifulSoup:
    r = session.get(url, timeout=30)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")

def pick_jsonld(soup: BeautifulSoup):
    """Return the first JSON-LD blob that looks like a place/business."""
    for tag in soup.find_all("script", attrs={"type": "application/ld+json"}):
        try:
            data = json.loads(tag.string or tag.text or "")
        except Exception:
            continue
        # Some pages wrap JSON-LD in a list
        objs = data if isinstance(data, list) else [data]
        for obj in objs:
            t = obj.get("@type", "")
            # Accept common types that contain address/geo
            if isinstance(t, list):
                types = [x.lower() for x in t]
            else:
                types = [str(t).lower()]
            if any(x in ("place", "localbusiness", "organization", "restaurant") for x in types):
                return obj
    return None

def text_or(d, *keys, default=""):
    v = d
    for k in keys:
        if not isinstance(v, dict):
            return default
        v = v.get(k)
    return v if v is not None else default

def parse_detail(url: str) -> dict:
    soup = get_soup(url)
    data = pick_jsonld(soup) or {}

    # Prefer JSON-LD fields; fall back to scraping visible text if needed
    addr = data.get("address", {}) if isinstance(data.get("address"), dict) else {}
    geo = data.get("geo", {}) if isinstance(data.get("geo"), dict) else {}

    # Try to pick a readable name and slug
    name = data.get("name") or soup.find("h1").get_text(strip=True) if soup.find("h1") else ""
    slug = urlparse(url).path.rstrip("/").split("/")[-1]

    row = {
        "brand": "7 Brew",
        "name": name,
        "slug": slug,
        "url": url,
        "line1": text_or(addr, "streetAddress", default=""),
        "city": text_or(addr, "addressLocality", default=""),
        "region": text_or(addr, "addressRegion", default=""),   # state (e.g., TX)
        "postalCode": text_or(addr, "postalCode", default=""),
        "country": text_or(addr, "addressCountry", default=""),
        "lat": text_or(geo, "latitude", default=""),
        "lon": text_or(geo, "longitude", default=""),
    }

    # If country missing, try to infer from visible address block (optional)
    if not row["country"]:
        addr_block = soup.find(text=re.compile(r"\bUSA\b")) or soup.find(text=re.compile(r"\bUnited States\b"))
        if addr_block:
            row["country"] = "US"

    return row

def scrape_listing() -> list[dict]:
    soup = get_soup(LIST_URL)

    # Grab every “View” button/link; dedupe by href
    links = []
    seen = set()
    for a in soup.find_all("a"):
        label = (a.get_text() or "").strip().lower()
        if label == "view":
            href = a.get("href") or ""
            if not href:
                continue
            url = urljoin(BASE, href)
            if url not in seen:
                seen.add(url)
                links.append(url)

    print(f"Found {len(links)} store detail links")

    rows = []
    for i, url in enumerate(links, 1):
        try:
            row = parse_detail(url)
            # keep only US locations
            if (row.get("country") or "").upper() == "US":
                rows.append(row)
                print(f"{i:04d}/{len(links)}  {row.get('city','')}, {row.get('region','')}  -> ok")
            else:
                print(f"{i:04d}/{len(links)}  (non-US, skipped)  {url}")
        except Exception as e:
            print(f"{i:04d}/{len(links)}  ERROR  {e}  {url}")
        time.sleep(0.2)  # be polite

    return rows

if __name__ == "__main__":
    rows = scrape_listing()
    if not rows:
        print("No rows scraped. The page layout might have changed.")
    else:
        out = "7brew_us_locations.csv"
        with open(out, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(
                f,
                fieldnames=[
                    "brand","name","slug","url",
                    "line1","city","region","postalCode","country",
                    "lat","lon"
                ],
            )
            w.writeheader()
            w.writerows(rows)
        print(f"Saved {len(rows)} rows → {out}")
