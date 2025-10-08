#!/usr/bin/env python3
"""
Scrape 2024 U.S. sales & units for restaurant chains from Restaurant Business Online
and match them to chain names in an Excel file.

Designed for GitHub Codespaces (or any local Python 3.10+ env).

Inputs
------
- Excel: "Top 500 chains 2024 - Technomic.xlsx"
  - Sheet: "Technomic Top 500 Chains"
  - Chain names in Column A (with header on row 1)

Outputs
-------
- A new Excel file alongside the input named:
  "Top 500 chains 2024 - Technomic+RBO.xlsx"
  with added columns: [RBO 2024 Sales ($), RBO 2024 Units, RBO Slug, RBO URL, RBO Status]

Usage
-----
pip install pandas openpyxl requests beautifulsoup4 lxml tenacity

python rbo_scraper_matcher.py \
  --excel "Top 500 chains 2024 - Technomic.xlsx" \
  --sheet "Technomic Top 500 Chains" \
  --name-column "A" \
  --sleep 1.5
"""
from __future__ import annotations

import argparse
import os
import re
import time
import unicodedata
from dataclasses import dataclass
from typing import Optional, Tuple, Dict, List

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

BASE = "https://www.restaurantbusinessonline.com/top-500-chains-2025"

# Add any non-obvious slug overrides here: {canonical_chain_name_lower: "custom-slug"}
SLUG_OVERRIDES: Dict[str, str] = {
    "mcdonald's": "mcdonalds",
    "wendy's": "wendys",
    "steak 'n shake": "steak-n-shake",
    "steak n shake": "steak-n-shake",
}

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

@dataclass
class ScrapeResult:
    name: str
    slug: str
    url: str
    sales_2024: Optional[int]
    units_2024: Optional[int]
    status: str


# -------------------------- Helpers --------------------------

def normalize_name(s: str) -> str:
    # lower, strip accents, normalize common punctuations
    s = (s or "").strip()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return s.lower()


def to_slug(name: str) -> str:
    n = normalize_name(name)
    # Special quick fixes
    n = n.replace("&", " and ")
    n = n.replace("+", " and ")
    n = n.replace("@", " at ")
    n = n.replace("/", " ")
    n = n.replace("\\", " ")
    # remove apostrophes entirely (e.g., McDonald's -> mcdonalds)
    n = n.replace("'", "")
    n = re.sub(r"[^a-z0-9]+", "-", n)
    n = re.sub(r"-+", "-", n).strip("-")
    return n


def parse_money_to_int(raw: str) -> Optional[int]:
    if not raw:
        return None
    s = raw.replace(",", "").replace(" ", "")
    m = re.search(r"\$?([0-9]*\.?[0-9]+)\s*([kKmMbB])?", s)
    if not m:
        # try plain integer like 123456789
        m2 = re.search(r"([0-9]{3,})", s)
        return int(m2.group(1)) if m2 else None
    val = float(m.group(1))
    suf = m.group(2)
    if suf:
        if suf.lower() == "k":
            val *= 1_000
        elif suf.lower() == "m":
            val *= 1_000_000
        elif suf.lower() == "b":
            val *= 1_000_000_000
    return int(round(val))


def parse_int(raw: str) -> Optional[int]:
    if not raw:
        return None
    m = re.search(r"([0-9][0-9,]*)", raw)
    return int(m.group(1).replace(",", "")) if m else None


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=6),
       retry=retry_if_exception_type((requests.RequestException,)))
def fetch_html(url: str) -> str:
    # Use a session and browser-like headers to reduce 4xx/5xx
    s = requests.Session()
    headers = {
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Upgrade-Insecure-Requests": "1",
    }
    resp = s.get(url, headers=headers, timeout=20)
    if resp.status_code >= 400:
        raise requests.RequestException(f"HTTP {resp.status_code} for {url}")
    return resp.text


def extract_metrics_from_html(html: str) -> Tuple[Optional[int], Optional[int]]:
    """Locate 2024 sales & units. Handles RBO label '2024 U.S. Sales ($000,000)'."""
    soup = BeautifulSoup(html, "lxml")

    # Consolidate page text into a single blob
    blob = soup.get_text("\n", strip=True)

    sales: Optional[int] = None
    units: Optional[int] = None

    # 1) Exact label with ($000,000) -> value is in millions, convert to dollars
    m = re.search(
        r"2024\s*(?:u\.?s\.?\s*)?(?:systemwide\s*)?sales\s*\(\$000,000\)\s*\$?([0-9,\.]+)",
        blob, flags=re.IGNORECASE
    )
    if m:
        val_millions = parse_int(m.group(1))
        if val_millions is not None:
            sales = val_millions * 1_000_000

    # 2) Units exact label
    m = re.search(r"2024\s*(?:u\.?s\.?\s*)?units\s*([0-9][0-9,]*)", blob, flags=re.IGNORECASE)
    if m:
        units = parse_int(m.group(1))

    # 3) Fallback generic patterns (for pages without the exact label)
    if sales is None:
        m = re.search(
            r"2024[^\n]{0,80}(systemwide\s+)?sales[^\n$]{0,80}([$0-9,\.,\s]*[kKmMbB]?)",
            blob, flags=re.IGNORECASE
        )
        if m:
            sales = parse_money_to_int(m.group(2))

    if units is None:
        m = re.search(r"2024[^\n]{0,80}units[^\n]{0,80}([0-9][0-9,]*)", blob, flags=re.IGNORECASE)
        if m:
            units = parse_int(m.group(1))

    # Guard against accidental zeros
    if sales == 0:
        sales = None
    return sales, units


def try_scrape_chain(name: str, sleep_seconds: float = 1.5) -> ScrapeResult:
    canon = normalize_name(name)
    slug = SLUG_OVERRIDES.get(canon) or to_slug(name)
    url = f"{BASE}/{slug}"
    try:
        html = fetch_html(url)
        sales, units = extract_metrics_from_html(html)
        status = "ok" if (sales is not None or units is not None) else "parsed-none"
    except Exception as e:
        sales, units = None, None
        status = f"error: {type(e).__name__}: {e}"
    time.sleep(sleep_seconds)
    return ScrapeResult(name=name, slug=slug, url=url, sales_2024=sales, units_2024=units, status=status)


# -------------------------- Main --------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--excel", default="Top 500 chains 2024 - Technomic.xlsx",
                    help="Path to the Excel workbook")
    ap.add_argument("--sheet", default="Technomic Top 500 Chains",
                    help="Worksheet name with chain names in column A")
    ap.add_argument("--name-column", default="A",
                    help="Excel column letter containing the chain names (default A)")
    ap.add_argument("--sleep", type=float, default=1.5,
                    help="Polite delay between requests in seconds (default 1.5)")
    ap.add_argument("--limit", type=int, default=0,
                    help="If > 0, only scrape the first N names for testing")

    args = ap.parse_args()

    # Load names
    df = pd.read_excel(args.excel, sheet_name=args.sheet, engine="openpyxl")

    # Resolve column index from letter or name
    name_col = args.name_column
    if len(name_col) == 1 and name_col.isalpha():
        idx = ord(name_col.upper()) - ord('A')
        name_series = df.iloc[:, idx]
    else:
        # assume it's a header name
        name_series = df[name_col]

    names = [str(x).strip() for x in name_series.dropna().tolist()]
    if args.limit and args.limit > 0:
        names = names[:args.limit]

    results: List[ScrapeResult] = []
    for i, name in enumerate(names, 1):
        print(f"[{i}/{len(names)}] {name} ...", flush=True)
        res = try_scrape_chain(name, sleep_seconds=args.sleep)
        print(f"    -> {res.status} | sales={res.sales_2024} units={res.units_2024}")
        results.append(res)

    # Append columns to dataframe aligned by original name column
    # We'll create a mapping by original values (string match)
    res_map: Dict[str, ScrapeResult] = {r.name: r for r in results}

    def get_res(name):
        return res_map.get(str(name).strip())

    df["RBO 2024 Sales ($)"] = name_series.apply(lambda n: (get_res(n).sales_2024 if get_res(n) else None))
    df["RBO 2024 Units"] = name_series.apply(lambda n: (get_res(n).units_2024 if get_res(n) else None))
    df["RBO Slug"] = name_series.apply(lambda n: (get_res(n).slug if get_res(n) else None))
    df["RBO URL"] = name_series.apply(lambda n: (get_res(n).url if get_res(n) else None))
    df["RBO Status"] = name_series.apply(lambda n: (get_res(n).status if get_res(n) else None))

    # Save output
    base, ext = os.path.splitext(args.excel)
    out_path = f"{base}+RBO{ext or '.xlsx'}"
    df.to_excel(out_path, index=False)

    print(f"\nDone. Wrote: {out_path}")


if __name__ == "__main__":
    main()
