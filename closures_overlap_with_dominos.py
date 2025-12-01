import math
import time
import re
import pandas as pd
import requests

# -------------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------------

INPUT_FILE = "UK Pizza stores (Big 3).xlsx"
OUTPUT_FILE = "UK Pizza stores (Big 3) - closures_overlap.xlsx"

CLOSURES_SHEET = "Pizza Hut closures"
DOMINOS_SHEET = "Dominos"

CLOSURES_POSTCODE_COL_INDEX = 5  # column F (0-based index)

THRESHOLD_MILES = 2.0

API_BASE = "https://api.postcodes.io/postcodes"
SLEEP_SECS = 0.1

# -------------------------------------------------------------------
# HELPERS
# -------------------------------------------------------------------

POSTCODE_RE = re.compile(r"([A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2})", re.IGNORECASE)


def clean_postcode(pc: str) -> str:
    if pc is None:
        return ""
    text = str(pc).strip()
    m = POSTCODE_RE.search(text)
    return m.group(1).upper().replace("  ", " ") if m else ""


def lookup_postcode(postcode: str):
    pc = clean_postcode(postcode)
    if not pc:
        return None, None
    try:
        resp = requests.get(f"{API_BASE}/{pc}", timeout=5)
        resp.raise_for_status()
    except requests.RequestException:
        return None, None

    data = resp.json()
    if data.get("status") != 200:
        return None, None

    r = data["result"]
    return r.get("latitude"), r.get("longitude")


def haversine_miles(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(float, [lat1, lon1, lat2, lon2])
    phi1, phi2 = map(math.radians, [lat1, lat2])
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    return 3958.8 * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def detect_header_row(df, required_cols):
    """
    Scan first 10 rows looking for Latitude & Longitude.
    Returns header row index or raises error.
    """
    for i in range(min(10, len(df))):
        row = df.iloc[i].astype(str).str.lower().tolist()
        if any(req.lower() in row for req in required_cols):
            return i
    raise ValueError(
        f"Could not find header row containing columns: {required_cols}. "
        f"First few rows looked like: {df.head(5)}"
    )


# -------------------------------------------------------------------
# MAIN
# -------------------------------------------------------------------

def main():
    print(f"Reading workbook: {INPUT_FILE}")
    raw = pd.read_excel(INPUT_FILE, sheet_name=None, header=None)

    # --- Fix Dominos sheet header ---
    if DOMINOS_SHEET not in raw:
        raise ValueError(f"Sheet '{DOMINOS_SHEET}' not found.")

    dom_raw = raw[DOMINOS_SHEET]

    required_cols = ["latitude", "longitude"]
    header_row_index = detect_header_row(dom_raw, required_cols)

    print(f"Dominos header row detected at index {header_row_index}")

    df_dom = pd.read_excel(
        INPUT_FILE,
        sheet_name=DOMINOS_SHEET,
        header=header_row_index
    )

    if "Latitude" not in df_dom.columns or "Longitude" not in df_dom.columns:
        raise ValueError(
            f"Dominos sheet still missing Latitude/Longitude. Found: {list(df_dom.columns)}"
        )

    dom_lats = df_dom["Latitude"].astype(float).values
    dom_lons = df_dom["Longitude"].astype(float).values

    print(f"Loaded {len(dom_lats)} Domino's stores.")

    # --- Load closures sheet ---
    df_closures = pd.read_excel(INPUT_FILE, sheet_name=CLOSURES_SHEET)

    postcode_col = df_closures.columns[CLOSURES_POSTCODE_COL_INDEX]

    # Add lat/lon columns
    df_closures["Latitude"] = None
    df_closures["Longitude"] = None

    # Geocode closures
    cache = {}
    for idx, row in df_closures.iterrows():
        pc = clean_postcode(row[postcode_col])
        if not pc:
            continue
        print(f"Geocoding closure postcode: {pc}")

        if pc in cache:
            lat, lon = cache[pc]
        else:
            lat, lon = lookup_postcode(pc)
            cache[pc] = (lat, lon)
            time.sleep(SLEEP_SECS)

        df_closures.at[idx, "Latitude"] = lat
        df_closures.at[idx, "Longitude"] = lon

    # --- Compute distances ---
    distances = []
    within = []

    for idx, row in df_closures.iterrows():
        lat = row["Latitude"]
        lon = row["Longitude"]

        if pd.isna(lat) or pd.isna(lon):
            distances.append(None)
            within.append(False)
            continue

        min_dist = min(
            haversine_miles(lat, lon, dlat, dlon)
            for dlat, dlon in zip(dom_lats, dom_lons)
        )

        distances.append(min_dist)
        within.append(min_dist <= THRESHOLD_MILES)

    df_closures["Nearest_Dominos_Distance_miles"] = distances
    df_closures["Within_2_miles"] = within

    pct = (sum(within) / len(df_closures)) * 100
    print("\n--- Results ---")
    print(f"Total closures: {len(df_closures)}")
    print(f"Within 2 miles of Domino's: {sum(within)}")
    print(f"Percentage: {pct:.1f}%")

    # --- Write updated workbook ---
    raw[CLOSURES_SHEET] = df_closures

    print(f"\nWriting output to {OUTPUT_FILE}")
    with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
        for name, df_sheet in raw.items():
            df_sheet.to_excel(writer, sheet_name=name, index=False)

    print("Done.")


if __name__ == "__main__":
    main()
