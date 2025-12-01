import time
import requests
import pandas as pd

# --- CONFIG -------------------------------------------------------------------

INPUT_EXCEL = "pizza_stores.xlsx"              # your existing file
OUTPUT_EXCEL = "pizza_stores_geocoded.xlsx"    # new file to create

# Map sheet names to their postcode column name (the *text* in the header row)
SHEETS = {
    "Papa Johns": "Postcode",   # change if your header is different
    "Pizza Hut": "Postcode",    # change if your header is different
}

API_BASE = "https://api.postcodes.io/postcodes"
SLEEP_SECS = 0.1  # small delay between API calls to be polite


# --- GEOCODING LOGIC ----------------------------------------------------------


def clean_postcode(pc: str) -> str:
    """
    Basic postcode cleaning: strip leading/trailing spaces.
    """
    if pc is None:
        return ""
    return str(pc).strip()


def lookup_postcode(postcode: str):
    """
    Query the postcodes.io API for a single postcode.
    Returns (lat, lon) or (None, None) if not found or on error.
    """
    pc = clean_postcode(postcode)
    if not pc:
        return None, None

    url = f"{API_BASE}/{pc}"
    try:
        resp = requests.get(url, timeout=5)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  Error looking up {pc}: {e}")
        return None, None

    data = resp.json()
    if data.get("status") != 200 or not data.get("result"):
        print(f"  Postcode not found or invalid: {pc}")
        return None, None

    result = data["result"]
    return result.get("latitude"), result.get("longitude")


def ensure_header_and_get_df(df: pd.DataFrame, postcode_col: str) -> pd.DataFrame:
    """
    Make sure the DataFrame has the correct header row.

    If postcode_col is not in df.columns, try using the first row as the header.
    """
    original_cols = list(df.columns)
    if postcode_col in df.columns:
        print(f"  Found postcode column '{postcode_col}' in columns: {original_cols}")
        return df

    print(
        f"  Postcode column '{postcode_col}' not found in columns: {original_cols}\n"
        f"  Attempting to use the first row as the header..."
    )

    # Use first row as header
    new_header = df.iloc[0]
    df = df[1:].copy()
    df.columns = new_header

    new_cols = list(df.columns)
    print(f"  New columns after using first row as header: {new_cols}")

    if postcode_col not in df.columns:
        raise ValueError(
            f"Postcode column '{postcode_col}' still not found after header fix.\n"
            f"Current columns: {list(df.columns)}\n"
            f"Make sure the sheet '{postcode_col}' really appears in the first data row."
        )

    return df


def geocode_sheet(df: pd.DataFrame, postcode_col: str, cache: dict) -> pd.DataFrame:
    """
    Given a DataFrame and the name of its postcode column,
    return a new DataFrame with Latitude/Longitude columns added.

    cache: dict to avoid re-looking up the same postcode over and over.
    """
    # Fix header if needed
    df = ensure_header_and_get_df(df, postcode_col)

    # Ensure the output columns exist
    if "Latitude" not in df.columns:
        df["Latitude"] = None
    if "Longitude" not in df.columns:
        df["Longitude"] = None

    for idx, row in df.iterrows():
        pc = clean_postcode(row[postcode_col])

        if not pc:
            continue

        if pd.notna(row.get("Latitude")) and pd.notna(row.get("Longitude")):
            # Already filled; skip
            continue

        if pc in cache:
            lat, lon = cache[pc]
        else:
            print(f"  Looking up postcode: {pc}")
            lat, lon = lookup_postcode(pc)
            cache[pc] = (lat, lon)
            time.sleep(SLEEP_SECS)

        df.at[idx, "Latitude"] = lat
        df.at[idx, "Longitude"] = lon

    return df


# --- MAIN ---------------------------------------------------------------------


def main():
    print(f"Reading {INPUT_EXCEL} ...")
    # Load all sheets, we’ll pick the ones we care about
    xls = pd.read_excel(INPUT_EXCEL, sheet_name=None, header=0)

    # Shared cache across all sheets (Papa Johns + Pizza Hut)
    postcode_cache = {}

    # Process each configured sheet
    for sheet_name, postcode_col in SHEETS.items():
        if sheet_name not in xls:
            print(f"WARNING: Sheet '{sheet_name}' not found in workbook; skipping.")
            continue

        print(f"\nProcessing sheet: {sheet_name}")
        df = xls[sheet_name]
        df = geocode_sheet(df, postcode_col, postcode_cache)
        xls[sheet_name] = df  # put it back

    print(f"\nWriting output to {OUTPUT_EXCEL} ...")
    with pd.ExcelWriter(OUTPUT_EXCEL, engine="openpyxl") as writer:
        for name, df in xls.items():
            df.to_excel(writer, sheet_name=name, index=False)

    print("Done.")


if __name__ == "__main__":
    main()
