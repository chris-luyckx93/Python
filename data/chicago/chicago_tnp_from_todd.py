import json
from pathlib import Path

import pandas as pd

RAW_JSON = Path("data/chicago/chicago_dashboard_data_raw.json")
OUTPUT_DAILY_CSV = Path("data/chicago/chicago_tnp_daily_from_todd.csv")
OUTPUT_MONTHLY_CSV = Path("data/chicago/chicago_tnp_monthly_from_todd.csv")

# if auto-detect fails, you can set these manually later:
DATE_COL_FALLBACK = None   # e.g. "date"
TRIP_COL_FALLBACK = None   # e.g. "trips"


def main():
    if not RAW_JSON.exists():
        raise FileNotFoundError(f"Raw JSON not found at {RAW_JSON}")

    with open(RAW_JSON) as f:
        data = json.load(f)

    # Pull out the daily TNP block
    if "tnp_daily" not in data:
        raise KeyError("Key 'tnp_daily' not found in JSON. Top-level keys are: "
                       + ", ".join(data.keys()))

    tnp_daily = data["tnp_daily"]

    if not isinstance(tnp_daily, list) or not tnp_daily:
        raise ValueError("'tnp_daily' is not a non-empty list; got type "
                         f"{type(tnp_daily)}")

    df = pd.DataFrame(tnp_daily)

    print("Columns in tnp_daily:")
    print(list(df.columns))

    # ---- Detect date column ----
    date_col = DATE_COL_FALLBACK
    if date_col is None:
        # heuristic: look for a column name containing 'date'
        candidates = [c for c in df.columns if "date" in c.lower()]
        if candidates:
            date_col = candidates[0]

    if date_col is None or date_col not in df.columns:
        raise ValueError(
            "Could not automatically detect the date column. "
            "Columns are: " + ", ".join(df.columns)
        )

    # ---- Detect trip column ----
    trip_col = TRIP_COL_FALLBACK
    if trip_col is None:
        # common possibilities, in order of preference
        for c in ["tnp_trips", "trips", "total_trips"]:
            if c in df.columns:
                trip_col = c
                break

    if trip_col is None or trip_col not in df.columns:
        raise ValueError(
            "Could not automatically detect the trip count column. "
            "Columns are: " + ", ".join(df.columns)
        )

    print(f"\nUsing date column: {date_col}")
    print(f"Using trips column: {trip_col}")

    # ---- Clean daily data ----
    df[date_col] = pd.to_datetime(df[date_col])
    df = df.sort_values(date_col).reset_index(drop=True)

    # Save daily snapshot (optional, but nice to have)
    OUTPUT_DAILY_CSV.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_DAILY_CSV, index=False)
    print(f"\n✅ Saved daily TNP data to {OUTPUT_DAILY_CSV}")

    # ---- Aggregate to monthly ----
    df["year"] = df[date_col].dt.year
    df["month_num"] = df[date_col].dt.month
    df["month_start"] = df[date_col].dt.to_period("M").dt.to_timestamp()

    monthly = (
        df.groupby(["year", "month_num", "month_start"], as_index=False)[trip_col]
        .sum()
        .rename(columns={trip_col: "trips_chicago_tnp"})
        .sort_values(["year", "month_num"])
        .reset_index(drop=True)
    )

    print("\nMonthly aggregated data (first rows):")
    print(monthly.head())

    monthly.to_csv(OUTPUT_MONTHLY_CSV, index=False)
    print(f"\n✅ Saved monthly Chicago TNP data to {OUTPUT_MONTHLY_CSV}")
    print(f"Rows: {len(monthly)}")


if __name__ == "__main__":
    main()
