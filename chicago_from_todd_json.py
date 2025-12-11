import json
from pathlib import Path

import pandas as pd

RAW_JSON = Path("data/chicago/chicago_dashboard_data_raw.json")
OUTPUT_DAILY_CSV = Path("data/chicago/chicago_tnp_daily_from_todd.csv")
OUTPUT_MONTHLY_CSV = Path("data/chicago/chicago_tnp_monthly_from_todd.csv")


def main():
    if not RAW_JSON.exists():
        raise FileNotFoundError(f"Raw JSON not found at {RAW_JSON}")

    with open(RAW_JSON) as f:
        data = json.load(f)

    if "tnp_daily" not in data:
        raise KeyError(
            "Key 'tnp_daily' not found in JSON. "
            f"Top-level keys are: {', '.join(data.keys())}"
        )

    tnp_daily = data["tnp_daily"]

    # We now know tnp_daily looks like:
    # {"date": [...], "trips": [...]}
    if not isinstance(tnp_daily, dict):
        raise TypeError(f"'tnp_daily' is {type(tnp_daily)}, expected dict")

    print("tnp_daily keys:", list(tnp_daily.keys()))

    # Build DataFrame directly
    df = pd.DataFrame(tnp_daily)

    # Expect columns 'date' and 'trips'
    if "date" not in df.columns or "trips" not in df.columns:
        raise ValueError(
            "Expected 'date' and 'trips' columns in tnp_daily, got: "
            + ", ".join(df.columns)
        )

    # Convert epoch ms → datetime
    df["date"] = pd.to_datetime(df["date"], unit="ms")
    df = df.sort_values("date").reset_index(drop=True)

    # Save daily
    OUTPUT_DAILY_CSV.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_DAILY_CSV, index=False)
    print(f"\n✅ Saved daily TNP data to {OUTPUT_DAILY_CSV}")
    print("Daily preview:")
    print(df.head())

    # Aggregate to monthly
    df["year"] = df["date"].dt.year
    df["month_num"] = df["date"].dt.month
    df["month_start"] = df["date"].dt.to_period("M").dt.to_timestamp()

    monthly = (
        df.groupby(["year", "month_num", "month_start"], as_index=False)["trips"]
        .sum()
        .rename(columns={"trips": "trips_chicago_tnp"})
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
