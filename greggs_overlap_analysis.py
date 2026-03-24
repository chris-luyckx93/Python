"""
greggs_overlap_analysis.py
Spatial overlap analysis for Greggs stores across mile increments.
Supports: self-overlap (density) and cross-brand overlap (vs competitors).
"""

import pandas as pd
import numpy as np
from sklearn.neighbors import BallTree

# ── Config ─────────────────────────────────────────────────────────────────────
GREGGS_CSV      = "greggs_stores.csv"
COMPETITOR_CSV  = "dominos_uk_stores.csv"   # swap for any competitor CSV
MILE_INCREMENTS = [0.25, 0.5, 1.0, 1.5, 2.0, 3.0, 5.0]
EARTH_RADIUS_MI = 3958.8

# ── Helpers ────────────────────────────────────────────────────────────────────
def to_radians(df: pd.DataFrame) -> np.ndarray:
    """Convert lat/lon columns to radians array for BallTree."""
    return np.radians(df[["latitude", "longitude"]].values)


def build_overlap_counts(
    query_coords_rad: np.ndarray,
    tree_coords_rad: np.ndarray,
    miles: list[float],
    exclude_self: bool = False,
) -> pd.DataFrame:
    """
    For each query point, count how many tree points fall within each mile radius.
    exclude_self=True subtracts 1 from counts (when query == tree dataset).
    Returns a DataFrame with one column per mile increment.
    """
    tree = BallTree(tree_coords_rad, metric="haversine")
    results = {}

    for m in miles:
        radius_rad = m / EARTH_RADIUS_MI
        counts = tree.query_radius(query_coords_rad, r=radius_rad, count_only=True)
        if exclude_self:
            counts = counts - 1   # remove self-match
        results[f"within_{m}mi"] = counts

    return pd.DataFrame(results)


def nearest_competitor(
    query_coords_rad: np.ndarray,
    tree_coords_rad: np.ndarray,
) -> np.ndarray:
    """Return the distance in miles to the nearest point in the tree."""
    tree = BallTree(tree_coords_rad, metric="haversine")
    dist_rad, _ = tree.query(query_coords_rad, k=1)
    return dist_rad.flatten() * EARTH_RADIUS_MI


# ── Load data ──────────────────────────────────────────────────────────────────
greggs = pd.read_csv(GREGGS_CSV).dropna(subset=["latitude", "longitude"])
print(f"Greggs stores loaded: {len(greggs):,}")

# ── 1. Self-overlap (Greggs density) ───────────────────────────────────────────
print("\n📍 Running self-overlap (Greggs vs Greggs)...")
g_rad = to_radians(greggs)
self_overlap = build_overlap_counts(g_rad, g_rad, MILE_INCREMENTS, exclude_self=True)

greggs_self = pd.concat([greggs[["shopCode", "shopName", "postCode", "city",
                                  "latitude", "longitude"]], self_overlap], axis=1)

greggs_self.to_csv("greggs_self_overlap.csv", index=False)
print(greggs_self[self_overlap.columns].describe().round(2))

# ── 2. Cross-brand overlap (Greggs vs Competitor) ─────────────────────────────
try:
    competitor = pd.read_csv(COMPETITOR_CSV).dropna(subset=["latitude", "longitude"])
    print(f"\n🍕 Competitor stores loaded: {len(competitor):,}")

    c_rad = to_radians(competitor)
    cross_overlap = build_overlap_counts(g_rad, c_rad, MILE_INCREMENTS, exclude_self=False)
    cross_overlap.columns = [f"comp_{c}" for c in cross_overlap.columns]

    # Nearest competitor distance
    greggs_self["nearest_competitor_mi"] = nearest_competitor(g_rad, c_rad)

    greggs_cross = pd.concat([
        greggs[["shopCode", "shopName", "postCode", "city", "latitude", "longitude"]],
        cross_overlap,
        greggs_self[["nearest_competitor_mi"]]
    ], axis=1)

    greggs_cross.to_csv("greggs_cross_overlap.csv", index=False)
    print(greggs_cross[cross_overlap.columns].describe().round(2))

except FileNotFoundError:
    print(f"ℹ️  No competitor file found at '{COMPETITOR_CSV}' — skipping cross-overlap.")

# ── 3. Summary table by mile increment ────────────────────────────────────────
print("\n📊 Self-overlap summary (% of stores with ≥1 Greggs nearby):")
summary_rows = []
for col in self_overlap.columns:
    miles = float(col.replace("within_", "").replace("mi", ""))
    pct_with_any = (greggs_self[col] >= 1).mean() * 100
    avg_count    = greggs_self[col].mean()
    max_count    = greggs_self[col].max()
    summary_rows.append({
        "radius_miles":   miles,
        "pct_stores_with_overlap": round(pct_with_any, 1),
        "avg_nearby_stores":       round(avg_count, 2),
        "max_nearby_stores":       int(max_count),
    })

summary = pd.DataFrame(summary_rows)
summary.to_csv("greggs_overlap_summary.csv", index=False)
print(summary.to_string(index=False))
