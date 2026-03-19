import pandas as pd

file_path = "Gas prices vs Disposable income.xlsx"
sheet_name = "Gas prices vs Disposable income"

# ------------ 1. Load with correct header ------------

df = pd.read_excel(file_path, sheet_name=sheet_name, header=1)

# Clean column names
df.columns = df.columns.astype(str).str.strip()

# Column names from your printout:
date_col = "Month"
gas_col = "Gas Prices"
disp_col = "Disp. Income"

# ------------ 2. Prepare data ------------

df[date_col] = pd.to_datetime(df[date_col])
df = df.set_index(date_col).sort_index()

df = df[[gas_col, disp_col]].rename(columns={
    gas_col: "gas",
    disp_col: "disp_income"
})

df["gas"] = pd.to_numeric(df["gas"], errors="coerce")
df["disp_income"] = pd.to_numeric(df["disp_income"], errors="coerce")
df = df.dropna(how="all", subset=["gas", "disp_income"])

print("Data span:", df.index.min(), "to", df.index.max())
print("Head:")
print(df.head())

# ------------ 3. Full-sample correlations with lags/leads ------------

results = []

# Contemporaneous
sub = df[["gas", "disp_income"]].dropna()
if len(sub) >= 3:
    r = sub["gas"].corr(sub["disp_income"])
    results.append({
        "window": "full_sample",
        "lag_months": 0,
        "n_obs": len(sub),
        "corr_vs_gas": r,
        "r_squared_vs_gas": r**2
    })

# Lags/leads (monthly): positive lag = gas leads disposable income
lags = [-6, -3, -2, -1, 1, 2, 3, 6]

for lag in lags:
    gas_shifted = df["gas"].shift(-lag)
    sub = pd.concat(
        [gas_shifted, df["disp_income"]],
        axis=1,
        keys=["gas_shifted", "disp_income"]
    ).dropna()
    if len(sub) < 3:
        continue
    r = sub["gas_shifted"].corr(sub["disp_income"])
    results.append({
        "window": "full_sample",
        "lag_months": lag,
        "n_obs": len(sub),
        "corr_vs_gas": r,
        "r_squared_vs_gas": r**2
    })

# ------------ 4. Supply-side shock windows ------------

windows = {
    "gulf_crisis_1990_1991": ("1990-08-01", "1991-03-31"),
    "iraq_war_2003": ("2002-09-01", "2003-09-30"),
    "oil_spike_2007_2008": ("2007-01-01", "2008-09-30"),
    "libya_arab_spring_2011": ("2010-12-01", "2011-12-31"),
    "russia_ukraine_2022": ("2021-10-01", "2022-12-31"),
}

for name, (start, end) in windows.items():
    sub_df = df.loc[start:end, ["gas", "disp_income"]].dropna(how="any")
    print(f"\nWindow: {name}, observations: {len(sub_df)}")
    if len(sub_df) < 6:
        print("  Skipping (too few observations).")
        continue

    # Contemporaneous
    s = sub_df[["gas", "disp_income"]].dropna()
    if len(s) >= 3:
        r = s["gas"].corr(s["disp_income"])
        results.append({
            "window": name,
            "lag_months": 0,
            "n_obs": len(s),
            "corr_vs_gas": r,
            "r_squared_vs_gas": r**2
        })

    # Leads/lags within the window
    for lag in lags:
        gas_shifted = sub_df["gas"].shift(-lag)
        s = pd.concat(
            [gas_shifted, sub_df["disp_income"]],
            axis=1,
            keys=["gas_shifted", "disp_income"]
        ).dropna()
        if len(s) < 3:
            continue
        r = s["gas_shifted"].corr(s["disp_income"])
        results.append({
            "window": name,
            "lag_months": lag,
            "n_obs": len(s),
            "corr_vs_gas": r,
            "r_squared_vs_gas": r**2
        })

# ------------ 5. Collect and save ------------

results_df = pd.DataFrame(results).sort_values(["window", "lag_months"])

print("\nCorrelations and R-squared vs Gas (by window, with leads/lags):")
print(results_df)

results_df.to_csv("gas_vs_disp_income_shocks.csv", index=False)

print("\nSaved: gas_vs_disp_income_shocks.csv")
