import pandas as pd

# ------------ 1. Load and prepare data ------------

# Adjust this if your file name differs
file_path = "data.xlsx"  # or "Data.xlsx" if that's what you have
sheet_name = "Gas prices vs indicators"

# Row 1 (0-based) is the header row in your file, row 0 is all NaNs
df = pd.read_excel(file_path, sheet_name=sheet_name, header=1)

# Clean column names
df.columns = df.columns.astype(str).str.strip()

# Expecting: Month, Gas Prices, QSR, Fast Casual, Casual Dining, Disp. Income
# Parse Month and set as index
df['Month'] = pd.to_datetime(df['Month'])
df = df.set_index('Month').sort_index()

# Rename to simpler internal names
df = df.rename(columns={
    'Gas Prices': 'gas',
    'QSR': 'qsr',
    'Fast Casual': 'fast_casual',
    'Casual Dining': 'casual_dining',
    'Disp. Income': 'disp_income'
})

cols = ['gas', 'qsr', 'fast_casual', 'casual_dining', 'disp_income']

# Ensure numeric types
df[cols] = df[cols].astype(float)

# ------------ 2. Full-sample correlation matrix ------------

corr_matrix = df[cols].corr(method='pearson')
print("Full-sample correlation matrix (all series):")
print(corr_matrix)

# ------------ 3. Full-sample correlations and R^2 vs gas (with leads/lags) ------------

results = []

# Contemporaneous correlations
for col in cols:
    if col == 'gas':
        continue
    sub = df[['gas', col]].dropna()
    if len(sub) < 3:
        continue

    r = sub['gas'].corr(sub[col])
    r_squared = r ** 2

    results.append({
        'indicator': col,
        'lag_months': 0,
        'n_obs': len(sub),
        'corr_vs_gas': r,
        'r_squared_vs_gas': r_squared
    })

# Lead/lag correlations: positive lag = gas leads, negative lag = gas lags
lags = [-3, -2, -1, 1, 2, 3]

for col in cols:
    if col == 'gas':
        continue

    for lag in lags:
        gas_shifted = df['gas'].shift(-lag)  # -lag so positive lag means gas leads
        sub = pd.concat([gas_shifted, df[col]], axis=1,
                        keys=['gas_shifted', col]).dropna()

        if len(sub) < 3:
            continue

        r = sub['gas_shifted'].corr(sub[col])
        r_squared = r ** 2

        results.append({
            'indicator': col,
            'lag_months': lag,
            'n_obs': len(sub),
            'corr_vs_gas': r,
            'r_squared_vs_gas': r_squared
        })

results_df = pd.DataFrame(results).sort_values(['indicator', 'lag_months'])

print("\nFull-sample correlations and R-squared vs Gas (with leads/lags):")
print(results_df)

# ------------ 4. Windowed correlations around shock periods ------------

# Define your shock windows here; adjust dates if you want tighter windows
windows = {
    "oil_collapse_2014_2016": ("2014-07-01", "2016-02-28"),
    "inflation_surge_2021_2022": ("2021-01-01", "2022-12-31")
}

window_results = []

for name, (start, end) in windows.items():
    sub_df = df.loc[start:end, cols].dropna(how="all")

    print(f"\nWindow: {name}, observations: {len(sub_df)}")

    if len(sub_df) < 6:
        print("  Skipping window (too few observations).")
        continue

    # Pairwise matrix in that window
    w_corr = sub_df.corr(method="pearson")
    print(f"  Correlation matrix for {name}:")
    print(w_corr)

    # Save window-specific matrix
    w_corr.to_csv(f"corr_matrix_{name}.csv")

    # Correlations vs gas with leads/lags in that window
    for col in cols:
        if col == "gas":
            continue

        # Contemporaneous
        s = sub_df[['gas', col]].dropna()
        if len(s) >= 3:
            r = s['gas'].corr(s[col])
            window_results.append({
                'window': name,
                'indicator': col,
                'lag_months': 0,
                'n_obs': len(s),
                'corr_vs_gas': r,
                'r_squared_vs_gas': r**2
            })

        # Leads/lags
        for lag in lags:
            gas_shifted = sub_df['gas'].shift(-lag)
            s = pd.concat([gas_shifted, sub_df[col]], axis=1,
                          keys=['gas_shifted', col]).dropna()
            if len(s) < 3:
                continue
            r = s['gas_shifted'].corr(s[col])
            window_results.append({
                'window': name,
                'indicator': col,
                'lag_months': lag,
                'n_obs': len(s),
                'corr_vs_gas': r,
                'r_squared_vs_gas': r**2
            })

window_results_df = pd.DataFrame(window_results).sort_values(
    ['window', 'indicator', 'lag_months']
)

print("\nWindowed correlations and R-squared vs Gas (by window, with leads/lags):")
print(window_results_df)

# ------------ 5. Save all outputs to CSV ------------

corr_matrix.to_csv("correlation_matrix_all_series.csv")
results_df.to_csv("correlation_vs_gas_with_lags.csv", index=False)
window_results_df.to_csv("correlation_vs_gas_by_window.csv", index=False)

print("\nSaved files:")
print(" - correlation_matrix_all_series.csv")
print(" - correlation_vs_gas_with_lags.csv")
print(" - corr_matrix_oil_collapse_2014_2016.csv")
print(" - corr_matrix_inflation_surge_2021_2022.csv")
print(" - correlation_vs_gas_by_window.csv")
