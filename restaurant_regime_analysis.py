"""
Restaurant Regime Change Analysis
==================================
Tests whether the gas price → restaurant traffic sensitivity has changed
in the post-2020 cumulative inflation regime.

Requires: Gas-prices-vs-other-variables.xlsx in working directory
Outputs:  output/ folder with charts (.png) and results (.csv)
"""

import os
import json
import warnings
import numpy as np
import pandas as pd
import statsmodels.api as sm
from scipy import stats
import plotly.graph_objects as go
import plotly.express as px
import plotly.io as pio

warnings.filterwarnings("ignore")
os.makedirs("output", exist_ok=True)

# ─────────────────────────────────────────────────────────────────────────────
# 1. LOAD & CLEAN DATA
# ─────────────────────────────────────────────────────────────────────────────

def load_data(path="Gas-prices-vs-other-variables.xlsx"):
    """
    Loads both sheets. Note: sheet naming in the file is inverted from
    econometric convention — 'Dependent Variables' = RHS, 'Independent Variables' = LHS.
    """
    # RHS: gas prices, CPI, FAFH CPI, real DPI, restaurant-grocery spread, unemployment, sentiment
    rhs = pd.read_excel(path, sheet_name="Dependent Variables", header=1)
    rhs.columns = [
        "date", "gas_price", "cpi", "fafh_cpi",
        "real_dpi", "rest_less_grocery_yoy", "unemployment", "sentiment"
    ]
    rhs = rhs[rhs["date"].notna() & (rhs["date"] != "Month")].copy()
    rhs["date"] = pd.to_datetime(rhs["date"])
    for c in rhs.columns[1:]:
        rhs[c] = pd.to_numeric(rhs[c], errors="coerce")

    # LHS: real restaurant spend, QSR traffic (Black Box), casual dining traffic, MCD SSS
    lhs = pd.read_excel(path, sheet_name="Independent Variables", header=1)
    lhs.columns = ["date", "real_rest_spend", "qsr_traffic", "casual_traffic", "quarter", "mcd_sss"]
    lhs = lhs[lhs["date"].notna() & (lhs["date"] != "Month")].copy()
    lhs["date"] = pd.to_datetime(lhs["date"])
    for c in ["real_rest_spend", "qsr_traffic", "casual_traffic"]:
        lhs[c] = pd.to_numeric(lhs[c], errors="coerce")

    # Merge on date
    df = pd.merge(rhs, lhs[["date", "real_rest_spend", "qsr_traffic", "casual_traffic"]], on="date", how="left")
    df = df.sort_values("date").reset_index(drop=True)
    return df


# ─────────────────────────────────────────────────────────────────────────────
# 2. CONSTRUCT DERIVED VARIABLES
# ─────────────────────────────────────────────────────────────────────────────

def build_variables(df, base_year=2019):
    """
    Constructs all analytical variables needed for the regression models.
    """
    # Real gas price: deflate nominal gas by headline CPI, rebased to base_year avg
    cpi_base  = df[df["date"].dt.year == base_year]["cpi"].mean()
    df["real_gas"] = df["gas_price"] / df["cpi"] * cpi_base

    # Cumulative FAFH inflation index (base_year = 100)
    fafh_base = df[df["date"].dt.year == base_year]["fafh_cpi"].mean()
    df["cum_inflation"] = df["fafh_cpi"] / fafh_base * 100

    # YoY % changes
    df["real_gas_yoy"]        = df["real_gas"].pct_change(12)
    df["real_rest_spend_yoy"] = df["real_rest_spend"].pct_change(12)
    df["real_dpi_yoy"]        = df["real_dpi"].pct_change(12)

    # Interaction term: gas shock × inflation regime
    df["gas_x_cuminflation"]  = df["real_gas_yoy"] * df["cum_inflation"]

    # Lagged gas (consumer adjustment lag)
    df["real_gas_yoy_lag1"]   = df["real_gas_yoy"].shift(1)
    df["real_gas_yoy_lag3"]   = df["real_gas_yoy"].shift(3)
    df["real_gas_yoy_lag6"]   = df["real_gas_yoy"].shift(6)

    # Grocery-restaurant price spread LEVEL (for regime indicator alternative)
    # Higher = restaurant more expensive relative to grocery → substitution pressure
    df["rest_gro_spread_level"] = df["fafh_cpi"] / df["cpi"] * 100  # relative price index

    return df


# ─────────────────────────────────────────────────────────────────────────────
# 3. SAMPLE FILTERS
# ─────────────────────────────────────────────────────────────────────────────

def get_samples(df):
    """
    Returns analysis-ready sub-samples with COVID distortion removed.
    COVID window: March 2020 – September 2021 (extreme outliers dominate OLS).
    """
    covid_mask  = (df["date"] >= "2020-03-01") & (df["date"] <= "2021-09-01")
    df_exc      = df[~covid_mask].copy()

    # Full long sample (ex-COVID): 1993–2025 for real restaurant spend
    df_long     = df_exc.dropna(subset=["real_rest_spend_yoy", "real_gas_yoy",
                                         "unemployment", "sentiment"]).copy()

    # Black Box sample (ex-COVID): 2013–2025 for QSR / casual traffic
    df_bb       = df_exc[df_exc["qsr_traffic"].notna()].dropna(
                      subset=["real_gas_yoy", "unemployment", "sentiment"]).copy()

    # Gas-shock-only sample (Black Box, only months where real gas is rising YoY)
    # Tests regime change conditional on actual gas shocks being active
    df_bb_shock = df_bb[df_bb["real_gas_yoy"] > 0].copy()

    # Pre / post regime split at Jan 2022 (cum_inflation crossed ~115)
    df_pre_reg  = df_bb[df_bb["date"] < "2022-01-01"].copy()
    df_post_reg = df_bb[df_bb["date"] >= "2022-01-01"].copy()

    return {
        "long":     df_long,
        "bb":       df_bb,
        "bb_shock": df_bb_shock,
        "pre_reg":  df_pre_reg,
        "post_reg": df_post_reg,
    }


# ─────────────────────────────────────────────────────────────────────────────
# 4. OLS WITH HAC STANDARD ERRORS (Newey-West, 12-month lag)
# ─────────────────────────────────────────────────────────────────────────────

def run_hac_ols(data, y_col, x_cols, max_lags=12):
    """
    OLS with Newey-West HAC standard errors.
    Returns statsmodels result object.
    """
    d = data[[y_col] + x_cols].dropna()
    X = sm.add_constant(d[x_cols])
    res = sm.OLS(d[y_col], X).fit(cov_type="HAC", cov_kwds={"maxlags": max_lags})
    return res, d


def results_table(res, label=""):
    """Returns a clean DataFrame summary of regression results."""
    tbl = pd.DataFrame({
        "coef": res.params,
        "se":   res.bse,
        "t":    res.tvalues,
        "p":    res.pvalues,
        "sig":  res.pvalues.apply(lambda p: "***" if p<0.01 else "**" if p<0.05 else "*" if p<0.10 else "")
    }).round(4)
    tbl.attrs["label"] = label
    tbl.attrs["N"]     = int(res.nobs)
    tbl.attrs["R2"]    = round(res.rsquared, 3)
    tbl.attrs["R2adj"] = round(res.rsquared_adj, 3)
    return tbl


# ─────────────────────────────────────────────────────────────────────────────
# 5. MODELS
# ─────────────────────────────────────────────────────────────────────────────

BASE_CONTROLS = ["real_dpi_yoy", "unemployment", "sentiment"]
BB_CONTROLS   = BASE_CONTROLS + ["rest_less_grocery_yoy"]

def run_all_models(samples):
    """Run all regression specifications and return dict of results."""
    results = {}

    # ── Model 1: Real restaurant spend — masking mechanism (long sample) ──
    xvars_m1 = ["real_gas_yoy", "cum_inflation", "gas_x_cuminflation"] + BASE_CONTROLS
    res, d = run_hac_ols(samples["long"], "real_rest_spend_yoy", xvars_m1)
    results["m1_spend"] = (res, d, "Model 1: Real Restaurant Spend YoY (1993-2025 ex-COVID)")

    # ── Model 2a: QSR traffic — full Black Box sample ──
    xvars_m2 = ["real_gas_yoy", "cum_inflation", "gas_x_cuminflation"] + BB_CONTROLS
    res, d = run_hac_ols(samples["bb"], "qsr_traffic", xvars_m2)
    results["m2_qsr"] = (res, d, "Model 2a: QSR Traffic YoY (Black Box 2013-2025 ex-COVID)")

    # ── Model 2b: QSR traffic — lagged gas price (3-month lag) ──
    xvars_m2b = ["real_gas_yoy_lag3", "cum_inflation", "gas_x_cuminflation"] + BB_CONTROLS
    res, d = run_hac_ols(samples["bb"], "qsr_traffic", xvars_m2b)
    results["m2_qsr_lag3"] = (res, d, "Model 2b: QSR Traffic YoY — Lag 3M Gas")

    # ── Model 3: QSR traffic — gas-shock episodes only ──
    # Tests sensitivity conditional on gas prices actually rising
    xvars_m3 = ["real_gas_yoy", "cum_inflation", "gas_x_cuminflation"] + BB_CONTROLS
    res, d = run_hac_ols(samples["bb_shock"], "qsr_traffic", xvars_m3)
    results["m3_shock"] = (res, d, "Model 3: QSR Traffic — Gas Shock Episodes Only")

    # ── Model 4: Casual dining traffic ──
    xvars_m4 = ["real_gas_yoy", "cum_inflation", "gas_x_cuminflation"] + BB_CONTROLS
    res, d = run_hac_ols(samples["bb"], "casual_traffic", xvars_m4)
    results["m4_casual"] = (res, d, "Model 4: Casual Dining Traffic YoY (Black Box 2013-2025 ex-COVID)")

    # ── Model 5: Pre-regime vs post-regime split (Chow test basis) ──
    xvars_chow = ["real_gas_yoy"] + BB_CONTROLS
    res_pre, d_pre = run_hac_ols(samples["pre_reg"],  "qsr_traffic", xvars_chow, max_lags=6)
    res_post, d_post = run_hac_ols(samples["post_reg"], "qsr_traffic", xvars_chow, max_lags=6)
    res_full, d_full = run_hac_ols(samples["bb"],       "qsr_traffic", xvars_chow, max_lags=6)
    results["m5_pre"]  = (res_pre,  d_pre,  "Model 5 Pre-2022 (cum_inflation < ~115)")
    results["m5_post"] = (res_post, d_post, "Model 5 Post-2022 (cum_inflation > ~115)")
    results["m5_full"] = (res_full, d_full, "Model 5 Full (Chow base)")

    return results


def chow_test(res_full, res_pre, res_post, k):
    """Compute Chow F-statistic and p-value for structural break."""
    rss_r = (res_full.resid**2).sum()
    rss_u = (res_pre.resid**2).sum() + (res_post.resid**2).sum()
    n     = int(res_full.nobs)
    F     = ((rss_r - rss_u) / k) / (rss_u / (n - 2*k))
    p     = 1 - stats.f.cdf(F, k, n - 2*k)
    return F, p


def marginal_effect_gas(res, ci_range, coef_gas="real_gas_yoy", coef_int="gas_x_cuminflation"):
    """
    Computes marginal effect of real_gas_yoy on LHS at each cum_inflation level,
    with delta-method standard errors.
    Returns (marg_eff, se_marg) arrays.
    """
    b_gas = res.params[coef_gas]
    b_int = res.params[coef_int]
    cov   = res.cov_params()
    marg  = b_gas + b_int * ci_range
    var   = (cov.loc[coef_gas, coef_gas]
             + ci_range**2 * cov.loc[coef_int, coef_int]
             + 2 * ci_range * cov.loc[coef_gas, coef_int])
    se    = np.sqrt(np.maximum(var, 0))
    return marg, se


# ─────────────────────────────────────────────────────────────────────────────
# 6. STRUCTURAL BREAK — SEQUENTIAL RSS MINIMIZATION (Bai-Perron style)
# ─────────────────────────────────────────────────────────────────────────────

def find_structural_break(df_clean, y_col, x_cols, min_periods=36):
    """
    Single structural break: finds date minimizing RSS(pre) + RSS(post).
    Returns DataFrame of all break candidates sorted by RSS.
    """
    df_clean = df_clean[['date', y_col] + x_cols].dropna().reset_index(drop=True)
    records  = []
    for i in range(min_periods, len(df_clean) - min_periods):
        for subset in [df_clean.iloc[:i], df_clean.iloc[i:]]:
            if len(subset) < len(x_cols) + 2:
                break
        pre  = df_clean.iloc[:i]
        post = df_clean.iloc[i:]
        rss  = 0
        for seg in [pre, post]:
            X = sm.add_constant(seg[x_cols])
            try:
                r = sm.OLS(seg[y_col], X).fit()
                rss += (r.resid**2).sum()
            except:
                rss = np.inf
                break
        records.append({"date": df_clean["date"].iloc[i], "rss": rss,
                         "n_pre": i, "n_post": len(df_clean)-i})
    return pd.DataFrame(records).sort_values("rss").reset_index(drop=True)


# ─────────────────────────────────────────────────────────────────────────────
# 7. ROLLING BETA
# ─────────────────────────────────────────────────────────────────────────────

def rolling_gas_beta(df_clean, y_col, gas_col, control_cols, window=24):
    """
    Rolling partial beta of gas price on traffic, controlling for other factors.
    Uses OLS on each window; returns DataFrame with date, beta, and R².
    """
    df_c   = df_clean[['date', y_col, gas_col] + control_cols].dropna().reset_index(drop=True)
    xvars  = [gas_col] + control_cols
    rows   = []
    for i in range(window, len(df_c)+1):
        chunk = df_c.iloc[i-window:i]
        X = np.column_stack([np.ones(len(chunk))] + [chunk[v].values for v in xvars])
        y = chunk[y_col].values
        try:
            coefs, _, _, _ = np.linalg.lstsq(X, y, rcond=None)
            y_hat = X @ coefs
            ss_res = ((y - y_hat)**2).sum()
            ss_tot = ((y - y.mean())**2).sum()
            r2 = 1 - ss_res/ss_tot if ss_tot > 0 else np.nan
            rows.append({
                "date":        df_c["date"].iloc[i-1],
                "beta_gas":    coefs[1],
                "r2":          r2,
                "cum_inflation": df_c.get("cum_inflation", pd.Series([np.nan]*len(df_c))).iloc[i-1]
                              if "cum_inflation" in df_c.columns else np.nan
            })
        except Exception:
            pass
    return pd.DataFrame(rows)


# ─────────────────────────────────────────────────────────────────────────────
# 8. CHARTS
# ─────────────────────────────────────────────────────────────────────────────

COLORS = ["#636EFA","#EF553B","#00CC96","#AB63FA","#FFA15A",
          "#19D3F3","#FF6692","#B6E880","#FF97FF","#FECB52"]

def chart_marginal_effect(res, label, filename):
    """Chart 1: Marginal effect of gas price on spend/traffic vs cum_inflation."""
    ci_range = np.linspace(50, 145, 300)
    marg, se = marginal_effect_gas(res, ci_range)

    # Key year labels
    ci_labels = {2013: 78, 2019: 100, 2022: 116, 2025: 137}

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=np.concatenate([ci_range, ci_range[::-1]]),
        y=np.concatenate([marg + 1.96*se, (marg - 1.96*se)[::-1]]),
        fill="toself", fillcolor="rgba(99,110,250,0.12)",
        line=dict(color="rgba(0,0,0,0)"), name="95% CI"
    ))
    fig.add_trace(go.Scatter(
        x=ci_range, y=marg, mode="lines",
        line=dict(color=COLORS[0], width=3), name="Marginal effect"
    ))
    fig.add_hline(y=0, line_dash="dash", line_color="#666", line_width=1.5)
    for yr, ci_val in ci_labels.items():
        me = float(res.params["real_gas_yoy"] + res.params["gas_x_cuminflation"] * ci_val)
        fig.add_annotation(x=ci_val, y=me + 0.015, text=str(yr),
                           showarrow=True, arrowhead=2, arrowsize=0.8,
                           font=dict(size=11), ax=0, ay=-25)
    fig.update_layout(
        title={"text": f"{label}<br><span style='font-size:14px;font-weight:normal'>Delta method 95% CI | Gas shock × cum inflation interaction</span>"},
        legend=dict(orientation="h", yanchor="top",    y=-0.15, xanchor="center", x=0.5)
    )
    fig.update_xaxes(title_text="Cum. Inflation (2019=100)", tickmode="linear", dtick=10)
    fig.update_yaxes(title_text="dSpend/dGas YoY", tickformat=".2f")
    fig.write_image(f"output/{filename}.png")
    with open(f"output/{filename}.png.meta.json", "w") as f:
        json.dump({"caption": f"Marginal Effect Plot: {label}",
                   "description": "How gas price sensitivity of restaurant spend/traffic varies with cumulative FAFH inflation level."}, f)


def chart_rolling_beta(roll_df, cum_df, filename):
    """Chart 2: Rolling 24M gas price beta on QSR traffic, with cum_inflation overlay."""
    merged = roll_df.merge(cum_df[["date","cum_inflation"]], on="date", how="left")

    fig = go.Figure()
    fig.add_hline(y=0, line_dash="dash", line_color="#999", line_width=1)
    fig.add_trace(go.Scatter(
        x=merged["date"], y=merged["beta_gas"],
        mode="lines", name="Rolling 24M Gas Beta",
        line=dict(color=COLORS[0], width=2.5)
    ))
    fig.add_trace(go.Scatter(
        x=merged["date"], y=merged["cum_inflation_y"] / 1000,
        mode="lines", name="Cum. Inflation / 1000",
        line=dict(color=COLORS[1], width=1.5, dash="dot")
    ))
    fig.update_layout(
        title={"text": "Rolling 24M Gas Price Beta on QSR Traffic (2015-2025)<br>"
                       "<span style='font-size:14px;font-weight:normal'>Black Box ex-COVID | Dotted = cum inflation / 1000</span>"},
        legend=dict(orientation="h", yanchor="top",    y=-0.15, xanchor="center", x=0.5)
    )
    fig.update_xaxes(title_text="Date")
    fig.update_yaxes(title_text="Beta / Index")
    fig.write_image(f"output/{filename}.png")
    with open(f"output/{filename}.png.meta.json", "w") as f:
        json.dump({"caption": "Rolling 24M Gas Price Beta on QSR Traffic vs Cumulative Inflation",
                   "description": "Rolling partial beta of real gas price YoY on QSR traffic YoY (controlling for DPI, unemployment, sentiment, grocery spread) vs cumulative FAFH inflation level."}, f)


def chart_traffic_drivers(df_bb, filename):
    """Chart 3: QSR traffic vs grocery spread and real gas, time series."""
    d = df_bb[["date","qsr_traffic","rest_less_grocery_yoy","real_gas_yoy","cum_inflation"]].dropna()

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=d["date"], y=d["qsr_traffic"],
        name="QSR Traffic YoY", marker_color=COLORS[0], opacity=0.6
    ))
    fig.add_trace(go.Scatter(
        x=d["date"], y=d["rest_less_grocery_yoy"],
        mode="lines", name="Rest-Grocery Spread YoY",
        line=dict(color=COLORS[1], width=2)
    ))
    fig.add_trace(go.Scatter(
        x=d["date"], y=d["real_gas_yoy"] * 0.1,
        mode="lines", name="Real Gas YoY × 0.1",
        line=dict(color=COLORS[2], width=1.5, dash="dot")
    ))
    fig.update_layout(
        title={"text": "QSR Traffic vs Key Drivers (2013-2025 ex-COVID)<br>"
                       "<span style='font-size:14px;font-weight:normal'>Black Box | Real gas scaled ×0.1 for axis</span>"},
        legend=dict(orientation="h", yanchor="top",    y=-0.15, xanchor="center", x=0.5)
    )
    fig.update_xaxes(title_text="Date")
    fig.update_yaxes(title_text="YoY Change")
    fig.write_image(f"output/{filename}.png")
    with open(f"output/{filename}.png.meta.json", "w") as f:
        json.dump({"caption": "QSR Traffic vs Grocery Spread and Gas Price (2013–2025)",
                   "description": "Bar = QSR traffic YoY. Red line = restaurant-less-grocery YoY spread (the key substitution driver). Dotted = real gas YoY scaled by 0.1."}, f)


def chart_regime_scatter(df_bb, filename):
    """Chart 4: Gas price YoY vs QSR traffic, colored by inflation regime."""
    d = df_bb[["date","qsr_traffic","real_gas_yoy","cum_inflation"]].dropna().copy()
    d["regime"] = pd.cut(d["cum_inflation"],
                          bins=[0, 105, 115, 200],
                          labels=["Low (<105)", "Transition (105-115)", "High (>115)"])
    regime_colors = {
        "Low (<105)":           COLORS[0],
        "Transition (105-115)": COLORS[4],
        "High (>115)":          COLORS[1]
    }
    fig = go.Figure()
    for reg, grp in d.groupby("regime"):
        fig.add_trace(go.Scatter(
            x=grp["real_gas_yoy"], y=grp["qsr_traffic"],
            mode="markers", name=str(reg),
            marker=dict(color=regime_colors.get(str(reg), "gray"), size=7, opacity=0.75)
        ))
    fig.add_hline(y=0, line_dash="dash", line_color="#aaa", line_width=1)
    fig.add_vline(x=0, line_dash="dash", line_color="#aaa", line_width=1)
    fig.update_layout(
        title={"text": "Gas Shock vs QSR Traffic by Inflation Regime<br>"
                       "<span style='font-size:14px;font-weight:normal'>Black Box ex-COVID | Color = cum inflation regime</span>"},
        legend=dict(orientation="h", yanchor="top",    y=-0.15, xanchor="center", x=0.5)
    )
    fig.update_xaxes(title_text="Real Gas YoY", tickformat=".0%")
    fig.update_yaxes(title_text="QSR Traffic YoY", tickformat=".0%")
    fig.update_traces(cliponaxis=False)
    fig.write_image(f"output/{filename}.png")
    with open(f"output/{filename}.png.meta.json", "w") as f:
        json.dump({"caption": "Gas Price Shock vs QSR Traffic by Inflation Regime",
                   "description": "Scatter of real gas price YoY vs QSR traffic YoY. Points colored by cumulative FAFH inflation regime. Tests whether slope differs between low, transition, and high inflation periods."}, f)


# ─────────────────────────────────────────────────────────────────────────────
# 9. SAVE OUTPUTS
# ─────────────────────────────────────────────────────────────────────────────

def save_model_outputs(results, df, samples):
    """Save regression tables and fitted values to CSV."""
    rows = []
    for key, (res, d, label) in results.items():
        tbl = results_table(res, label)
        tbl.to_csv(f"output/model_{key}.csv")
        rows.append({
            "model": key, "label": label,
            "N": int(res.nobs), "R2": round(res.rsquared, 3),
            "R2_adj": round(res.rsquared_adj, 3),
            "AIC": round(res.aic, 1), "BIC": round(res.bic, 1)
        })
    pd.DataFrame(rows).to_csv("output/model_summary.csv", index=False)

    # Full analytical dataset with constructed variables
    df.to_csv("output/analytical_dataset.csv", index=False)
    print("Outputs saved to output/")


# ─────────────────────────────────────────────────────────────────────────────
# 10. MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print("Loading data...")
    df = load_data()
    df = build_variables(df)
    samples = get_samples(df)

    print(f"  Long sample (ex-COVID): N={len(samples['long'])}")
    print(f"  Black Box sample (ex-COVID): N={len(samples['bb'])}")
    print(f"  Gas-shock episodes only: N={len(samples['bb_shock'])}")

    print("\nRunning regressions...")
    results = run_all_models(samples)

    for key, (res, d, label) in results.items():
        tbl = results_table(res, label)
        print(f"\n{label}")
        print(f"  N={tbl.attrs['N']} | R²={tbl.attrs['R2']} | Adj R²={tbl.attrs['R2adj']}")
        print(tbl.to_string())

    # Chow test
    res_pre  = results["m5_pre"][0]
    res_post = results["m5_post"][0]
    res_full = results["m5_full"][0]
    k        = len(results["m5_full"][0].params)
    F, p     = chow_test(res_full, res_pre, res_post, k)
    print(f"\nChow Test (break Jan 2022): F={F:.3f}, p={p:.4f}")
    print(f"  Pre-2022 gas beta:  {res_pre.params['real_gas_yoy']:.4f}  (p={res_pre.pvalues['real_gas_yoy']:.3f})")
    print(f"  Post-2022 gas beta: {res_post.params['real_gas_yoy']:.4f} (p={res_post.pvalues['real_gas_yoy']:.3f})")

    # Structural break
    print("\nFinding structural break in real restaurant spend...")
    sb = find_structural_break(
        samples["long"], "real_rest_spend_yoy",
        ["real_gas_yoy", "real_dpi_yoy", "unemployment", "sentiment"]
    )
    print(f"  Optimal break: {sb['date'].iloc[0].strftime('%Y-%m')}")
    print(sb.head(5).to_string(index=False))

    # Rolling beta
    print("\nComputing rolling 24M gas beta on QSR traffic...")
    roll_df = rolling_gas_beta(
        samples["bb"].assign(cum_inflation=samples["bb"]["cum_inflation"]),
        y_col="qsr_traffic", gas_col="real_gas_yoy",
        control_cols=["real_dpi_yoy", "unemployment", "sentiment", "rest_less_grocery_yoy"],
        window=24
    )

    # Charts
    print("\nGenerating charts...")
    chart_marginal_effect(results["m1_spend"][0],
                          "Gas Price Sensitivity Falls as Inflation Rises",
                          "chart1_masking_spend")
    chart_regime_scatter(samples["bb"], "chart2_regime_scatter")
    chart_traffic_drivers(samples["bb"], "chart3_traffic_drivers")
    chart_rolling_beta(roll_df, samples["bb"][["date","cum_inflation"]], "chart4_rolling_beta")

    # Save outputs
    save_model_outputs(results, df, samples)
    print("\nDone. All outputs in output/")


if __name__ == "__main__":
    main()
