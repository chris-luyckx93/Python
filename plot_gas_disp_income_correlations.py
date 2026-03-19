import pandas as pd
import plotly.express as px

# Load the correlation results
results_df = pd.read_csv("gas_vs_disp_income_shocks.csv")

# 1) Line chart: full-sample correlation vs lag
full = results_df[results_df["window"] == "full_sample"].copy()
full = full.sort_values("lag_months")

fig1 = px.line(
    full,
    x="lag_months",
    y="corr_vs_gas",
    markers=True,
    title="Gas vs DI correlation by lag (full sample)"
)
fig1.update_xaxes(title_text="Lag (months)")
fig1.update_yaxes(title_text="Correlation")
fig1.update_traces(cliponaxis=False, fill="tozeroy")
fig1.update_layout(
    legend=dict(
        orientation="h",
        yanchor="bottom",
        y=1.05,
        xanchor="center",
        x=0.5
    )
)

# Save as interactive HTML instead of PNG
fig1.write_html("full_sample_corr_lags.html")

# 2) Multi-line chart: major shock windows
shock_windows = [
    "oil_spike_2007_2008",
    "libya_arab_spring_2011",
    "iraq_war_2003",
    "russia_ukraine_2022"
]

shock_df = results_df[results_df["window"].isin(shock_windows)].copy()
shock_df = shock_df.sort_values(["window", "lag_months"])

fig2 = px.line(
    shock_df,
    x="lag_months",
    y="corr_vs_gas",
    color="window",
    markers=True,
    title="Gas vs DI correlation by lag (major supply shocks)"
)
fig2.update_xaxes(title_text="Lag (months)")
fig2.update_yaxes(title_text="Correlation")
fig2.update_traces(cliponaxis=False)
fig2.update_layout(
    legend=dict(
        orientation="h",
        yanchor="bottom",
        y=1.05,
        xanchor="center",
        x=0.5
    )
)

fig2.write_html("shock_windows_corr_lags.html")

print("Saved charts: full_sample_corr_lags.html, shock_windows_corr_lags.html")
