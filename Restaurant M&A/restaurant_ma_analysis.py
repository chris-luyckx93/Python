#!/usr/bin/env python3
"""
Quick launcher for Restaurant M&A Analysis
Handles directory navigation automatically
"""

import os
import sys

# Navigate to the Restaurant M&A directory
target_dir = "Restaurant M&A"

if os.path.exists(target_dir):
    os.chdir(target_dir)
    print(f"✓ Changed directory to: {os.getcwd()}")
elif os.path.exists(f"../{target_dir}"):
    os.chdir(f"../{target_dir}")
    print(f"✓ Changed directory to: {os.getcwd()}")
else:
    print(f"❌ Cannot find '{target_dir}' directory")
    print(f"Current directory: {os.getcwd()}")
    sys.exit(1)

# Check if Deal_Results.xlsx exists
if not os.path.exists("Deal_Results.xlsx"):
    print("❌ Deal_Results.xlsx not found in this directory")
    print(f"Files here: {os.listdir('.')}")
    sys.exit(1)

print("✓ Found Deal_Results.xlsx")
print("\nStarting analysis...\n")

# Now import and run the analysis
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import warnings
warnings.filterwarnings('ignore')

print("="*80)
print("RESTAURANT M&A ANALYSIS")
print("="*80)

# Load data
df = pd.read_excel('Deal_Results.xlsx', skiprows=3)
print(f"\n✓ Loaded {len(df)} transactions")

# Clean and prepare data
df['Deal Size (MM, USD)'] = pd.to_numeric(df['Deal Size (MM, USD)'], errors='coerce')
df['EV/EBITDA'] = pd.to_numeric(df['EV/EBITDA'], errors='coerce')
df['Year'] = df['Announcement Date'].dt.year
df['Quarter'] = df['Announcement Date'].dt.quarter
df['YearQuarter'] = df['Year'].astype(str) + ' Q' + df['Quarter'].astype(str)

def get_primary_buyer(buyer_string):
    if pd.isna(buyer_string):
        return 'Unknown'
    return buyer_string.split(',')[0].strip()

df['Primary Buyer'] = df['Buyer Name'].apply(get_primary_buyer)

deals_with_value = df[df['Deal Size (MM, USD)'].notna()].copy()
multiples_data = df[df['EV/EBITDA'].notna()].copy()

deal_size_bins = [0, 10, 50, 100, 500, 1000, 20000]
deal_size_labels = ['< $10M', '$10-50M', '$50-100M', '$100-500M', '$500M-$1B', '> $1B']
deals_with_value['Size Category'] = pd.cut(
    deals_with_value['Deal Size (MM, USD)'], 
    bins=deal_size_bins, 
    labels=deal_size_labels, 
    include_lowest=True
)

print(f"✓ {len(deals_with_value)} deals with disclosed value ({len(deals_with_value)/len(df)*100:.1f}%)")
print(f"✓ {len(multiples_data)} deals with EV/EBITDA ({len(multiples_data)/len(df)*100:.1f}%)")

print("\n" + "="*80)
print("GENERATING CHARTS")
print("="*80)

# CHART 1: Deal Trends
print("\n[1/8] Deal Volume & Value Trends...")
yearly_data = df[df['Year'] >= 2000].groupby('Year').agg({
    'Deal ID': 'count',
    'Deal Size (MM, USD)': 'sum'
}).reset_index()
yearly_data.columns = ['Year', 'Deal Count', 'Total Value']
yearly_data['MA_Count'] = yearly_data['Deal Count'].rolling(window=3, center=True).mean()

fig1 = make_subplots(specs=[[{"secondary_y": True}]])
fig1.add_trace(go.Bar(x=yearly_data['Year'], y=yearly_data['Deal Count'], 
                      name='Deal Count', marker_color='rgb(99, 110, 250)', opacity=0.7),
               secondary_y=False)
fig1.add_trace(go.Scatter(x=yearly_data['Year'], y=yearly_data['MA_Count'],
                          name='3Y MA', mode='lines', 
                          line=dict(color='rgb(99, 110, 250)', width=3, dash='dash')),
               secondary_y=False)
fig1.add_trace(go.Scatter(x=yearly_data['Year'], y=yearly_data['Total Value']/1000,
                          name='Value ($B)', mode='lines+markers',
                          line=dict(color='rgb(239, 85, 59)', width=3), marker=dict(size=8)),
               secondary_y=True)
fig1.update_xaxes(title_text="Year")
fig1.update_yaxes(title_text="Deal Count", secondary_y=False)
fig1.update_yaxes(title_text="Value ($B)", secondary_y=True)
fig1.update_layout(
    title="Restaurant M&A: Volume vs. Value (2000-2026)<br><sub>Mega-deals drive value spikes</sub>",
    hovermode='x unified', height=600
)
fig1.write_html("chart1_deal_trends.html")

# CHART 2: Top Acquirers
print("[2/8] Top Acquirers...")
top_vol = df['Primary Buyer'].value_counts().head(20).reset_index()
top_vol.columns = ['Acquirer', 'Deal Count']
top_val = df[df['Deal Size (MM, USD)'].notna()].groupby('Primary Buyer').agg({
    'Deal Size (MM, USD)': 'sum'
}).round(2).sort_values('Deal Size (MM, USD)', ascending=False).head(20).reset_index()
top_val.columns = ['Acquirer', 'Total Value ($M)']

fig2 = make_subplots(rows=1, cols=2, subplot_titles=("By Volume", "By Value ($B)"), 
                     horizontal_spacing=0.15)
fig2.add_trace(go.Bar(x=top_vol['Deal Count'], y=top_vol['Acquirer'], orientation='h',
                      text=top_vol['Deal Count'], textposition='outside',
                      marker_color='rgb(99, 110, 250)', showlegend=False), row=1, col=1)
fig2.add_trace(go.Bar(x=top_val['Total Value ($M)']/1000, y=top_val['Acquirer'], orientation='h',
                      text=[f"${v/1000:.1f}B" for v in top_val['Total Value ($M)']],
                      textposition='outside', marker_color='rgb(239, 85, 59)', showlegend=False),
               row=1, col=2)
fig2.update_xaxes(title_text="Deal Count", row=1, col=1)
fig2.update_xaxes(title_text="Value ($B)", row=1, col=2)
fig2.update_layout(title="Top 20 Acquirers<br><sub>Franchisees vs. PE buyers</sub>", height=700)
fig2.write_html("chart2_top_acquirers.html")

# CHART 3: Deal Sizes
print("[3/8] Deal Size Distribution...")
size_bins_data = deals_with_value['Size Category'].value_counts().reindex(deal_size_labels)
fig3 = go.Figure()
fig3.add_trace(go.Bar(x=size_bins_data.index, y=size_bins_data.values,
                      text=[f"{v}<br>({v/len(deals_with_value)*100:.1f}%)" for v in size_bins_data.values],
                      textposition='outside', marker_color='rgb(99, 110, 250)'))
median_val = deals_with_value['Deal Size (MM, USD)'].median()
mean_val = deals_with_value['Deal Size (MM, USD)'].mean()
fig3.add_annotation(text=f"Median: ${median_val:.1f}M<br>Mean: ${mean_val:.1f}M",
                    xref="paper", yref="paper", x=0.98, y=0.98, showarrow=False,
                    bgcolor="white", bordercolor="black", borderwidth=1, font=dict(size=14))
fig3.update_layout(title="Deal Size Distribution<br><sub>64% under $50M</sub>",
                   xaxis_title="Deal Size Range", yaxis_title="Deal Count", height=500)
fig3.write_html("chart3_deal_sizes.html")

# CHART 4: Multiples
print("[4/8] Valuation Multiples...")
clean_multiples = multiples_data[(multiples_data['EV/EBITDA'] > 0) & 
                                  (multiples_data['EV/EBITDA'] < 30)].copy()
fig4 = make_subplots(rows=1, cols=2, subplot_titles=("Distribution", "Trend 2015-2025"), 
                     horizontal_spacing=0.12)
fig4.add_trace(go.Histogram(x=clean_multiples['EV/EBITDA'], nbinsx=25,
                            marker_color='rgb(99, 110, 250)', name='Deals'), row=1, col=1)
multiples_by_year = clean_multiples[clean_multiples['Year'] >= 2015].groupby('Year').agg({
    'EV/EBITDA': ['median', 'mean']
}).reset_index()
multiples_by_year.columns = ['Year', 'Median', 'Mean']
fig4.add_trace(go.Scatter(x=multiples_by_year['Year'], y=multiples_by_year['Median'],
                          name='Median', mode='lines+markers', line=dict(width=3), 
                          marker=dict(size=10)), row=1, col=2)
fig4.add_trace(go.Scatter(x=multiples_by_year['Year'], y=multiples_by_year['Mean'],
                          name='Mean', mode='lines+markers', line=dict(width=3, dash='dash'),
                          marker=dict(size=10)), row=1, col=2)
median_mult = clean_multiples['EV/EBITDA'].median()
fig4.add_vline(x=median_mult, line_dash="dash", line_color="red", 
               annotation_text=f"Median: {median_mult:.1f}x", row=1, col=1)
fig4.update_xaxes(title_text="EV/EBITDA (x)", row=1, col=1)
fig4.update_xaxes(title_text="Year", row=1, col=2)
fig4.update_yaxes(title_text="Deal Count", row=1, col=1)
fig4.update_yaxes(title_text="Multiple (x)", row=1, col=2)
fig4.update_layout(title=f"EV/EBITDA Analysis<br><sub>Median 8.1x</sub>", height=500)
fig4.write_html("chart4_multiples.html")

# CHART 5: Quarterly
print("[5/8] Quarterly Trends...")
recent = df[df['Year'] >= 2020].copy()
quarterly_data = recent.groupby('YearQuarter').agg({
    'Deal ID': 'count', 'Deal Size (MM, USD)': 'sum'
}).reset_index()
quarterly_data.columns = ['Quarter', 'Deal Count', 'Total Value']
quarters_sorted = sorted(quarterly_data['Quarter'].unique(), 
                        key=lambda x: (int(x.split()[0]), int(x.split()[1][1])))
quarterly_data['Quarter'] = pd.Categorical(quarterly_data['Quarter'], 
                                           categories=quarters_sorted, ordered=True)
quarterly_data = quarterly_data.sort_values('Quarter')
fig5 = make_subplots(specs=[[{"secondary_y": True}]])
fig5.add_trace(go.Bar(x=quarterly_data['Quarter'], y=quarterly_data['Deal Count'],
                      name='Deal Count', marker_color='rgb(99, 110, 250)'), secondary_y=False)
fig5.add_trace(go.Scatter(x=quarterly_data['Quarter'], y=quarterly_data['Total Value']/1000,
                          name='Value ($B)', mode='lines+markers',
                          line=dict(color='rgb(239, 85, 59)', width=3), marker=dict(size=8)),
               secondary_y=True)
fig5.update_xaxes(title_text="Quarter")
fig5.update_yaxes(title_text="Deal Count", secondary_y=False)
fig5.update_yaxes(title_text="Value ($B)", secondary_y=True)
fig5.update_layout(title="Quarterly Activity (2020-2026)<br><sub>Q4 2024 spike</sub>",
                   hovermode='x unified', height=500)
fig5.write_html("chart5_quarterly_trends.html")

# CHART 6: Mix
print("[6/8] Transaction Mix...")
fig6 = make_subplots(rows=1, cols=2, specs=[[{"type": "pie"}, {"type": "pie"}]],
                     subplot_titles=("Transaction Type", "Target Subsector"))
txn_type = df['Transaction Type'].value_counts()
fig6.add_trace(go.Pie(labels=txn_type.index, values=txn_type.values,
                      textposition='inside', textinfo='label+percent'), row=1, col=1)
subsector = df['RBICS Industry/Sector (Target/Issuer)'].value_counts()
fig6.add_trace(go.Pie(labels=subsector.index, values=subsector.values,
                      textposition='inside', textinfo='label+percent'), row=1, col=2)
fig6.update_layout(title="Deal Structure & Targets<br><sub>Full acquisitions dominate</sub>", height=500)
fig6.write_html("chart6_deal_mix.html")

# CHART 7: Heatmap
print("[7/8] Activity Heatmap...")
heatmap_data = df[df['Year'] >= 2015].groupby(['Year', 'Quarter']).size().reset_index(name='Count')
heatmap_pivot = heatmap_data.pivot(index='Year', columns='Quarter', values='Count').fillna(0)
fig7 = go.Figure(data=go.Heatmap(z=heatmap_pivot.values, x=[f'Q{i}' for i in heatmap_pivot.columns],
                                  y=heatmap_pivot.index, colorscale='Blues',
                                  text=heatmap_pivot.values.astype(int), texttemplate='%{text}',
                                  textfont={"size": 12}, colorbar=dict(title="Deals")))
fig7.update_layout(title="Activity Heatmap (2015-2026)<br><sub>Seasonal patterns</sub>",
                   xaxis_title="Quarter", yaxis_title="Year", height=500)
fig7.write_html("chart7_heatmap.html")

# CHART 8: Mega Deals
print("[8/8] Mega Deals Timeline...")
mega_deals = df.nlargest(10, 'Deal Size (MM, USD)')[
    ['Target/Issuer Name', 'Primary Buyer', 'Deal Size (MM, USD)', 'Announcement Date']
].copy()
mega_deals['Label'] = mega_deals.apply(
    lambda x: f"{x['Target/Issuer Name'][:30]}<br>${x['Deal Size (MM, USD)']/1000:.1f}B", axis=1
)
fig8 = go.Figure()
fig8.add_trace(go.Scatter(x=mega_deals['Announcement Date'], y=mega_deals['Deal Size (MM, USD)']/1000,
                          mode='markers+text', marker=dict(size=20, 
                          color=mega_deals['Deal Size (MM, USD)'],
                          colorscale='Viridis', showscale=True, colorbar=dict(title="Value ($B)")),
                          text=mega_deals['Label'], textposition='top center',
                          hovertemplate='<b>%{text}</b><br>Date: %{x}<br>$%{y:.2f}B<extra></extra>'))
fig8.update_layout(title="Top 10 Largest Deals<br><sub>Roark Capital dominates</sub>",
                   xaxis_title="Date", yaxis_title="Value ($B)", height=600, showlegend=False)
fig8.write_html("chart8_mega_deals.html")

# Summary tables
print("\n" + "="*80)
print("CREATING SUMMARY TABLES")
print("="*80)

summary_stats = pd.DataFrame({
    'Metric': ['Total Deals', 'Disclosed Deals', 'Total Value', 'Median Size', 'Mean Size',
               'Deals w/ Multiple', 'Median Multiple', 'Mean Multiple', 'Recent (2023-26)', 'Recent Value'],
    'Value': [f"{len(df):,}", f"{len(deals_with_value):,}", 
              f"${deals_with_value['Deal Size (MM, USD)'].sum()/1000:.1f}B",
              f"${deals_with_value['Deal Size (MM, USD)'].median():.1f}M",
              f"${deals_with_value['Deal Size (MM, USD)'].mean():.1f}M",
              f"{len(multiples_data):,}", f"{multiples_data['EV/EBITDA'].median():.2f}x",
              f"{multiples_data['EV/EBITDA'].mean():.2f}x", f"{len(df[df['Year'] >= 2023]):,}",
              f"${df[df['Year'] >= 2023]['Deal Size (MM, USD)'].sum()/1000:.1f}B"]
})
summary_stats.to_csv('summary_statistics.csv', index=False)
print("✓ summary_statistics.csv")

top_vol_export = df['Primary Buyer'].value_counts().head(30).reset_index()
top_vol_export.columns = ['Acquirer', 'Deal Count']
top_vol_export.to_csv('top_acquirers_volume.csv', index=False)
print("✓ top_acquirers_volume.csv")

top_val_export = df[df['Deal Size (MM, USD)'].notna()].groupby('Primary Buyer').agg({
    'Deal Size (MM, USD)': 'sum'
}).round(2).sort_values('Deal Size (MM, USD)', ascending=False).head(30).reset_index()
top_val_export.columns = ['Acquirer', 'Total Value ($M)']
top_val_export.to_csv('top_acquirers_value.csv', index=False)
print("✓ top_acquirers_value.csv")

print("\n" + "="*80)
print("✅ ANALYSIS COMPLETE!")
print("="*80)
print(f"""
📊 Generated Files:
   • 8 interactive HTML charts (chart1-8)
   • 3 summary CSV tables

📈 Quick Stats:
   • {len(df):,} total deals
   • ${deals_with_value['Deal Size (MM, USD)'].sum()/1000:.1f}B disclosed value
   • {multiples_data['EV/EBITDA'].median():.2f}x median multiple

💡 Next Steps:
   • Open .html files in browser for interactive charts
   • Use CSV files for your models/presentations

Files saved in: {os.getcwd()}
""")
