import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# Set style
plt.style.use('seaborn-v0_8-whitegrid')
sns.set_palette("Set2")

# Load data
print("Loading data...")
df = pd.read_excel('Technomic-Data.xlsx', sheet_name='Technomic Top 500 Chains')

# Create output directory for charts
import os
if not os.path.exists('charts'):
    os.makedirs('charts')
    print("Created 'charts' directory")

# ============================================================================
# CHART 1: Top 15 Menu Types by Sales with Growth Rates
# ============================================================================
print("\nGenerating Chart 1: Top Menu Types...")

menu_sales = df.groupby('Menu Type').agg({
    '2024 U.S. Sales ($000)': 'sum',
    '2023 U.S. Sales ($000)': 'sum',
    '2019 U.S. Sales ($000)': 'sum'
}).reset_index()

menu_sales['YoY_Growth'] = ((menu_sales['2024 U.S. Sales ($000)'] / menu_sales['2023 U.S. Sales ($000)']) - 1) * 100
menu_sales['5Y_CAGR'] = ((menu_sales['2024 U.S. Sales ($000)'] / menu_sales['2019 U.S. Sales ($000)']) ** (1/5) - 1) * 100
menu_sales = menu_sales.nlargest(15, '2024 U.S. Sales ($000)')

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 8))

# Sales bar chart
ax1.barh(menu_sales['Menu Type'], menu_sales['2024 U.S. Sales ($000)'] / 1e6, color='steelblue')
ax1.set_xlabel('2024 Sales ($B)', fontsize=12, fontweight='bold')
ax1.set_ylabel('Menu Type', fontsize=12, fontweight='bold')
ax1.set_title('Top 15 Menu Types by 2024 Sales', fontsize=14, fontweight='bold')
ax1.invert_yaxis()
for i, v in enumerate(menu_sales['2024 U.S. Sales ($000)'] / 1e6):
    ax1.text(v + 1, i, f'${v/1000:.1f}B', va='center', fontsize=9)

# Growth rate comparison
x_pos = np.arange(len(menu_sales))
width = 0.35
ax2.barh(x_pos - width/2, menu_sales['YoY_Growth'], width, label='YoY Growth %', color='coral')
ax2.barh(x_pos + width/2, menu_sales['5Y_CAGR'], width, label='5Y CAGR %', color='seagreen')
ax2.set_yticks(x_pos)
ax2.set_yticklabels(menu_sales['Menu Type'])
ax2.set_xlabel('Growth Rate (%)', fontsize=12, fontweight='bold')
ax2.set_title('Growth Rates: YoY vs 5-Year CAGR', fontsize=14, fontweight='bold')
ax2.legend(loc='best')
ax2.invert_yaxis()
ax2.axvline(x=0, color='black', linestyle='-', linewidth=0.5)

plt.tight_layout()
plt.savefig('charts/01_menu_type_analysis.png', dpi=300, bbox_inches='tight')
print("✓ Saved: charts/01_menu_type_analysis.png")
plt.close()

# ============================================================================
# CHART 2: Subsegment Performance
# ============================================================================
print("Generating Chart 2: Subsegment Performance...")

subseg = df.groupby('Subsegment').agg({
    '2024 U.S. Sales ($000)': 'sum',
    '2023 U.S. Sales ($000)': 'sum',
    '2024 U.S. Units': 'sum',
    '2023 U.S. Units': 'sum'
}).reset_index()

subseg['Sales_YoY'] = ((subseg['2024 U.S. Sales ($000)'] / subseg['2023 U.S. Sales ($000)']) - 1) * 100
subseg['Units_YoY'] = ((subseg['2024 U.S. Units'] / subseg['2023 U.S. Units']) - 1) * 100

fig, axes = plt.subplots(2, 2, figsize=(16, 12))

# Sales by subsegment
axes[0, 0].bar(subseg['Subsegment'], subseg['2024 U.S. Sales ($000)'] / 1e6, color='teal')
axes[0, 0].set_title('2024 Sales by Subsegment', fontsize=14, fontweight='bold')
axes[0, 0].set_ylabel('Sales ($B)', fontsize=11, fontweight='bold')
axes[0, 0].tick_params(axis='x', rotation=45)
for i, v in enumerate(subseg['2024 U.S. Sales ($000)'] / 1e6):
    axes[0, 0].text(i, v + 5, f'${v/1000:.1f}B', ha='center', fontsize=9)

# Units by subsegment
axes[0, 1].bar(subseg['Subsegment'], subseg['2024 U.S. Units'] / 1000, color='orange')
axes[0, 1].set_title('2024 Units by Subsegment', fontsize=14, fontweight='bold')
axes[0, 1].set_ylabel('Units (thousands)', fontsize=11, fontweight='bold')
axes[0, 1].tick_params(axis='x', rotation=45)
for i, v in enumerate(subseg['2024 U.S. Units'] / 1000):
    axes[0, 1].text(i, v + 2, f'{v:.0f}K', ha='center', fontsize=9)

# Sales YoY growth
colors_sales = ['green' if x >= 0 else 'red' for x in subseg['Sales_YoY']]
axes[1, 0].bar(subseg['Subsegment'], subseg['Sales_YoY'], color=colors_sales)
axes[1, 0].set_title('Sales YoY Growth % by Subsegment', fontsize=14, fontweight='bold')
axes[1, 0].set_ylabel('YoY Growth (%)', fontsize=11, fontweight='bold')
axes[1, 0].axhline(y=0, color='black', linestyle='-', linewidth=0.8)
axes[1, 0].tick_params(axis='x', rotation=45)
for i, v in enumerate(subseg['Sales_YoY']):
    axes[1, 0].text(i, v + 0.3 if v >= 0 else v - 0.5, f'{v:.1f}%', ha='center', fontsize=9)

# Units YoY growth
colors_units = ['green' if x >= 0 else 'red' for x in subseg['Units_YoY']]
axes[1, 1].bar(subseg['Subsegment'], subseg['Units_YoY'], color=colors_units)
axes[1, 1].set_title('Unit YoY Growth % by Subsegment', fontsize=14, fontweight='bold')
axes[1, 1].set_ylabel('YoY Growth (%)', fontsize=11, fontweight='bold')
axes[1, 1].axhline(y=0, color='black', linestyle='-', linewidth=0.8)
axes[1, 1].tick_params(axis='x', rotation=45)
for i, v in enumerate(subseg['Units_YoY']):
    axes[1, 1].text(i, v + 0.3 if v >= 0 else v - 0.5, f'{v:.1f}%', ha='center', fontsize=9)

plt.tight_layout()
plt.savefig('charts/02_subsegment_analysis.png', dpi=300, bbox_inches='tight')
print("✓ Saved: charts/02_subsegment_analysis.png")
plt.close()

# ============================================================================
# CHART 3: Segment Trends Over Time (LSR vs FSR)
# ============================================================================
print("Generating Chart 3: Segment Trends...")

years = [2019, 2020, 2021, 2022, 2023, 2024]
lsr_sales = []
fsr_sales = []
lsr_units = []
fsr_units = []

for year in years:
    lsr_s = df[df['Segment'] == 'LSR'][f'{year} U.S. Sales ($000)'].sum()
    fsr_s = df[df['Segment'] == 'FSR'][f'{year} U.S. Sales ($000)'].sum()
    lsr_u = df[df['Segment'] == 'LSR'][f'{year} U.S. Units'].sum()
    fsr_u = df[df['Segment'] == 'FSR'][f'{year} U.S. Units'].sum()

    lsr_sales.append(lsr_s / 1e6)
    fsr_sales.append(fsr_s / 1e6)
    lsr_units.append(lsr_u / 1000)
    fsr_units.append(fsr_u / 1000)

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

# Sales trend
ax1.plot(years, lsr_sales, marker='o', linewidth=2.5, markersize=8, label='LSR', color='#2E86AB')
ax1.plot(years, fsr_sales, marker='s', linewidth=2.5, markersize=8, label='FSR', color=''#A23B72')
ax1.set_xlabel('Year', fontsize=12, fontweight='bold')
ax1.set_ylabel('Sales ($B)', fontsize=12, fontweight='bold')
ax1.set_title('Sales Trend: LSR vs FSR (2019-2024)', fontsize=14, fontweight='bold')
ax1.legend(fontsize=11)
ax1.grid(True, alpha=0.3)
for i, year in enumerate(years):
    ax1.text(year, lsr_sales[i] + 5, f'${lsr_sales[i]/1000:.1f}B', ha='center', fontsize=8)
    ax1.text(year, fsr_sales[i] - 5, f'${fsr_sales[i]/1000:.1f}B', ha='center', fontsize=8)

# Units trend
ax2.plot(years, lsr_units, marker='o', linewidth=2.5, markersize=8, label='LSR', color='#2E86AB')
ax2.plot(years, fsr_units, marker='s', linewidth=2.5, markersize=8, label='FSR', color='#A23B72')
ax2.set_xlabel('Year', fontsize=12, fontweight='bold')
ax2.set_ylabel('Units (thousands)', fontsize=12, fontweight='bold')
ax2.set_title('Unit Count Trend: LSR vs FSR (2019-2024)', fontsize=14, fontweight='bold')
ax2.legend(fontsize=11)
ax2.grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig('charts/03_segment_trends.png', dpi=300, bbox_inches='tight')
print("✓ Saved: charts/03_segment_trends.png")
plt.close()

# ============================================================================
# CHART 4: Fastest Growing Chains
# ============================================================================
print("Generating Chart 4: Fastest Growing Chains...")

growth_chains = df[df['2024 U.S. Sales ($000)'] >= 50000].nlargest(20, '5-Year Sales CAGR')

fig, ax = plt.subplots(figsize=(14, 10))

y_pos = np.arange(len(growth_chains))
colors = plt.cm.RdYlGn(growth_chains['5-Year Sales CAGR'] / growth_chains['5-Year Sales CAGR'].max())

ax.barh(y_pos, growth_chains['5-Year Sales CAGR'] * 100, color=colors)
ax.set_yticks(y_pos)
ax.set_yticklabels(growth_chains['Chain Name'], fontsize=10)
ax.set_xlabel('5-Year Sales CAGR (%)', fontsize=12, fontweight='bold')
ax.set_title('Top 20 Fastest Growing Chains (5Y CAGR, min $50M sales)', fontsize=14, fontweight='bold')
ax.invert_yaxis()

for i, (idx, row) in enumerate(growth_chains.iterrows()):
    cagr = row['5-Year Sales CAGR'] * 100
    sales = row['2024 U.S. Sales ($000)'] / 1000
    ax.text(cagr + 2, i, f'{cagr:.1f}% | ${sales:.0f}M', va='center', fontsize=8)

plt.tight_layout()
plt.savefig('charts/04_fastest_growing_chains.png', dpi=300, bbox_inches='tight')
print("✓ Saved: charts/04_fastest_growing_chains.png")
plt.close()

# ============================================================================
# CHART 5: Unit Economics Comparison
# ============================================================================
print("Generating Chart 5: Unit Economics...")

unit_econ = df[df['2024 U.S. Units'] > 0].groupby('Menu Type').agg({
    '2024 AUV': 'mean',
    '2024 U.S. Units': 'sum',
    '2024 U.S. Sales ($000)': 'sum'
}).reset_index()

unit_econ = unit_econ.nlargest(15, '2024 AUV')

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 8))

# Average AUV
ax1.barh(unit_econ['Menu Type'], unit_econ['2024 AUV'] / 1000, color='mediumseagreen')
ax1.set_xlabel('Average AUV ($M)', fontsize=12, fontweight='bold')
ax1.set_ylabel('Menu Type', fontsize=12, fontweight='bold')
ax1.set_title('Top 15 Menu Types by Average Unit Volume', fontsize=14, fontweight='bold')
ax1.invert_yaxis()
for i, v in enumerate(unit_econ['2024 AUV'] / 1000):
    ax1.text(v + 0.1, i, f'${v:.2f}M', va='center', fontsize=9)

# Total units vs total sales scatter
scatter_data = df[df['2024 U.S. Units'] > 0].groupby('Menu Type').agg({
    '2024 U.S. Units': 'sum',
    '2024 U.S. Sales ($000)': 'sum'
}).reset_index()

ax2.scatter(scatter_data['2024 U.S. Units'], scatter_data['2024 U.S. Sales ($000)'] / 1e6, 
            s=100, alpha=0.6, c='purple')
ax2.set_xlabel('Total Units', fontsize=12, fontweight='bold')
ax2.set_ylabel('Total Sales ($B)', fontsize=12, fontweight='bold')
ax2.set_title('Menu Type: Unit Count vs Sales', fontsize=14, fontweight='bold')
ax2.grid(True, alpha=0.3)

# Annotate top menu types
top_to_label = scatter_data.nlargest(8, '2024 U.S. Sales ($000)')
for _, row in top_to_label.iterrows():
    ax2.annotate(row['Menu Type'], 
                 (row['2024 U.S. Units'], row['2024 U.S. Sales ($000)'] / 1e6),
                 fontsize=8, alpha=0.7, xytext=(5, 5), textcoords='offset points')

plt.tight_layout()
plt.savefig('charts/05_unit_economics.png', dpi=300, bbox_inches='tight')
print("✓ Saved: charts/05_unit_economics.png")
plt.close()

# ============================================================================
# CHART 6: Market Concentration
# ============================================================================
print("Generating Chart 6: Market Concentration...")

df_sorted = df.sort_values('2024 U.S. Sales ($000)', ascending=False).reset_index(drop=True)
df_sorted['Cumulative_Sales'] = df_sorted['2024 U.S. Sales ($000)'].cumsum()
df_sorted['Cumulative_Pct'] = (df_sorted['Cumulative_Sales'] / df_sorted['2024 U.S. Sales ($000)'].sum()) * 100
df_sorted['Chain_Rank'] = df_sorted.index + 1

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

# Cumulative sales curve
ax1.plot(df_sorted['Chain_Rank'][:100], df_sorted['Cumulative_Pct'][:100], 
         linewidth=2.5, color='darkblue')
ax1.axhline(y=50, color='red', linestyle='--', linewidth=1.5, label='50% of sales')
ax1.axhline(y=80, color='orange', linestyle='--', linewidth=1.5, label='80% of sales')
ax1.set_xlabel('Number of Chains (Ranked by Sales)', fontsize=12, fontweight='bold')
ax1.set_ylabel('Cumulative Market Share (%)', fontsize=12, fontweight='bold')
ax1.set_title('Market Concentration: Cumulative Sales Share', fontsize=14, fontweight='bold')
ax1.legend(fontsize=10)
ax1.grid(True, alpha=0.3)

# Top 20 market share
top_20 = df.nlargest(20, '2024 U.S. Sales ($000)')
ax2.bar(range(1, 21), top_20['2024 Market share (%)'], color='steelblue')
ax2.set_xlabel('Chain Rank', fontsize=12, fontweight='bold')
ax2.set_ylabel('Market Share (%)', fontsize=12, fontweight='bold')
ax2.set_title('Top 20 Chains by Market Share', fontsize=14, fontweight='bold')
ax2.set_xticks(range(1, 21))

plt.tight_layout()
plt.savefig('charts/06_market_concentration.png', dpi=300, bbox_inches='tight')
print("✓ Saved: charts/06_market_concentration.png")
plt.close()

print("\n" + "="*80)
print("ALL CHARTS GENERATED SUCCESSFULLY!")
print("="*80)
print(f"\nCheck the 'charts/' directory for all {6} visualization files.")