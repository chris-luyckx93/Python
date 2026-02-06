import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

# Set style for better-looking charts
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")

# Load data
print("Loading Technomic Top 500 data...")
df = pd.read_excel('Technomic-Data.xlsx', sheet_name='Technomic Top 500 Chains')
print(f"Loaded {len(df)} chains with {len(df.columns)} columns\n")

# ============================================================================
# 1. YEAR-OVER-YEAR GROWTH BY MENU TYPE
# ============================================================================
print("="*80)
print("1. ANALYZING GROWTH BY MENU TYPE (Year-over-Year)")
print("="*80)

# Calculate YoY growth for each year
years = [2024, 2023, 2022, 2021, 2020]

menu_growth_data = []
for year in years:
    if year == 2019:
        continue

    current_year_col = f'{year} U.S. Sales ($000)'
    prev_year = year - 1
    prev_year_col = f'{prev_year} U.S. Sales ($000)'

    # Group by Menu Type
    menu_summary = df.groupby('Menu Type').agg({
        current_year_col: 'sum',
        prev_year_col: 'sum',
        f'{year} U.S. Units': 'sum',
        f'{prev_year} U.S. Units': 'sum'
    }).reset_index()

    menu_summary[f'Sales_Growth_{year}'] = ((menu_summary[current_year_col] / menu_summary[prev_year_col]) - 1) * 100
    menu_summary[f'Unit_Growth_{year}'] = ((menu_summary[f'{year} U.S. Units'] / menu_summary[f'{prev_year} U.S. Units']) - 1) * 100
    menu_summary['Year'] = f'{prev_year}-{year}'

    menu_growth_data.append(menu_summary[['Menu Type', 'Year', f'Sales_Growth_{year}', f'Unit_Growth_{year}', current_year_col]])

# Display top menu types by 2024 sales with growth metrics
print("\nTop 15 Menu Types - Sales Growth Analysis:")
top_menus_2024 = df.groupby('Menu Type')['2024 U.S. Sales ($000)'].sum().nlargest(15).reset_index()
for idx, row in top_menus_2024.iterrows():
    menu = row['Menu Type']
    sales_2024 = row['2024 U.S. Sales ($000)']
    sales_2023 = df[df['Menu Type'] == menu]['2023 U.S. Sales ($000)'].sum()
    sales_2019 = df[df['Menu Type'] == menu]['2019 U.S. Sales ($000)'].sum()

    yoy_growth = ((sales_2024 / sales_2023) - 1) * 100 if sales_2023 > 0 else 0
    cagr_5y = ((sales_2024 / sales_2019) ** (1/5) - 1) * 100 if sales_2019 > 0 else 0

    print(f"  {idx+1}. {menu:25s} | 2024 Sales: ${sales_2024/1e6:>7,.1f}M | YoY: {yoy_growth:>6.2f}% | 5Y CAGR: {cagr_5y:>6.2f}%")

# ============================================================================
# 2. SUBSEGMENT ANALYSIS
# ============================================================================
print("\n" + "="*80)
print("2. ANALYZING GROWTH BY SUBSEGMENT")
print("="*80)

subsegment_analysis = df.groupby('Subsegment').agg({
    '2024 U.S. Sales ($000)': 'sum',
    '2023 U.S. Sales ($000)': 'sum',
    '2019 U.S. Sales ($000)': 'sum',
    '2024 U.S. Units': 'sum',
    '2023 U.S. Units': 'sum',
    '2019 U.S. Units': 'sum',
    'Chain Name': 'count'
}).reset_index()

subsegment_analysis.columns = ['Subsegment', 'Sales_2024', 'Sales_2023', 'Sales_2019', 
                                 'Units_2024', 'Units_2023', 'Units_2019', 'Chain_Count']

subsegment_analysis['Sales_YoY_Growth_%'] = ((subsegment_analysis['Sales_2024'] / subsegment_analysis['Sales_2023']) - 1) * 100
subsegment_analysis['Sales_5Y_CAGR_%'] = ((subsegment_analysis['Sales_2024'] / subsegment_analysis['Sales_2019']) ** (1/5) - 1) * 100
subsegment_analysis['Units_YoY_Growth_%'] = ((subsegment_analysis['Units_2024'] / subsegment_analysis['Units_2023']) - 1) * 100
subsegment_analysis['Units_5Y_CAGR_%'] = ((subsegment_analysis['Units_2024'] / subsegment_analysis['Units_2019']) ** (1/5) - 1) * 100

subsegment_analysis = subsegment_analysis.sort_values('Sales_2024', ascending=False)

print("\nSubsegment Performance Summary:")
print(subsegment_analysis.to_string(index=False))

# ============================================================================
# 3. FASTEST GROWING CHAINS
# ============================================================================
print("\n" + "="*80)
print("3. FASTEST GROWING CHAINS")
print("="*80)

# Filter chains with meaningful size (>$50M in 2024 sales)
growth_df = df[df['2024 U.S. Sales ($000)'] >= 50000].copy()

print("\nTop 20 Fastest Growing Chains (5-Year Sales CAGR, min $50M sales):")
top_growth = growth_df.nlargest(20, '5-Year Sales CAGR')[
    ['Rank', 'Chain Name', 'Menu Type', 'Subsegment', 
     '2024 U.S. Sales ($000)', '2024 U.S. Units', '5-Year Sales CAGR', '5-Year Unit CAGR']
].copy()

top_growth['2024 Sales ($M)'] = (top_growth['2024 U.S. Sales ($000)'] / 1000).round(1)
top_growth['5Y Sales CAGR (%)'] = (top_growth['5-Year Sales CAGR'] * 100).round(1)
top_growth['5Y Unit CAGR (%)'] = (top_growth['5-Year Unit CAGR'] * 100).round(1)

print(top_growth[['Rank', 'Chain Name', 'Menu Type', '2024 Sales ($M)', 
                   '2024 U.S. Units', '5Y Sales CAGR (%)', '5Y Unit CAGR (%)']].to_string(index=False))

# ============================================================================
# 4. SEGMENT-LEVEL TRENDS (LSR vs FSR)
# ============================================================================
print("\n" + "="*80)
print("4. SEGMENT TRENDS: LSR vs FSR")
print("="*80)

segment_trends = []
for year in [2019, 2020, 2021, 2022, 2023, 2024]:
    seg_data = df.groupby('Segment').agg({
        f'{year} U.S. Sales ($000)': 'sum',
        f'{year} U.S. Units': 'sum'
    }).reset_index()
    seg_data['Year'] = year
    seg_data.columns = ['Segment', 'Sales', 'Units', 'Year']
    segment_trends.append(seg_data)

segment_trends_df = pd.concat(segment_trends, ignore_index=True)
segment_trends_df['AUV'] = segment_trends_df['Sales'] / segment_trends_df['Units']

print("\nSegment Performance by Year:")
segment_pivot = segment_trends_df.pivot(index='Year', columns='Segment', values='Sales')
segment_pivot['Total'] = segment_pivot.sum(axis=1)
segment_pivot['LSR_Share_%'] = (segment_pivot['LSR'] / segment_pivot['Total'] * 100).round(1)
segment_pivot['FSR_Share_%'] = (segment_pivot['FSR'] / segment_pivot['Total'] * 100).round(1)
print(segment_pivot)

# ============================================================================
# 5. UNIT ECONOMICS ANALYSIS
# ============================================================================
print("\n" + "="*80)
print("5. UNIT ECONOMICS BY MENU TYPE")
print("="*80)

# Calculate average AUV by menu type for chains with units
unit_economics = df[df['2024 U.S. Units'] > 0].groupby('Menu Type').agg({
    '2024 AUV': 'mean',
    '2024 U.S. Units': 'sum',
    '2024 U.S. Sales ($000)': 'sum',
    'Chain Name': 'count'
}).reset_index()

unit_economics.columns = ['Menu Type', 'Avg_AUV', 'Total_Units', 'Total_Sales', 'Chain_Count']
unit_economics = unit_economics.sort_values('Avg_AUV', ascending=False).head(15)

print("\nTop 15 Menu Types by Average Unit Volume:")
print(unit_economics.to_string(index=False))

# ============================================================================
# 6. EXPORT RESULTS TO CSV
# ============================================================================
print("\n" + "="*80)
print("6. EXPORTING ANALYSIS TO CSV FILES")
print("="*80)

# Export key datasets
subsegment_analysis.to_csv('subsegment_analysis.csv', index=False)
print("✓ Exported: subsegment_analysis.csv")

top_growth.to_csv('fastest_growing_chains.csv', index=False)
print("✓ Exported: fastest_growing_chains.csv")

segment_trends_df.to_csv('segment_trends.csv', index=False)
print("✓ Exported: segment_trends.csv")

unit_economics.to_csv('unit_economics.csv', index=False)
print("✓ Exported: unit_economics.csv")

# Create comprehensive menu type analysis
menu_type_full = df.groupby('Menu Type').agg({
    '2024 U.S. Sales ($000)': 'sum',
    '2023 U.S. Sales ($000)': 'sum',
    '2019 U.S. Sales ($000)': 'sum',
    '2024 U.S. Units': 'sum',
    '2023 U.S. Units': 'sum',
    '2024 AUV': 'mean',
    '5-Year Sales CAGR': 'mean',
    'Chain Name': 'count'
}).reset_index()

menu_type_full.columns = ['Menu_Type', 'Sales_2024', 'Sales_2023', 'Sales_2019', 
                           'Units_2024', 'Units_2023', 'Avg_AUV', 'Avg_5Y_CAGR', 'Chain_Count']
menu_type_full['YoY_Sales_Growth_%'] = ((menu_type_full['Sales_2024'] / menu_type_full['Sales_2023']) - 1) * 100
menu_type_full['5Y_Sales_CAGR_%'] = ((menu_type_full['Sales_2024'] / menu_type_full['Sales_2019']) ** (1/5) - 1) * 100
menu_type_full = menu_type_full.sort_values('Sales_2024', ascending=False)

menu_type_full.to_csv('menu_type_analysis.csv', index=False)
print("✓ Exported: menu_type_analysis.csv")

print("\n" + "="*80)
print("ANALYSIS COMPLETE!")
print("="*80)
print("\nAll CSV files have been exported to your working directory.")
print("You can now use these files for further analysis or visualization.")
