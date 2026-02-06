import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

print("="*80)
print("TECHNOMIC ADVANCED ANALYTICS")
print("="*80)

# Load data
df = pd.read_excel('Technomic-Data.xlsx', sheet_name='Technomic Top 500 Chains')
print(f"\nLoaded {len(df)} chains\n")

# ============================================================================
# 1. YEAR-BY-YEAR GROWTH MATRIX
# ============================================================================
print("="*80)
print("1. YEAR-BY-YEAR GROWTH RATES")
print("="*80)

years = [2020, 2021, 2022, 2023, 2024]
growth_matrix = []

for year in years:
    prev_year = year - 1
    total_current = df[f'{year} U.S. Sales ($000)'].sum()
    total_prev = df[f'{prev_year} U.S. Sales ($000)'].sum()
    growth_pct = ((total_current / total_prev) - 1) * 100

    units_current = df[f'{year} U.S. Units'].sum()
    units_prev = df[f'{prev_year} U.S. Units'].sum()
    unit_growth_pct = ((units_current / units_prev) - 1) * 100

    growth_matrix.append({
        'Period': f'{prev_year}-{year}',
        'Total_Sales_$B': total_current / 1e6,
        'Sales_Growth_%': growth_pct,
        'Total_Units_K': units_current / 1000,
        'Unit_Growth_%': unit_growth_pct
    })

growth_df = pd.DataFrame(growth_matrix)
print("\nIndustry Growth by Year:")
print(growth_df.to_string(index=False))

# ============================================================================
# 2. CHAIN POSITIONING: GROWTH VS SIZE MATRIX
# ============================================================================
print("\n" + "="*80)
print("2. CHAIN POSITIONING MATRIX (Growth vs Size)")
print("="*80)

positioning = df[df['2024 U.S. Sales ($000)'] >= 100000].copy()
positioning['Size_Category'] = pd.cut(positioning['2024 U.S. Sales ($000)'], 
                                       bins=[0, 500000, 2000000, 10000000, 60000000],
                                       labels=['Small ($100-500M)', 'Medium ($500M-2B)', 
                                               'Large ($2-10B)', 'Mega (>$10B)'])

positioning['Growth_Category'] = pd.cut(positioning['5-Year Sales CAGR'],
                                         bins=[-2, -0.1, 0.05, 0.15, 2],
                                         labels=['Declining', 'Slow Growth', 
                                                 'Moderate Growth', 'High Growth'])

matrix_summary = positioning.groupby(['Size_Category', 'Growth_Category']).agg({
    'Chain Name': 'count',
    '2024 U.S. Sales ($000)': 'sum'
}).reset_index()

print("\nChain Count by Size and Growth Category:")
pivot_count = matrix_summary.pivot(index='Size_Category', columns='Growth_Category', values='Chain Name').fillna(0)
print(pivot_count)

print("\nKey Chains in Each Quadrant:")
for size in ['Mega (>$10B)', 'Large ($2-10B)']:
    for growth in ['High Growth', 'Declining']:
        chains = positioning[(positioning['Size_Category'] == size) & 
                            (positioning['Growth_Category'] == growth)][['Chain Name', '2024 U.S. Sales ($000)', '5-Year Sales CAGR']]
        if len(chains) > 0:
            print(f"\n{size} + {growth}:")
            print(chains.head(5).to_string(index=False))

# ============================================================================
# 3. MENU TYPE COMPETITIVE DYNAMICS
# ============================================================================
print("\n" + "="*80)
print("3. MENU TYPE COMPETITIVE DYNAMICS")
print("="*80)

menu_dynamics = df.groupby('Menu Type').agg({
    'Chain Name': 'count',
    '2024 U.S. Sales ($000)': ['sum', 'mean', 'std'],
    '2024 U.S. Units': 'sum',
    '5-Year Sales CAGR': ['mean', 'std'],
    '2024 YoY units %': 'mean'
}).reset_index()

menu_dynamics.columns = ['Menu_Type', 'Chain_Count', 'Total_Sales', 'Avg_Sales', 'StdDev_Sales',
                          'Total_Units', 'Avg_5Y_CAGR', 'StdDev_CAGR', 'Avg_Unit_Growth']

menu_dynamics = menu_dynamics.sort_values('Total_Sales', ascending=False).head(15)
menu_dynamics['HHI_Proxy'] = (menu_dynamics['StdDev_Sales'] / menu_dynamics['Avg_Sales']) * 100

print("\nTop 15 Menu Types - Competitive Structure:")
print(menu_dynamics[['Menu_Type', 'Chain_Count', 'Total_Sales', 'Avg_5Y_CAGR', 'HHI_Proxy']].to_string(index=False))
print("\n* HHI_Proxy = Coefficient of Variation (higher = more concentrated)")

# ============================================================================
# 4. RECOVERY ANALYSIS (2019-2024)
# ============================================================================
print("\n" + "="*80)
print("4. COVID RECOVERY ANALYSIS (2019 vs 2024)")
print("="*80)

recovery = df[df['2019 U.S. Sales ($000)'] > 0].copy()
recovery['Sales_Change_$M'] = (recovery['2024 U.S. Sales ($000)'] - recovery['2019 U.S. Sales ($000)']) / 1000
recovery['Sales_Change_%'] = ((recovery['2024 U.S. Sales ($000)'] / recovery['2019 U.S. Sales ($000)']) - 1) * 100
recovery['Units_Change'] = recovery['2024 U.S. Units'] - recovery['2019 U.S. Units']
recovery['Recovery_Status'] = recovery['Sales_Change_%'].apply(
    lambda x: 'Strong Recovery' if x >= 20 else ('Modest Recovery' if x >= 0 else 'Below 2019'))

status_summary = recovery.groupby(['Segment', 'Recovery_Status']).agg({
    'Chain Name': 'count',
    'Sales_Change_$M': 'sum'
}).reset_index()

print("\nRecovery Status by Segment:")
print(status_summary.to_string(index=False))

print("\nTop Recoveries (Largest $ Gains):")
top_recoveries = recovery.nlargest(15, 'Sales_Change_$M')[
    ['Chain Name', 'Menu Type', '2019 U.S. Sales ($000)', '2024 U.S. Sales ($000)', 
     'Sales_Change_$M', 'Sales_Change_%']
]
print(top_recoveries.to_string(index=False))

print("\nBiggest Declines (Largest $ Losses):")
biggest_declines = recovery.nsmallest(10, 'Sales_Change_$M')[
    ['Chain Name', 'Menu Type', '2019 U.S. Sales ($000)', '2024 U.S. Sales ($000)', 
     'Sales_Change_$M', 'Sales_Change_%']
]
print(biggest_declines.to_string(index=False))

# ============================================================================
# 5. SUBSEGMENT DEEP DIVE
# ============================================================================
print("\n" + "="*80)
print("5. SUBSEGMENT DEEP DIVE")
print("="*80)

for subseg in ['QSR', 'FC', 'CDR']:
    print(f"\n{'='*80}")
    print(f"{subseg} ANALYSIS")
    print(f"{'='*80}")

    subseg_data = df[df['Subsegment'] == subseg].copy()

    # Top chains
    print(f"\nTop 10 {subseg} Chains by 2024 Sales:")
    top_chains = subseg_data.nlargest(10, '2024 U.S. Sales ($000)')[
        ['Rank', 'Chain Name', 'Menu Type', '2024 U.S. Sales ($000)', 
         '2024 U.S. Units', '5-Year Sales CAGR']
    ]
    print(top_chains.to_string(index=False))

    # Growth leaders
    print(f"\nFastest Growing {subseg} Chains (min $100M):")
    growth_leaders = subseg_data[subseg_data['2024 U.S. Sales ($000)'] >= 100000].nlargest(5, '5-Year Sales CAGR')[
        ['Chain Name', 'Menu Type', '2024 U.S. Sales ($000)', '5-Year Sales CAGR', '5-Year Unit CAGR']
    ]
    print(growth_leaders.to_string(index=False))

    # Summary stats
    print(f"\n{subseg} Summary Statistics:")
    print(f"  Total Chains: {len(subseg_data)}")
    print(f"  Total 2024 Sales: ${subseg_data['2024 U.S. Sales ($000)'].sum()/1e6:.1f}B")
    print(f"  Total Units: {subseg_data['2024 U.S. Units'].sum():,}")
    print(f"  Avg 5Y Sales CAGR: {subseg_data['5-Year Sales CAGR'].mean()*100:.1f}%")
    print(f"  Avg YoY Unit Growth: {subseg_data['2024 YoY units %'].mean()*100:.1f}%")

# ============================================================================
# 6. EXPORT ADVANCED ANALYTICS
# ============================================================================
print("\n" + "="*80)
print("6. EXPORTING ADVANCED ANALYTICS")
print("="*80)

# Export positioning matrix
positioning[['Chain Name', 'Menu Type', 'Subsegment', '2024 U.S. Sales ($000)', 
             '5-Year Sales CAGR', 'Size_Category', 'Growth_Category']].to_csv(
    'chain_positioning_matrix.csv', index=False)
print("✓ Exported: chain_positioning_matrix.csv")

# Export recovery analysis
recovery[['Chain Name', 'Segment', 'Menu Type', '2019 U.S. Sales ($000)', 
          '2024 U.S. Sales ($000)', 'Sales_Change_$M', 'Sales_Change_%', 
          'Recovery_Status']].to_csv('covid_recovery_analysis.csv', index=False)
print("✓ Exported: covid_recovery_analysis.csv")

# Export competitive dynamics
menu_dynamics.to_csv('menu_type_competitive_dynamics.csv', index=False)
print("✓ Exported: menu_type_competitive_dynamics.csv")

print("\n" + "="*80)
print("ADVANCED ANALYSIS COMPLETE!")
print("="*80)
