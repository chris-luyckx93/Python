import pandas as pd
import numpy as np

def calculate_hhi(group_df, sales_column='2024 U.S. Sales ($000)'):
    """
    Calculate Herfindahl-Hirschman Index (HHI) for a group.
    HHI = sum of squared market shares (in percentage points)

    Parameters:
    -----------
    group_df : DataFrame
        DataFrame containing chains in a specific category
    sales_column : str
        Column name containing sales data

    Returns:
    --------
    float : HHI value (0-10,000)
    """
    total_sales = group_df[sales_column].sum()
    if total_sales == 0:
        return 0

    group_df = group_df.copy()
    group_df['market_share_pct'] = (group_df[sales_column] / total_sales) * 100
    hhi = (group_df['market_share_pct'] ** 2).sum()

    return hhi

def classify_concentration(hhi):
    """Classify market concentration based on DOJ/FTC guidelines"""
    if hhi < 1500:
        return "Unconcentrated"
    elif hhi < 2500:
        return "Moderate"
    else:
        return "High"

# Load data
print("Loading Technomic data...")
df = pd.read_excel('Technomic-Data.xlsx', sheet_name='Technomic Top 500 Chains')

print("\n" + "="*80)
print("HERFINDAHL-HIRSCHMAN INDEX (HHI) ANALYSIS BY MENU TYPE")
print("="*80)

# ============================================================================
# 1. HHI BY MENU TYPE
# ============================================================================

menu_hhi_data = []

for menu_type in df['Menu Type'].unique():
    menu_df = df[df['Menu Type'] == menu_type]

    # Calculate HHI for multiple years
    hhi_2024 = calculate_hhi(menu_df, '2024 U.S. Sales ($000)')
    hhi_2023 = calculate_hhi(menu_df, '2023 U.S. Sales ($000)')
    hhi_2019 = calculate_hhi(menu_df, '2019 U.S. Sales ($000)')

    # Category metrics
    total_sales_2024 = menu_df['2024 U.S. Sales ($000)'].sum()
    num_chains = len(menu_df)

    # Top 3 chains market share
    top3_share = (menu_df.nlargest(3, '2024 U.S. Sales ($000)')['2024 U.S. Sales ($000)'].sum() / total_sales_2024) * 100

    # Get top chain details
    top_chain_row = menu_df.nlargest(1, '2024 U.S. Sales ($000)').iloc[0]
    top_chain_share = (top_chain_row['2024 U.S. Sales ($000)'] / total_sales_2024) * 100

    menu_hhi_data.append({
        'Menu_Type': menu_type,
        'Total_Sales_2024_$M': round(total_sales_2024 / 1000, 1),
        'Num_Chains': num_chains,
        'HHI_2024': round(hhi_2024, 1),
        'HHI_2023': round(hhi_2023, 1),
        'HHI_2019': round(hhi_2019, 1),
        'HHI_Change_5Y': round(hhi_2024 - hhi_2019, 1),
        'HHI_Change_1Y': round(hhi_2024 - hhi_2023, 1),
        'Concentration_2024': classify_concentration(hhi_2024),
        'Top_Chain': top_chain_row['Chain Name'],
        'Top_Chain_Share_%': round(top_chain_share, 1),
        'Top3_Share_%': round(top3_share, 1)
    })

hhi_df = pd.DataFrame(menu_hhi_data)
hhi_df = hhi_df.sort_values('Total_Sales_2024_$M', ascending=False)

print("\nMENU TYPE HHI RANKINGS (Sorted by Total Sales):")
print(hhi_df.to_string(index=False))

# ============================================================================
# 2. CONCENTRATION TRENDS
# ============================================================================

print("\n" + "="*80)
print("CONCENTRATION TREND ANALYSIS")
print("="*80)

print("\nCategories with INCREASING Concentration (Top 10):")
increasing = hhi_df.nlargest(10, 'HHI_Change_5Y')[
    ['Menu_Type', 'HHI_2019', 'HHI_2024', 'HHI_Change_5Y', 'Concentration_2024']
]
print(increasing.to_string(index=False))

print("\nCategories with DECREASING Concentration (Top 10):")
decreasing = hhi_df.nsmallest(10, 'HHI_Change_5Y')[
    ['Menu_Type', 'HHI_2019', 'HHI_2024', 'HHI_Change_5Y', 'Concentration_2024']
]
print(decreasing.to_string(index=False))

# ============================================================================
# 3. DOMINANT PLAYER ANALYSIS
# ============================================================================

print("\n" + "="*80)
print("DOMINANT PLAYER ANALYSIS")
print("="*80)

print("\nMost Concentrated Categories (HHI ≥ 4000):")
highly_concentrated = hhi_df[hhi_df['HHI_2024'] >= 4000][
    ['Menu_Type', 'HHI_2024', 'Top_Chain', 'Top_Chain_Share_%', 'Num_Chains']
]
print(highly_concentrated.to_string(index=False))

print("\nMost Competitive Categories (HHI < 1500):")
competitive = hhi_df[hhi_df['HHI_2024'] < 1500][
    ['Menu_Type', 'HHI_2024', 'Top_Chain', 'Top_Chain_Share_%', 'Num_Chains']
]
print(competitive.to_string(index=False))

# ============================================================================
# 4. SUBSEGMENT HHI ANALYSIS
# ============================================================================

print("\n" + "="*80)
print("HHI BY SUBSEGMENT")
print("="*80)

subseg_hhi_data = []

for subseg in df['Subsegment'].unique():
    subseg_df = df[df['Subsegment'] == subseg]

    hhi_2024 = calculate_hhi(subseg_df, '2024 U.S. Sales ($000)')
    hhi_2019 = calculate_hhi(subseg_df, '2019 U.S. Sales ($000)')

    total_sales = subseg_df['2024 U.S. Sales ($000)'].sum()
    top_chain = subseg_df.nlargest(1, '2024 U.S. Sales ($000)').iloc[0]
    top_share = (top_chain['2024 U.S. Sales ($000)'] / total_sales) * 100

    subseg_hhi_data.append({
        'Subsegment': subseg,
        'Total_Sales_$B': round(total_sales / 1e6, 1),
        'Num_Chains': len(subseg_df),
        'HHI_2024': round(hhi_2024, 1),
        'HHI_2019': round(hhi_2019, 1),
        'HHI_Change': round(hhi_2024 - hhi_2019, 1),
        'Concentration': classify_concentration(hhi_2024),
        'Top_Chain': top_chain['Chain Name'],
        'Top_Share_%': round(top_share, 1)
    })

subseg_hhi_df = pd.DataFrame(subseg_hhi_data)
subseg_hhi_df = subseg_hhi_df.sort_values('Total_Sales_$B', ascending=False)

print("\nSubsegment HHI Analysis:")
print(subseg_hhi_df.to_string(index=False))

# ============================================================================
# 5. DETAILED CHAIN-LEVEL SHARES (FOR TOP CATEGORIES)
# ============================================================================

print("\n" + "="*80)
print("DETAILED MARKET SHARES - TOP 5 MENU TYPES")
print("="*80)

top_categories = hhi_df.head(5)['Menu_Type'].values

for category in top_categories:
    print(f"\n{'='*80}")
    print(f"{category.upper()} - Detailed Market Shares")
    print(f"{'='*80}")

    cat_df = df[df['Menu Type'] == category].copy()
    total_cat_sales = cat_df['2024 U.S. Sales ($000)'].sum()

    cat_df['Market_Share_%'] = (cat_df['2024 U.S. Sales ($000)'] / total_cat_sales) * 100
    cat_df['Cumulative_Share_%'] = cat_df.sort_values('2024 U.S. Sales ($000)', ascending=False)['Market_Share_%'].cumsum()

    top_chains = cat_df.nlargest(10, '2024 U.S. Sales ($000)')[
        ['Chain Name', '2024 U.S. Sales ($000)', 'Market_Share_%', '2024 U.S. Units', '5-Year Sales CAGR']
    ].copy()

    top_chains['Sales_$M'] = (top_chains['2024 U.S. Sales ($000)'] / 1000).round(1)
    top_chains['Market_Share_%'] = top_chains['Market_Share_%'].round(1)
    top_chains['5Y_CAGR_%'] = (top_chains['5-Year Sales CAGR'] * 100).round(1)

    print(top_chains[['Chain Name', 'Sales_$M', 'Market_Share_%', '2024 U.S. Units', '5Y_CAGR_%']].to_string(index=False))

    # Calculate HHI for this category
    cat_hhi = calculate_hhi(cat_df, '2024 U.S. Sales ($000)')
    print(f"\nCategory HHI: {cat_hhi:.1f} ({classify_concentration(cat_hhi)} concentration)")

# ============================================================================
# 6. EXPORT RESULTS
# ============================================================================

print("\n" + "="*80)
print("EXPORTING HHI ANALYSIS")
print("="*80)

hhi_df.to_csv('hhi_menu_type_analysis.csv', index=False)
print("✓ Exported: hhi_menu_type_analysis.csv")

subseg_hhi_df.to_csv('hhi_subsegment_analysis.csv', index=False)
print("✓ Exported: hhi_subsegment_analysis.csv")

# Create detailed market share file for each major category
market_shares_list = []
for category in df['Menu Type'].unique():
    cat_df = df[df['Menu Type'] == category].copy()
    total_cat_sales = cat_df['2024 U.S. Sales ($000)'].sum()
    cat_df['Category'] = category
    cat_df['Market_Share_in_Category_%'] = (cat_df['2024 U.S. Sales ($000)'] / total_cat_sales) * 100
    market_shares_list.append(cat_df[['Category', 'Chain Name', '2024 U.S. Sales ($000)', 
                                       'Market_Share_in_Category_%', '2024 U.S. Units']])

detailed_shares = pd.concat(market_shares_list, ignore_index=True)
detailed_shares = detailed_shares.sort_values(['Category', '2024 U.S. Sales ($000)'], ascending=[True, False])
detailed_shares.to_csv('detailed_market_shares_by_category.csv', index=False)
print("✓ Exported: detailed_market_shares_by_category.csv")

print("\n" + "="*80)
print("HHI INTERPRETATION GUIDE")
print("="*80)
print("HHI < 1,500:         Unconcentrated (highly competitive)")
print("1,500 ≤ HHI < 2,500: Moderately concentrated")
print("HHI ≥ 2,500:         Highly concentrated")
print("HHI ≥ 4,000:         Oligopoly/near-monopoly")
print("\nBased on U.S. Department of Justice / Federal Trade Commission guidelines")
print("\nANALYSIS COMPLETE!")
