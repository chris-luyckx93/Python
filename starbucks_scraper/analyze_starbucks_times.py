import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np

def analyze_opening_times(filename='Starbucks store opening times.csv'):
    """
    Analyze opening times from Column A of the file
    """
    # Read the CSV file
    df = pd.read_csv(filename)
    
    # Get Column A (first column) - assumes opening times are in first column
    opening_times = df.iloc[:, 0].dropna()
    
    print(f"Loaded {len(opening_times)} opening times")
    print(f"\nColumn name: {df.columns[0]}")
    print(f"\nFirst 5 entries:")
    print(opening_times.head())
    
    # Convert times to datetime objects for analysis
    def parse_time(time_str):
        """Parse time string like '5:00 AM' to datetime.time"""
        try:
            return datetime.strptime(str(time_str).strip(), '%I:%M %p').time()
        except:
            try:
                return datetime.strptime(str(time_str).strip(), '%H:%M').time()
            except:
                return None
    
    def time_to_minutes(t):
        """Convert time to minutes since midnight"""
        if t is None:
            return None
        return t.hour * 60 + t.minute
    
    # Parse all times
    parsed_times = opening_times.apply(parse_time)
    valid_times = parsed_times.dropna()
    
    print(f"\nSuccessfully parsed: {len(valid_times)} times")
    
    # Convert to minutes for average calculation
    minutes = valid_times.apply(time_to_minutes)
    
    # Calculate average
    avg_minutes = minutes.mean()
    avg_hour = int(avg_minutes // 60)
    avg_min = int(avg_minutes % 60)
    avg_period = "AM" if avg_hour < 12 else "PM"
    display_hour = avg_hour if avg_hour <= 12 else avg_hour - 12
    if display_hour == 0:
        display_hour = 12
    
    # Calculate median
    median_minutes = minutes.median()
    median_hour = int(median_minutes // 60)
    median_min = int(median_minutes % 60)
    median_period = "AM" if median_hour < 12 else "PM"
    median_display_hour = median_hour if median_hour <= 12 else median_hour - 12
    if median_display_hour == 0:
        median_display_hour = 12
    
    print("\n" + "="*60)
    print("OPENING TIME STATISTICS")
    print("="*60)
    print(f"Total Stores: {len(opening_times):,}")
    print(f"Average Opening Time: {display_hour}:{avg_min:02d} {avg_period}")
    print(f"Median Opening Time: {median_display_hour}:{median_min:02d} {median_period}")
    print(f"Earliest Opening: {valid_times.min()}")
    print(f"Latest Opening: {valid_times.max()}")
    
    # Distribution analysis
    print("\n" + "="*60)
    print("DISTRIBUTION OF OPENING TIMES")
    print("="*60)
    
    time_counts = opening_times.value_counts().sort_values(ascending=False)
    for opening_time, count in time_counts.items():
        pct = (count / len(opening_times)) * 100
        bar = '█' * int(pct / 2)
        print(f"{str(opening_time):15} {count:5,} ({pct:5.1f}%)  {bar}")
    
    # Create visualizations
    fig = plt.figure(figsize=(18, 12))
    
    # Plot 1: Bar chart of opening times (ranked by count)
    ax1 = plt.subplot(2, 3, 1)
    time_counts_sorted = time_counts.head(15)
    ax1.bar(range(len(time_counts_sorted)), time_counts_sorted.values, color='#00704A')
    ax1.set_xlabel('Opening Time', fontsize=11)
    ax1.set_ylabel('Number of Stores', fontsize=11)
    ax1.set_title('Top 15 Opening Times (by frequency)', fontsize=12, fontweight='bold')
    ax1.set_xticks(range(len(time_counts_sorted)))
    ax1.set_xticklabels(time_counts_sorted.index, rotation=45, ha='right', fontsize=9)
    ax1.grid(axis='y', alpha=0.3)
    
    # Plot 2: Horizontal bar chart
    ax2 = plt.subplot(2, 3, 2)
    top_10 = time_counts.head(10)
    ax2.barh(range(len(top_10)), top_10.values, color='#00704A')
    ax2.set_yticks(range(len(top_10)))
    ax2.set_yticklabels(top_10.index, fontsize=10)
    ax2.set_xlabel('Number of Stores', fontsize=11)
    ax2.set_title('Top 10 Most Common Opening Times', fontsize=12, fontweight='bold')
    ax2.invert_yaxis()
    ax2.grid(axis='x', alpha=0.3)
    
    # Add count labels
    for i, count in enumerate(top_10.values):
        ax2.text(count + max(top_10.values)*0.01, i, f'{count:,}', va='center', fontsize=9, fontweight='bold')
    
    # Plot 3: Pie chart of top times
    ax3 = plt.subplot(2, 3, 3)
    top_5 = time_counts.head(5)
    other_count = time_counts.iloc[5:].sum() if len(time_counts) > 5 else 0
    
    if other_count > 0:
        pie_data = list(top_5.values) + [other_count]
        pie_labels = list(top_5.index) + ['Other']
    else:
        pie_data = list(top_5.values)
        pie_labels = list(top_5.index)
    
    colors = ['#00704A', '#1E8449', '#27AE60', '#52BE80', '#7DCEA0', '#D5D8DC']
    ax3.pie(pie_data, labels=pie_labels, autopct='%1.1f%%', startangle=90, colors=colors)
    ax3.set_title('Opening Time Distribution (Top 5 + Other)', fontsize=12, fontweight='bold')
    
    # Plot 4: Timeline view (chronological)
    ax4 = plt.subplot(2, 3, 4)
    
    # Create chronological ordering
    time_with_minutes = []
    for time_str in opening_times.value_counts().index:
        t = parse_time(time_str)
        if t:
            mins = time_to_minutes(t)
            count = opening_times.value_counts()[time_str]
            time_with_minutes.append((mins, time_str, count))
    
    time_with_minutes.sort()
    
    if time_with_minutes:
        x_positions = [x[0] for x in time_with_minutes]
        y_values = [x[2] for x in time_with_minutes]
        labels = [x[1] for x in time_with_minutes]
        
        ax4.plot(x_positions, y_values, marker='o', linewidth=2, markersize=8, color='#00704A')
        ax4.fill_between(x_positions, y_values, alpha=0.3, color='#00704A')
        ax4.set_xlabel('Time of Day', fontsize=11)
        ax4.set_ylabel('Number of Stores', fontsize=11)
        ax4.set_title('Opening Times Throughout the Day', fontsize=12, fontweight='bold')
        ax4.grid(alpha=0.3)
        
        # Add vertical line for average
        ax4.axvline(avg_minutes, color='red', linestyle='--', linewidth=2, 
                    label=f'Average: {display_hour}:{avg_min:02d} {avg_period}')
        ax4.legend()
        
        # Set x-axis labels
        ax4.set_xticks(x_positions[::max(1, len(x_positions)//10)])
        ax4.set_xticklabels([labels[i] for i in range(0, len(labels), max(1, len(labels)//10))], 
                           rotation=45, ha='right', fontsize=8)
    
    # Plot 5: Cumulative distribution
    ax5 = plt.subplot(2, 3, 5)
    if time_with_minutes:
        cumsum = np.cumsum(y_values)
        cumsum_pct = (cumsum / cumsum[-1]) * 100
        
        ax5.plot(x_positions, cumsum_pct, linewidth=2.5, color='#00704A')
        ax5.fill_between(x_positions, cumsum_pct, alpha=0.3, color='#00704A')
        ax5.set_xlabel('Time of Day', fontsize=11)
        ax5.set_ylabel('Cumulative % of Stores', fontsize=11)
        ax5.set_title('Cumulative Distribution of Opening Times', fontsize=12, fontweight='bold')
        ax5.grid(alpha=0.3)
        ax5.set_ylim([0, 100])
        
        # Add reference lines
        ax5.axhline(50, color='red', linestyle='--', alpha=0.5, label='50% of stores')
        ax5.axhline(90, color='orange', linestyle='--', alpha=0.5, label='90% of stores')
        ax5.legend()
        
        ax5.set_xticks(x_positions[::max(1, len(x_positions)//10)])
        ax5.set_xticklabels([labels[i] for i in range(0, len(labels), max(1, len(labels)//10))], 
                           rotation=45, ha='right', fontsize=8)
    
    # Plot 6: Statistics box
    ax6 = plt.subplot(2, 3, 6)
    ax6.axis('off')
    
    stats_text = f"""
    SUMMARY STATISTICS
    {'='*40}
    
    Total Stores: {len(opening_times):,}
    
    Average: {display_hour}:{avg_min:02d} {avg_period}
    Median: {median_display_hour}:{median_min:02d} {median_period}
    
    Earliest: {valid_times.min()}
    Latest: {valid_times.max()}
    
    Most Common: {time_counts.index[0]}
    ({time_counts.values[0]:,} stores, {time_counts.values[0]/len(opening_times)*100:.1f}%)
    
    Unique Opening Times: {len(time_counts)}
    
    Standard Deviation: {minutes.std():.1f} minutes
    """
    
    ax6.text(0.1, 0.5, stats_text, fontsize=11, family='monospace', 
             verticalalignment='center', bbox=dict(boxstyle='round', facecolor='#F0F0F0', alpha=0.8))
    
    plt.tight_layout()
    plt.savefig('opening_times_analysis.png', dpi=300, bbox_inches='tight')
    print(f"\n✓ Visualization saved as: opening_times_analysis.png")
    
    # Export summary to CSV
    summary_df = pd.DataFrame({
        'Opening Time': time_counts.index,
        'Store Count': time_counts.values,
        'Percentage': (time_counts.values / len(opening_times) * 100).round(2)
    })
    summary_df.to_csv('opening_times_summary.csv', index=False)
    print(f"✓ Summary exported to: opening_times_summary.csv")
    
    return opening_times, time_counts

if __name__ == "__main__":
    print("Analyzing Starbucks Opening Times...")
    print("="*60 + "\n")
    
    times, distribution = analyze_opening_times()
    
    print("\n" + "="*60)
    print("✓ Analysis complete!")
