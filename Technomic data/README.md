# Technomic Top 500 Restaurant Chains Analysis

This directory contains Python scripts for comprehensive analysis of the Technomic Top 500 dataset.

## Files Overview

### Analysis Scripts
- **technomic_analysis.py** - Core data analysis with CSV exports
- **technomic_charts.py** - Visualization generation (6 charts)
- **technomic_advanced.py** - Advanced analytics (see below)

### Data Files
- **Technomic-Data.xlsx** - Source data (500 chains, 2019-2024)

## Quick Start

### 1. Run Core Analysis
```bash
python technomic_analysis.py
```

**Outputs:**
- Console analysis of menu types, subsegments, growth leaders
- CSV exports: subsegment_analysis.csv, fastest_growing_chains.csv, segment_trends.csv, unit_economics.csv, menu_type_analysis.csv

### 2. Generate Charts
```bash
python technomic_charts.py
```

**Outputs:**
- 6 PNG files in `charts/` directory:
  1. Menu type sales and growth analysis
  2. Subsegment performance dashboard
  3. LSR vs FSR segment trends
  4. Fastest growing chains
  5. Unit economics comparison
  6. Market concentration curves

### 3. Advanced Analytics
```bash
python technomic_advanced.py
```

**Includes:**
- Year-by-year growth matrices
- Chain positioning analysis (growth vs size)
- Menu type competitive dynamics
- Recovery analysis (2019-2024)
- Detailed subsegment deep-dives
