"""
ESG Data Visualization
Generates charts for ING Hackathon analysis
"""
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from pathlib import Path

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 8)

# Load data
data_path = Path(__file__).parent.parent.parent / 'data/processed/esg_extracted_data.csv'
df = pd.read_csv(data_path)

# Normalize units to tCO2e (metric tons)
def normalize_emissions(value, unit):
    """Convert all units to metric tons CO2e"""
    if pd.isna(value) or pd.isna(unit):
        return None
    
    value = float(value)
    unit = str(unit).lower()
    
    # Mt, MtCO2e = million metric tons = 1,000,000 tons
    if 'mt' in unit and 'mmt' not in unit:
        return value * 1_000_000
    
    # MMT = million metric tons
    if 'mmt' in unit:
        return value * 1_000_000
    
    # million tonnes
    if 'million' in unit:
        return value * 1_000_000
    
    # Already in metric tons
    if 'mt' not in unit and ('ton' in unit or 'tco' in unit):
        return value
    
    # Default: assume already metric tons
    return value

# Apply normalization
df['scope1_normalized'] = df.apply(lambda row: normalize_emissions(row['scope1_value'], row['scope1_unit']), axis=1)
df['scope2_normalized'] = df.apply(lambda row: normalize_emissions(row['scope2_market_value'], row['scope2_market_unit']), axis=1)

# Calculate total emissions
df['total_emissions'] = df['scope1_normalized'] + df['scope2_normalized']

# Output directory
output_dir = Path(__file__).parent.parent.parent / 'reports/visualizations'
output_dir.mkdir(exist_ok=True, parents=True)

print("=" * 60)
print("GENERATING ESG VISUALIZATIONS")
print("=" * 60)

# ===== CHART 1: Time Series - Emissions by Company =====
fig, axes = plt.subplots(2, 4, figsize=(20, 10))
fig.suptitle('GHG Emissions Trends by Company (2021-2023)', fontsize=16, fontweight='bold')

companies = df['company'].unique()
for idx, company in enumerate(companies):
    row = idx // 4
    col = idx % 4
    ax = axes[row, col]
    
    company_data = df[df['company'] == company].sort_values('year')
    
    years = company_data['year']
    scope1 = company_data['scope1_normalized'] / 1_000_000  # Convert to Mt
    scope2 = company_data['scope2_normalized'] / 1_000_000
    
    ax.plot(years, scope1, marker='o', linewidth=2, label='Scope 1', color='#e74c3c')
    ax.plot(years, scope2, marker='s', linewidth=2, label='Scope 2', color='#3498db')
    
    ax.set_title(company, fontweight='bold', fontsize=12)
    ax.set_xlabel('Year')
    ax.set_ylabel('Emissions (Mt CO₂e)')
    ax.legend(loc='best')
    ax.grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig(output_dir / '01_time_series_by_company.png', dpi=300, bbox_inches='tight')
print(f"✓ Saved: 01_time_series_by_company.png")
plt.close()

# ===== CHART 2: Total Emissions Comparison (2023) =====
fig, ax = plt.subplots(figsize=(14, 8))

latest_data = df[df['year'] == 2023].copy()
latest_data['total_mt'] = latest_data['total_emissions'] / 1_000_000
latest_data = latest_data.sort_values('total_mt', ascending=True)

colors = ['#2ecc71' if x < 20 else '#f39c12' if x < 50 else '#e74c3c' 
          for x in latest_data['total_mt']]

bars = ax.barh(latest_data['company'], latest_data['total_mt'], color=colors, alpha=0.8)

ax.set_xlabel('Total Emissions (Mt CO₂e)', fontsize=12, fontweight='bold')
ax.set_title('Total GHG Emissions Comparison (2023)', fontsize=14, fontweight='bold')
ax.grid(axis='x', alpha=0.3)

# Add value labels
for i, (bar, val) in enumerate(zip(bars, latest_data['total_mt'])):
    if pd.notna(val):
        ax.text(val + 2, bar.get_y() + bar.get_height()/2, 
                f'{val:.1f} Mt', va='center', fontsize=10)

plt.tight_layout()
plt.savefig(output_dir / '02_total_emissions_2023.png', dpi=300, bbox_inches='tight')
print(f"✓ Saved: 02_total_emissions_2023.png")
plt.close()

# ===== CHART 3: Emissions Change 2021 → 2023 =====
fig, ax = plt.subplots(figsize=(14, 8))

change_data = []
for company in companies:
    data_2021 = df[(df['company'] == company) & (df['year'] == 2021)]['total_emissions']
    data_2023 = df[(df['company'] == company) & (df['year'] == 2023)]['total_emissions']
    
    if not data_2021.empty and not data_2023.empty:
        val_2021 = data_2021.values[0]
        val_2023 = data_2023.values[0]
        
        if pd.notna(val_2021) and pd.notna(val_2023):
            pct_change = ((val_2023 - val_2021) / val_2021) * 100
            change_data.append({'company': company, 'change_pct': pct_change})

change_df = pd.DataFrame(change_data).sort_values('change_pct')

colors = ['#2ecc71' if x < 0 else '#e74c3c' for x in change_df['change_pct']]
bars = ax.barh(change_df['company'], change_df['change_pct'], color=colors, alpha=0.8)

ax.axvline(x=0, color='black', linewidth=1)
ax.set_xlabel('Emissions Change (%)', fontsize=12, fontweight='bold')
ax.set_title('Emissions Change: 2021 → 2023', fontsize=14, fontweight='bold')
ax.grid(axis='x', alpha=0.3)

# Add value labels
for bar, val in zip(bars, change_df['change_pct']):
    ax.text(val + (2 if val > 0 else -2), bar.get_y() + bar.get_height()/2,
            f'{val:+.1f}%', va='center', ha='left' if val > 0 else 'right', fontsize=10)

plt.tight_layout()
plt.savefig(output_dir / '03_emissions_change.png', dpi=300, bbox_inches='tight')
print(f"✓ Saved: 03_emissions_change.png")
plt.close()

# ===== CHART 4: Assurance Coverage =====
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

# Pie chart - overall assurance
assurance_counts = df.groupby('assurance')['company'].count()
colors_pie = ['#e74c3c', '#2ecc71']
ax1.pie(assurance_counts, labels=['No Assurance', 'Assured'], autopct='%1.1f%%',
        colors=colors_pie, startangle=90)
ax1.set_title('Assurance Coverage (All Reports)', fontsize=12, fontweight='bold')

# Bar chart - by company (2023)
assurance_2023 = df[df['year'] == 2023].copy()
assurance_2023['has_assurance'] = assurance_2023['assurance'].astype(int)

colors_bar = ['#2ecc71' if x else '#e74c3c' 
              for x in assurance_2023.sort_values('company')['assurance']]

ax2.barh(assurance_2023.sort_values('company')['company'], 
         assurance_2023.sort_values('company')['has_assurance'],
         color=colors_bar, alpha=0.8)
ax2.set_xlabel('Has External Assurance', fontsize=11)
ax2.set_title('External Assurance Status (2023)', fontsize=12, fontweight='bold')
ax2.set_xlim(0, 1.2)
ax2.set_xticks([0, 1])
ax2.set_xticklabels(['No', 'Yes'])

plt.tight_layout()
plt.savefig(output_dir / '04_assurance_coverage.png', dpi=300, bbox_inches='tight')
print(f"✓ Saved: 04_assurance_coverage.png")
plt.close()

# ===== CHART 5: Target Years Distribution =====
fig, ax = plt.subplots(figsize=(14, 8))

target_years = df[df['target_year'].notna()].groupby(['company', 'target_year']).size().reset_index()
target_years.columns = ['company', 'target_year', 'count']

# Get unique companies and target years
companies_with_targets = target_years['company'].unique()
target_year_values = sorted(target_years['target_year'].unique())

# Create position for each company
y_pos = np.arange(len(companies_with_targets))

for i, year in enumerate(target_year_values):
    year_data = target_years[target_years['target_year'] == year]
    
    # Match companies in order
    x_vals = []
    for company in companies_with_targets:
        if company in year_data['company'].values:
            x_vals.append(year)
        else:
            x_vals.append(None)
    
    # Plot markers
    valid_idx = [i for i, x in enumerate(x_vals) if x is not None]
    ax.scatter([x_vals[i] for i in valid_idx], [y_pos[i] for i in valid_idx],
               s=200, alpha=0.7, label=f'{int(year)}')

ax.set_yticks(y_pos)
ax.set_yticklabels(companies_with_targets)
ax.set_xlabel('Target Year', fontsize=12, fontweight='bold')
ax.set_title('Net Zero / Emission Reduction Target Years', fontsize=14, fontweight='bold')
ax.legend(title='Target Year', bbox_to_anchor=(1.05, 1), loc='upper left')
ax.grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig(output_dir / '05_target_years.png', dpi=300, bbox_inches='tight')
print(f"✓ Saved: 05_target_years.png")
plt.close()

# ===== SUMMARY STATISTICS =====
print("\n" + "=" * 60)
print("DATA SUMMARY")
print("=" * 60)

print(f"\n📊 Total companies analyzed: {len(companies)}")
print(f"📅 Years covered: {sorted(df['year'].unique())}")
print(f"📈 Total data points: {len(df)}")

print(f"\n🔍 Data Completeness:")
print(f"  Scope 1: {df['scope1_value'].notna().sum()}/{len(df)} ({df['scope1_value'].notna().sum()/len(df)*100:.1f}%)")
print(f"  Scope 2: {df['scope2_market_value'].notna().sum()}/{len(df)} ({df['scope2_market_value'].notna().sum()/len(df)*100:.1f}%)")
print(f"  Assurance: {df['assurance'].sum()}/{len(df)} ({df['assurance'].sum()/len(df)*100:.1f}%)")
print(f"  Targets: {df['target_year'].notna().sum()}/{len(df)} ({df['target_year'].notna().sum()/len(df)*100:.1f}%)")

print(f"\n✅ All visualizations saved to: {output_dir}")
print("=" * 60)
