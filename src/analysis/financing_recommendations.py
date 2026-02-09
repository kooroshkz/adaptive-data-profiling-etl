"""
ESG Data Analysis & Financing Recommendations
ING Hackathon 2026
"""
import pandas as pd
import numpy as np
from pathlib import Path

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
    
    if 'mt' in unit and 'mmt' not in unit:
        return value * 1_000_000
    if 'mmt' in unit:
        return value * 1_000_000
    if 'million' in unit:
        return value * 1_000_000
    return value

# Apply normalization
df['scope1_normalized'] = df.apply(lambda row: normalize_emissions(row['scope1_value'], row['scope1_unit']), axis=1)
df['scope2_normalized'] = df.apply(lambda row: normalize_emissions(row['scope2_market_value'], row['scope2_market_unit']), axis=1)
df['total_emissions'] = df['scope1_normalized'] + df['scope2_normalized']

print("=" * 80)
print("ESG DATA ANALYSIS & FINANCING RECOMMENDATIONS")
print("=" * 80)

# ===== ANALYSIS 1: EMISSION TRENDS =====
print("\n📈 EMISSION TRENDS (2021 → 2023)")
print("-" * 80)

trend_analysis = []
for company in df['company'].unique():
    data_2021 = df[(df['company'] == company) & (df['year'] == 2021)]['total_emissions'].values
    data_2023 = df[(df['company'] == company) & (df['year'] == 2023)]['total_emissions'].values
    
    if len(data_2021) > 0 and len(data_2023) > 0 and pd.notna(data_2021[0]) and pd.notna(data_2023[0]):
        change_pct = ((data_2023[0] - data_2021[0]) / data_2021[0]) * 100
        trend = "IMPROVING" if change_pct < 0 else "DETERIORATING"
        
        # Calculate average year-over-year change
        company_data = df[df['company'] == company].sort_values('year')
        yoy_changes = []
        for i in range(len(company_data) - 1):
            curr = company_data.iloc[i]['total_emissions']
            next_val = company_data.iloc[i+1]['total_emissions']
            if pd.notna(curr) and pd.notna(next_val):
                yoy = ((next_val - curr) / curr) * 100
                yoy_changes.append(yoy)
        
        avg_yoy = np.mean(yoy_changes) if yoy_changes else change_pct / 2
        
        trend_analysis.append({
            'company': company,
            'total_change_pct': change_pct,
            'avg_yoy_change': avg_yoy,
            'trend': trend,
            'emissions_2023_mt': data_2023[0] / 1_000_000
        })

trend_df = pd.DataFrame(trend_analysis).sort_values('total_change_pct')

for _, row in trend_df.iterrows():
    symbol = "✅" if row['trend'] == "IMPROVING" else "⚠️"
    print(f"{symbol} {row['company']:15} | {row['total_change_pct']:+7.1f}% | "
          f"Avg YoY: {row['avg_yoy_change']:+6.1f}% | {row['emissions_2023_mt']:8.1f} Mt")

# ===== ANALYSIS 2: DATA RELIABILITY SCORING =====
print("\n🔍 DATA RELIABILITY SCORE")
print("-" * 80)

reliability_scores = []
for company in df['company'].unique():
    company_data = df[df['company'] == company]
    
    # Scoring factors
    scope1_complete = company_data['scope1_value'].notna().sum() / len(company_data)
    scope2_complete = company_data['scope2_market_value'].notna().sum() / len(company_data)
    has_assurance = company_data['assurance'].sum() > 0
    assurance_level = "limited" if has_assurance else "none"
    
    # Calculate reliability score (0-100)
    score = 0
    score += scope1_complete * 30  # Scope 1 completeness (30 points)
    score += scope2_complete * 30  # Scope 2 completeness (30 points)
    score += 40 if has_assurance else 0  # External assurance (40 points)
    
    reliability_scores.append({
        'company': company,
        'reliability_score': score,
        'scope1_complete': scope1_complete * 100,
        'scope2_complete': scope2_complete * 100,
        'has_assurance': has_assurance,
        'grade': 'A' if score >= 90 else 'B' if score >= 70 else 'C' if score >= 50 else 'D'
    })

reliability_df = pd.DataFrame(reliability_scores).sort_values('reliability_score', ascending=False)

for _, row in reliability_df.iterrows():
    symbol = "🟢" if row['grade'] in ['A', 'B'] else "🟡" if row['grade'] == 'C' else "🔴"
    assurance = "✓" if row['has_assurance'] else "✗"
    print(f"{symbol} {row['company']:15} | Score: {row['reliability_score']:5.1f} | Grade: {row['grade']} | "
          f"Assurance: {assurance} | Data: {row['scope1_complete']:.0f}%/{row['scope2_complete']:.0f}%")

# ===== ANALYSIS 3: TARGET AMBITION ASSESSMENT =====
print("\n🎯 TARGET AMBITION ASSESSMENT")
print("-" * 80)

target_assessment = []
for company in df['company'].unique():
    company_data = df[df['company'] == company]
    
    # Get most recent target
    latest_target = company_data[company_data['target_year'].notna()].iloc[-1] if any(company_data['target_year'].notna()) else None
    
    if latest_target is not None:
        target_year = latest_target['target_year']
        current_year = 2023
        years_to_target = target_year - current_year
        
        # Assess ambition
        if target_year <= 2030:
            ambition = "HIGH (near-term)"
        elif target_year <= 2040:
            ambition = "MEDIUM (mid-term)"
        else:
            ambition = "LOW (long-term)"
        
        target_assessment.append({
            'company': company,
            'target': latest_target['target'],
            'target_year': int(target_year),
            'years_remaining': years_to_target,
            'ambition': ambition
        })

target_df = pd.DataFrame(target_assessment).sort_values('target_year')

for _, row in target_df.iterrows():
    symbol = "🔥" if "HIGH" in row['ambition'] else "⚡" if "MEDIUM" in row['ambition'] else "🕐"
    print(f"{symbol} {row['company']:15} | Target: {row['target_year']} ({row['years_remaining']} years) | "
          f"Ambition: {row['ambition']}")

# ===== FINANCING RECOMMENDATIONS =====
print("\n" + "=" * 80)
print("💰 ING FINANCING RECOMMENDATIONS")
print("=" * 80)

# Combine all analyses
final_scores = []
for company in df['company'].unique():
    # Get trend score (0-40 points)
    trend_row = trend_df[trend_df['company'] == company]
    if not trend_row.empty:
        change_pct = trend_row.iloc[0]['total_change_pct']
        trend_score = max(0, 40 - abs(change_pct))  # Better score for smaller changes or reductions
        if change_pct < 0:  # Bonus for reductions
            trend_score = min(40, trend_score + 20)
    else:
        trend_score = 0
    
    # Get reliability score (0-30 points, normalized from 0-100)
    reliability_row = reliability_df[reliability_df['company'] == company]
    reliability_score = (reliability_row.iloc[0]['reliability_score'] / 100) * 30 if not reliability_row.empty else 0
    
    # Get target score (0-30 points)
    target_row = target_df[target_df['company'] == company]
    if not target_row.empty:
        years = target_row.iloc[0]['years_remaining']
        if years <= 7:  # 2030 target
            target_score = 30
        elif years <= 17:  # 2040 target
            target_score = 20
        else:  # 2050 target
            target_score = 10
    else:
        target_score = 0
    
    total_score = trend_score + reliability_score + target_score
    
    # Determine recommendation
    if total_score >= 75:
        recommendation = "STRONGLY RECOMMEND"
        priority = "HIGH PRIORITY"
    elif total_score >= 60:
        recommendation = "RECOMMEND"
        priority = "MEDIUM PRIORITY"
    elif total_score >= 45:
        recommendation = "CONDITIONAL"
        priority = "LOW PRIORITY"
    else:
        recommendation = "NOT RECOMMENDED"
        priority = "AVOID"
    
    final_scores.append({
        'company': company,
        'total_score': total_score,
        'trend_score': trend_score,
        'reliability_score': reliability_score,
        'target_score': target_score,
        'recommendation': recommendation,
        'priority': priority
    })

final_df = pd.DataFrame(final_scores).sort_values('total_score', ascending=False)

print("\nRANKING:")
for idx, row in final_df.iterrows():
    if row['priority'] == "HIGH PRIORITY":
        symbol = "🟢"
    elif row['priority'] == "MEDIUM PRIORITY":
        symbol = "🟡"
    elif row['priority'] == "LOW PRIORITY":
        symbol = "🟠"
    else:
        symbol = "🔴"
    
    print(f"\n{symbol} {row['company']}")
    print(f"   Overall Score: {row['total_score']:.1f}/100")
    print(f"   - Trend: {row['trend_score']:.1f}/40  |  Reliability: {row['reliability_score']:.1f}/30  |  Target: {row['target_score']:.1f}/30")
    print(f"   Decision: {row['recommendation']} ({row['priority']})")

# ===== EXECUTIVE SUMMARY =====
print("\n" + "=" * 80)
print("📋 EXECUTIVE SUMMARY FOR ING BANK")
print("=" * 80)

high_priority = final_df[final_df['priority'] == "HIGH PRIORITY"]
medium_priority = final_df[final_df['priority'] == "MEDIUM PRIORITY"]
improving_companies = trend_df[trend_df['trend'] == "IMPROVING"]

print(f"""
TOP RECOMMENDATIONS:
{chr(10).join([f"  🟢 {row['company']} (Score: {row['total_score']:.1f})" for _, row in high_priority.iterrows()])}

SECONDARY CONSIDERATIONS:
{chr(10).join([f"  🟡 {row['company']} (Score: {row['total_score']:.1f})" for _, row in medium_priority.iterrows()])}

KEY INSIGHTS:
  • {len(improving_companies)}/{len(trend_df)} companies show emission reductions (2021→2023)
  • {len(reliability_df[reliability_df['has_assurance']])} companies have external assurance
  • {len(target_df[target_df['target_year'] <= 2030])} companies have near-term targets (≤2030)
  • Average data completeness: {reliability_df['reliability_score'].mean():.1f}/100

TRANSITION RISK ASSESSMENT:
  • LOW RISK: Companies with improving trends + high data quality + ambitious targets
  • MEDIUM RISK: Companies with stable emissions but strong governance
  • HIGH RISK: Companies with rising emissions or poor data transparency

RECOMMENDATION FOR ING:
  Prioritize financing for companies demonstrating:
    1. Measurable emission reductions
    2. External assurance of sustainability data
    3. Near-term science-based targets (2030-2040)
    4. Transparent reporting practices
""")

# Save analysis to file
output_path = Path(__file__).parent.parent.parent / 'reports/analysis_summary.txt'
with open(output_path, 'w') as f:
    f.write("=" * 80 + "\n")
    f.write("ESG DATA ANALYSIS & FINANCING RECOMMENDATIONS\n")
    f.write("ING Hackathon 2026\n")
    f.write("=" * 80 + "\n\n")
    
    f.write("FINANCING PRIORITY RANKING:\n")
    f.write("-" * 80 + "\n")
    for idx, row in final_df.iterrows():
        f.write(f"\n{row['company']}\n")
        f.write(f"  Score: {row['total_score']:.1f}/100\n")
        f.write(f"  Recommendation: {row['recommendation']} ({row['priority']})\n")

print(f"\n✅ Analysis saved to: {output_path}")
print("=" * 80)
