# extract esg metrics using llm
import json
import csv
from pathlib import Path
from tqdm import tqdm
from llm_client import extract_with_gpt, search_relevant_pages, prepare_extraction_context


def extract_scope1(company_data):
    """extract scope 1 emissions"""
    keywords = [
        'scope 1', 'direct emissions', 'scope 1 ghg', 'scope 1 emissions',
        'direct ghg', 'operational emissions', 'scope1', 'direct greenhouse'
    ]
    
    # find relevant pages
    relevant_pages = search_relevant_pages(company_data, keywords, max_pages=15)
    
    if not relevant_pages:
        return {"value": None, "unit": None, "confidence": "none", "note": "No relevant pages found"}
    
    # prepare context
    context = prepare_extraction_context(relevant_pages, max_chars=12000)
    
    # extraction prompt
    prompt = """Extract Scope 1 emissions data from this sustainability report.

**Scope 1 Definition**: Direct greenhouse gas emissions from sources OWNED or CONTROLLED by the organization (e.g., facilities, company vehicles, manufacturing).

**Instructions**:
1. Look for ABSOLUTE VALUES (not percentages or reductions)
2. Find the TOTAL Scope 1 emissions for the reporting year
3. Extract BOTH the numeric value AND the unit
4. Common units: tCO₂e, mtCO₂e, MMT CO₂e, tonnes CO₂e, kt CO₂e
5. If you see tables, look for "Scope 1" row
6. Ignore location-based vs market-based distinctions (that's for Scope 2)

**Return JSON**:
{
    "value": <number>,
    "unit": "<exact unit from report>",
    "confidence": "high|medium|low|none",
    "note": "<brief explanation of where found or why not found>"
}

**Examples**:
- "Scope 1: 14.27 MMT CO₂e" → {"value": 14.27, "unit": "MMT CO₂e", "confidence": "high"}
- "Direct emissions: 482,123 tCO₂e" → {"value": 482123, "unit": "tCO₂e", "confidence": "high"}
- Only percentages found → {"value": null, "unit": null, "confidence": "none", "note": "Only percentage reductions mentioned"}

Return ONLY valid JSON, no additional text."""
    
    try:
        response = extract_with_gpt(prompt, context)
        result = json.loads(response)
        return result
    except Exception as e:
        return {"value": None, "unit": None, "confidence": "error", "note": str(e)}


def extract_scope2_market(company_data):
    """extract scope 2 market-based emissions"""
    keywords = [
        'scope 2', 'market-based', 'market based', 'indirect emissions',
        'scope2', 'purchased electricity', 'scope 2 ghg', 'energy indirect'
    ]
    
    relevant_pages = search_relevant_pages(company_data, keywords, max_pages=15)
    
    if not relevant_pages:
        return {"value": None, "unit": None, "confidence": "none", "note": "No relevant pages found"}
    
    context = prepare_extraction_context(relevant_pages, max_chars=12000)
    
    prompt = """Extract Scope 2 MARKET-BASED emissions from this sustainability report.

**Scope 2 Definition**: Indirect emissions from purchased energy (electricity, steam, heat, cooling).

**CRITICAL**: Extract ONLY the MARKET-BASED value, NOT location-based!
- Market-based uses emission factors from specific supplier contracts
- Location-based uses average grid emission factors
- We want MARKET-BASED values ONLY

**Instructions**:
1. Look for ABSOLUTE VALUES for Scope 2 market-based
2. Extract the numeric value AND unit
3. Common units: tCO₂e, mtCO₂e, MMT CO₂e, tonnes CO₂e, kt CO₂e
4. If only "Scope 2" is shown (no location/market distinction), extract that value
5. If you see both location and market-based, choose market-based

**Return JSON**:
{
    "value": <number>,
    "unit": "<exact unit>",
    "confidence": "high|medium|low|none",
    "note": "<brief explanation>"
}

**Examples**:
- "Scope 2 (market-based): 11 mtCO₂e" → {"value": 11, "unit": "mtCO₂e", "confidence": "high"}
- Only location-based shown → {"value": null, "unit": null, "confidence": "none", "note": "Only location-based found"}

Return ONLY valid JSON."""
    
    try:
        response = extract_with_gpt(prompt, context)
        result = json.loads(response)
        return result
    except Exception as e:
        return {"value": None, "unit": None, "confidence": "error", "note": str(e)}


def extract_targets(company_data):
    """extract emission reduction targets"""
    keywords = [
        'target', 'net zero', 'net-zero', 'carbon neutral', 'emission reduction',
        'reduction target', '2030', '2040', '2050', 'baseline', 'commitment'
    ]
    
    relevant_pages = search_relevant_pages(company_data, keywords, max_pages=15)
    
    if not relevant_pages:
        return {"target": None, "target_year": None, "baseline_year": None, "confidence": "none"}
    
    context = prepare_extraction_context(relevant_pages, max_chars=12000)
    
    prompt = """Extract GHG emission reduction TARGETS from this sustainability report.

**Target Definition**: Measurable goals to reduce greenhouse gas emissions within a defined timeframe.

**Instructions**:
1. Find the main emission reduction target (percentage or absolute value)
2. Extract the target year (e.g., 2030, 2050)
3. Extract the baseline year if mentioned (e.g., base year 2019)
4. Look for commitments like "net zero by 2050" or "reduce by 30% by 2030"

**Return JSON**:
{
    "target": "<target description>",
    "target_year": <year as number>,
    "baseline_year": <year as number or null>,
    "confidence": "high|medium|low|none",
    "note": "<brief explanation>"
}

**Examples**:
- "30% reduction by 2030 (base year 2019)" → {"target": "30% reduction", "target_year": 2030, "baseline_year": 2019, "confidence": "high"}
- "Net zero by 2050" → {"target": "net zero", "target_year": 2050, "baseline_year": null, "confidence": "high"}
- No specific target → {"target": null, "target_year": null, "baseline_year": null, "confidence": "none"}

Return ONLY valid JSON."""
    
    try:
        response = extract_with_gpt(prompt, context)
        result = json.loads(response)
        return result
    except Exception as e:
        return {"target": None, "target_year": None, "baseline_year": None, "confidence": "error", "note": str(e)}


def extract_assurance(company_data):
    """check for external assurance"""
    keywords = ['assurance', 'verification', 'verified', 'assured', 'independent audit', 'third party']
    
    relevant_pages = search_relevant_pages(company_data, keywords, max_pages=8)
    
    if not relevant_pages:
        return {"assured": False, "confidence": "low"}
    
    context = prepare_extraction_context(relevant_pages, max_chars=6000)
    
    prompt = """Determine if this sustainability report has EXTERNAL ASSURANCE.

External assurance means:
- Independent third-party verification/audit of sustainability data
- Limited or reasonable assurance by external auditors (e.g., KPMG, EY, PwC, Deloitte)
- Explicit statements about external verification

Return JSON format:
{
    "assured": true/false,
    "assurance_provider": "<company name or null>",
    "assurance_level": "limited|reasonable|null",
    "confidence": "high|medium|low"
}

Set assured=true only if external verification is explicitly mentioned.
Set assured=false if only internal reviews or no assurance is mentioned."""
    
    try:
        response = extract_with_gpt(prompt, context)
        result = json.loads(response)
        return result
    except Exception as e:
        print(f"  Error extracting assurance: {e}")
        return {"assured": False, "confidence": "error"}


def extract_all_metrics(json_file):
    """extract all metrics from one company-year file"""
    data = json.load(open(json_file))
    
    company = data['company']
    year = data['year']
    
    print(f"\n{company} {year}:")
    
    # extract each metric
    scope1 = extract_scope1(data)
    print(f"  Scope 1: {scope1.get('value')} {scope1.get('unit')} [{scope1.get('confidence')}]")
    
    scope2 = extract_scope2_market(data)
    print(f"  Scope 2: {scope2.get('value')} {scope2.get('unit')} [{scope2.get('confidence')}]")
    
    assurance = extract_assurance(data)
    print(f"  Assurance: {assurance.get('assured')} [{assurance.get('confidence')}]")
    
    targets = extract_targets(data)
    print(f"  Target: {targets.get('target')} by {targets.get('target_year')} [{targets.get('confidence')}]")
    
    return {
        'company': company,
        'year': year,
        'scope1_value': scope1.get('value'),
        'scope1_unit': scope1.get('unit'),
        'scope1_confidence': scope1.get('confidence'),
        'scope1_note': scope1.get('note', ''),
        'scope2_market_value': scope2.get('value'),
        'scope2_market_unit': scope2.get('unit'),
        'scope2_market_confidence': scope2.get('confidence'),
        'scope2_note': scope2.get('note', ''),
        'assurance': assurance.get('assured'),
        'assurance_provider': assurance.get('assurance_provider'),
        'assurance_level': assurance.get('assurance_level'),
        'assurance_confidence': assurance.get('confidence'),
        'target': targets.get('target'),
        'target_year': targets.get('target_year'),
        'baseline_year': targets.get('baseline_year'),
        'target_confidence': targets.get('confidence'),
        'target_note': targets.get('note', '')
    }


def extract_all_companies(limit=None):
    """extract from all companies and save to csv"""
    # get to project root
    project_root = Path(__file__).parent.parent.parent
    raw_dir = project_root / 'data' / 'raw'
    output_dir = project_root / 'data' / 'processed'
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # get all json files
    files = sorted([f for f in raw_dir.glob('*.json') if f.name != 'extraction_stats.json'])
    
    if limit:
        # for testing, get diverse sample including BP and Shell
        test_files = [
            'BP_2023.json', 'Shell_2022.json', 'Amazon_2023.json',
            'Intel_2023.json', 'Nestle_2022.json'
        ]
        files = [raw_dir / f for f in test_files if (raw_dir / f).exists()][:limit]
    
    results = []
    
    print(f"Extracting ESG data from {len(files)} companies...")
    print("=" * 60)
    
    for file in tqdm(files, desc="Companies"):
        try:
            result = extract_all_metrics(file)
            results.append(result)
        except Exception as e:
            print(f"\nFailed on {file.name}: {e}")
            continue
    
    # save to csv
    output_file = output_dir / 'esg_extracted_data.csv'
    
    if results:
        fieldnames = results[0].keys()
        with open(output_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)
        
        print(f"\n✓ Saved {len(results)} records to {output_file}")
    
    return results


if __name__ == "__main__":
    import sys
    
    if 'all' in sys.argv:
        print("FULL EXTRACTION MODE: Processing all 24 companies\n")
        results = extract_all_companies(limit=None)
    else:
        # test on 3 diverse companies
        print("TEST MODE: Processing 3 sample companies")
        print("Run with 'python extract_esg_data.py all' for full extraction\n")
        results = extract_all_companies(limit=3)
