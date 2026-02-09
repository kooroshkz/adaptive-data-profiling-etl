# extract all text from pdfs and save to data/raw
import json
from pathlib import Path
from tqdm import tqdm
from utils import get_company_reports, extract_text_from_pdf


def parse_all_reports(output_dir="data/raw", skip_existing=True):
    """parse all pdfs and save raw text"""
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # get all reports
    companies = get_company_reports()
    
    total_reports = sum(len(years) for years in companies.values())
    print(f"found {total_reports} reports across {len(companies)} companies")
    
    # track stats
    stats = {
        'total_processed': 0,
        'skipped': 0,
        'failed': [],
        'companies': {}
    }
    
    # process each report
    for company, years in tqdm(companies.items(), desc="companies"):
        company_stats = {}
        
        for year, pdf_path in years.items():
            output_file = output_path / f"{company}_{year}.json"
            
            # skip if already exists
            if skip_existing and output_file.exists():
                stats['skipped'] += 1
                company_stats[year] = {'status': 'skipped', 'output_file': str(output_file)}
                continue
            
            try:
                # extract text
                print(f"\nprocessing: {company} {year}")
                result = extract_text_from_pdf(pdf_path)
                
                # save to json
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump({
                        'company': company,
                        'year': year,
                        'pdf_path': pdf_path,
                        'total_pages': result['metadata']['total_pages'],
                        'pages': result['pages']
                    }, f, indent=2, ensure_ascii=False)
                
                company_stats[year] = {
                    'status': 'success',
                    'pages': result['metadata']['total_pages'],
                    'output_file': str(output_file)
                }
                stats['total_processed'] += 1
                
            except KeyboardInterrupt:
                print("\n\nstopped by user")
                raise
            except Exception as e:
                print(f"\nfailed: {company} {year} - {str(e)}")
                stats['failed'].append({
                    'company': company,
                    'year': year,
                    'error': str(e)
                })
                company_stats[year] = {'status': 'failed', 'error': str(e)}
        
        stats['companies'][company] = company_stats
    
    # save stats
    stats_file = output_path / "extraction_stats.json"
    with open(stats_file, 'w') as f:
        json.dump(stats, f, indent=2)
    
    print(f"\n✓ processed {stats['total_processed']} reports")
    print(f"→ skipped {stats['skipped']} (already exist)")
    print(f"✗ failed {len(stats['failed'])} reports")
    print(f"→ saved to {output_dir}/")
    
    return stats


def test_single_pdf(company="Amazon", year="2023"):
    """test extraction on single pdf"""
    companies = get_company_reports()
    
    if company not in companies:
        print(f"company {company} not found")
        return
    
    if year not in companies[company]:
        print(f"year {year} not found for {company}")
        return
    
    pdf_path = companies[company][year]
    print(f"testing: {pdf_path}")
    
    result = extract_text_from_pdf(pdf_path)
    
    print(f"\ntotal pages: {result['metadata']['total_pages']}")
    print(f"pages with text: {len(result['pages'])}")
    
    # show first page sample
    if result['pages']:
        first_page = result['pages'][0]
        print(f"\nfirst page preview:")
        print("-" * 50)
        print(first_page['text'][:500])
        print("-" * 50)
    
    return result


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        # test mode
        company = sys.argv[2] if len(sys.argv) > 2 else "Amazon"
        year = sys.argv[3] if len(sys.argv) > 3 else "2023"
        test_single_pdf(company, year)
    else:
        # full extraction
        parse_all_reports()
