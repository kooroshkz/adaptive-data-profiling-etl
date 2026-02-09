# quick test script for pdf parsing
import sys
sys.path.insert(0, 'src/extraction')

from parse_all_pdfs import test_single_pdf, parse_all_reports
from utils import get_company_reports


def main():
    print("=== PDF Parsing Test ===\n")
    
    # check what reports we have
    print("1. checking available reports...")
    companies = get_company_reports()
    
    for company, years in companies.items():
        print(f"  {company}: {sorted(years.keys())}")
    
    print(f"\ntotal: {sum(len(y) for y in companies.values())} reports")
    
    # test single pdf
    print("\n2. testing single pdf extraction...")
    print("-" * 60)
    result = test_single_pdf("Amazon", "2023")
    
    # ask if should continue
    print("\n" + "=" * 60)
    response = input("\nparse all PDFs? (y/n): ")
    
    if response.lower() == 'y':
        print("\n3. parsing all pdfs...")
        print("-" * 60)
        stats = parse_all_reports()
        
        print("\n=== complete ===")
        print(f"check data/raw/ for extracted text files")
    else:
        print("\nskipped full extraction")


if __name__ == "__main__":
    main()
