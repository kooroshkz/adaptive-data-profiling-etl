# helper functions for pdf reading
import pdfplumber
import os
import json
from pathlib import Path


def get_company_reports(reports_dir="reports"):
    """get list of all company reports with paths"""
    reports_path = Path(reports_dir)
    companies = {}
    
    for company_dir in sorted(reports_path.iterdir()):
        if company_dir.is_dir():
            company_name = company_dir.name
            companies[company_name] = {}
            
            for year_dir in sorted(company_dir.iterdir()):
                if year_dir.is_dir():
                    year = year_dir.name
                    pdf_path = year_dir / "annual_report.pdf"
                    if pdf_path.exists():
                        companies[company_name][year] = str(pdf_path)
    
    return companies


def extract_text_from_pdf(pdf_path, page_numbers=None, include_metadata=True):
    """extract text from pdf pages with metadata"""
    text_blocks = []
    metadata = {}
    
    with pdfplumber.open(pdf_path) as pdf:
        # get pdf metadata
        if include_metadata:
            metadata = {
                'total_pages': len(pdf.pages),
                'metadata': pdf.metadata if hasattr(pdf, 'metadata') else {}
            }
        
        pages_to_process = page_numbers if page_numbers else range(len(pdf.pages))
        
        for page_num in pages_to_process:
            page = pdf.pages[page_num]
            text = page.extract_text()
            if text:
                text_blocks.append({
                    'page': page_num + 1,
                    'text': text
                })
    
    return {'pages': text_blocks, 'metadata': metadata}


def search_in_pdf(pdf_path, keyword, case_sensitive=False):
    """find pages containing specific keywords"""
    matching_pages = []
    
    with pdfplumber.open(pdf_path) as pdf:
        for i, page in enumerate(pdf.pages):
            text = page.extract_text()
            if text:
                if case_sensitive:
                    if keyword in text:
                        matching_pages.append(i)
                else:
                    if keyword.lower() in text.lower():
                        matching_pages.append(i)
    
    return matching_pages


def chunk_text_for_llm(text, max_chars=4000):
    """split long text into chunks for llm processing"""
    chunks = []
    current = ""
    
    for para in text.split('\n\n'):
        if len(current) + len(para) < max_chars:
            current += para + '\n\n'
        else:
            if current:
                chunks.append(current.strip())
            current = para + '\n\n'
    
    if current:
        chunks.append(current.strip())
    
    return chunks

