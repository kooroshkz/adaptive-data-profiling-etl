# Extraction Strategy

## ✅ PHASE 1: COMPLETED - Raw Text Extraction

### what we did
extracted all text from 24 PDFs (8 companies × 3 years) using **pdfplumber**

### results
- ✓ 24/24 PDFs successfully parsed
- ✓ saved to `data/raw/` as JSON files
- ✓ each file contains: company, year, total pages, text per page
- ✓ no failures

### file structure
```json
{
  "company": "Amazon",
  "year": "2023",
  "pdf_path": "reports/Amazon/2023/annual_report.pdf",
  "total_pages": 98,
  "pages": [
    {"page": 1, "text": "..."},
    {"page": 2, "text": "..."}
  ]
}
```

---

## PHASE 2: NEXT - LLM Structured Extraction

Now that we have raw text, use LLM to extract specific ESG fields

### approach

### 1. field by field extraction
- do scope 1 across ALL companies/years first
- then scope 2, then assurance, etc
- avoids context switching
- easier to refine prompts per field

### 2. pdf chunking strategy

**step 1: keyword search**
```
find pages with "scope 1", "emissions", "ghg"
extract only relevant pages (saves tokens)
```

**step 2: chunk pages**
```
each page text → 4000 char chunks
send to llm with specific extraction prompt
```

**step 3: llm extraction**
```
system: "you extract ESG data from reports"
user: "find scope 1 emissions value and unit from this text"
response: structured json
```

### 3. data flow

```
reports/Company/Year/pdf
    ↓
pdfplumber extracts text
    ↓
chunk into sections
    ↓
llm extracts specific field
    ↓
save to data/raw/scope1.csv
    ↓
repeat for all fields
    ↓
merge into final dataset
```

## why this works

- **pdfplumber**: fast, accurate text extraction
- **chunking**: fit within llm context limits
- **field by field**: optimize prompts per metric
- **llm**: handles varying report formats
- **incremental**: can stop and resume anytime

## validation

- ✅ all 24 pdfs parsed successfully
- ✅ text quality verified on samples
- ✅ page counts match source pdfs
- next: llm extracts structured data from raw text
