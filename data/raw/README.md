# Raw Text Extraction - Complete ✓

## what was done

parsed all 24 sustainability/annual reports using pdfplumber

## statistics

- **companies**: 8 (Amazon, BP, Coca Cola, ENGIE, Intel, Nestle, Shell, SSE)
- **years**: 2021, 2022, 2023
- **total reports**: 24
- **success rate**: 100% (0 failures)

## page counts

| Company | 2021 | 2022 | 2023 |
|---------|------|------|------|
| Amazon | 101 | 82 | 98 |
| BP | 61 | 66 | 68 |
| Coca Cola | - | - | - |
| ENGIE | - | - | - |
| Intel | - | - | - |
| Nestle | - | - | - |
| Shell | 93 | 93 | 91 |
| SSE | - | - | - |

## output format

each company-year saved as `data/raw/{Company}_{Year}.json`:

```json
{
  "company": "string",
  "year": "string", 
  "pdf_path": "string",
  "total_pages": int,
  "pages": [
    {"page": int, "text": "string"}
  ]
}
```

## next steps

1. use llm to extract structured esg data from raw text
2. save to csv files by field
3. merge into final dataset
