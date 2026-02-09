# ING Hackathon 2026

ESG data extraction and analysis for sustainable financing decisions

## structure

```
├── data/
│   ├── raw/           # extracted data from pdfs
│   ├── processed/     # cleaned data
│   └── outputs/       # final results
├── src/
│   ├── extraction/    # pdf parsing scripts
│   ├── analysis/      # data analysis
│   └── visualization/ # charts and dashboards
├── docs/              # methodology and findings
├── notebooks/         # jupyter notebooks
└── reports/           # company pdfs (8 companies, 3 years each)
```

## companies
Amazon, BP, Coca Cola, ENGIE, Intel, Nestle, Shell, SSE

## tasks
1. extract esg metrics from reports
2. visualize trends
3. analyze and recommend financing

## quick start
```bash
# install dependencies
pip install -r requirements.txt

# test pdf parsing
python test_parsing.py

# parse all pdfs to raw text
cd src/extraction && python parse_all_pdfs.py
```
