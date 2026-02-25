# Zerodha Holdings Discrepancies

End-to-end flow to extract CAS transactions and upload them to Zerodha MF holdings discrepancies.

## Files in this flow

- `CAS_Test.pdf`: input CAS PDF.
- `CAS.txt`: extracted text from PDF.
- `transactions.csv`: parsed transactions from the statement text.
- `trade_groups/<ISIN>.csv`: per-ISIN files with `Date,Price,Units`.
- `run_reports/upload_*.csv`: uploader run reports.

## Step 1: Extract text from PDF

```bash
python3 extract_pdf_text.py CAS_Test.pdf -o CAS.txt
```

Expected output:

```text
Text written to /Users/flukebox/Workspace/Random/Zerodha Holdings Discrepancies/CAS.txt
```

## Step 2: Parse statement text to transactions CSV

```bash
python3 parse_statement_text.py CAS.txt -o transactions.csv
```

Expected output:

```text
Extracted 31 transactions to /Users/flukebox/Workspace/Random/Zerodha Holdings Discrepancies/transactions.csv
```

## Step 3: Split by ISIN (one file per ISIN)

```bash
python3 split_transactions_by_isin.py transactions.csv
```

Expected output (example):

```text
Created 1 file(s) in /Users/flukebox/Workspace/Random/Zerodha Holdings Discrepancies/trade_groups
INF846K01DP8 -> /Users/flukebox/Workspace/Random/Zerodha Holdings Discrepancies/trade_groups/INF846K01DP8.csv
```

## Step 4: Upload to Zerodha

```bash
python3 uploader.py --fund test
```

Example output:

```text
Fund: test (Test Fund)
Input CSV: trade_groups/INF846K01DP8.csv
Trades to process: 7
Report: run_reports/upload_test_20260225_193137.csv
```

## Troubleshooting

- `Created 0 file(s)` from `split_transactions_by_isin.py` usually means the input CSV did not have usable rows with all required fields (`isin`, date, price/nav, units), or an older/incorrect CSV was used.
- Verify headers in input:
  - `transactions.csv` from parser should include: `isin,folio_no,txn_date,amount,units,nav,balance_units`.
- Re-run parse step, then split again on the fresh output file.
- Uploader requires a valid Playwright session (`storage_state.json`) and Zerodha login state.
