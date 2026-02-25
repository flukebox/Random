# Random

This repository currently contains one primary project:

## Zerodha Holdings Discrepancies

Directory: `ZerodhaHoldingsDiscrepancies/`

What it does:
- Extracts text from a CAMS CAS PDF (`extract_pdf_text.py`).
- Parses transaction rows from CAS text into CSV (`parse_statement_text.py`).
- Splits parsed transactions into one CSV per ISIN (`split_transactions_by_isin.py`).
- Uploads normalized trades into Zerodha Holdings Discrepancies via Playwright automation (`uploader.py`).

For setup, usage steps, and troubleshooting, see:

- `ZerodhaHoldingsDiscrepancies/README.md`
