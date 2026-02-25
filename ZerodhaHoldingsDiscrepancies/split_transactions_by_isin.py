#!/usr/bin/env python3
import argparse
import csv
import re
import sys
from datetime import datetime
from pathlib import Path


def normalize_date(value: str) -> str:
    text = (value or "").strip()
    if not text:
        raise ValueError("missing date")
    for fmt in ("%Y-%m-%d", "%d-%b-%Y", "%d/%m/%Y", "%d/%m/%y"):
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    raise ValueError(f"invalid date: {value!r}")


def sanitize_filename(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
    return cleaned or "unknown_isin"


def first_non_empty(row: dict[str, str], keys: list[str]) -> str:
    for key in keys:
        value = (row.get(key) or "").strip()
        if value:
            return value
    return ""


def split_transactions_by_isin(
    source_csv: Path,
    output_dir: Path,
    *,
    date_header: str = "Date",
    price_header: str = "Price",
    units_header: str = "Units",
) -> dict[str, Path]:
    if not source_csv.exists():
        raise SystemExit(f"Input CSV not found: {source_csv}")

    rows_by_isin: dict[str, list[dict[str, str]]] = {}

    with source_csv.open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise SystemExit(f"No CSV header found in {source_csv}")

        normalized_headers = {h.strip().lower() for h in reader.fieldnames if h}
        if "isin" not in normalized_headers:
            raise SystemExit(
                "Missing required column 'isin'. "
                f"Found headers: {reader.fieldnames}"
            )

        for row_number, row in enumerate(reader, start=2):
            normalized_row = {
                (k or "").strip().lower(): (v or "").strip()
                for k, v in row.items()
            }

            isin = first_non_empty(normalized_row, ["isin"])
            date_raw = first_non_empty(normalized_row, ["txn_date", "date"])
            price = first_non_empty(normalized_row, ["nav", "price"])
            units = first_non_empty(normalized_row, ["units", "quantity", "qty"])

            if not isin:
                continue
            if not (date_raw and price and units):
                # Skip incomplete lines (for example, tax lines with no units).
                continue

            try:
                date_value = normalize_date(date_raw)
            except ValueError as exc:
                raise SystemExit(f"Row {row_number}: {exc}") from exc

            rows_by_isin.setdefault(isin, []).append(
                {
                    date_header: date_value,
                    price_header: price,
                    units_header: units,
                }
            )

    output_dir.mkdir(parents=True, exist_ok=True)
    written: dict[str, Path] = {}
    output_columns = [date_header, price_header, units_header]

    for isin, rows in rows_by_isin.items():
        rows.sort(key=lambda item: item[date_header])
        filename = f"{sanitize_filename(isin)}.csv"
        output_path = output_dir / filename
        with output_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=output_columns)
            writer.writeheader()
            writer.writerows(rows)
        written[isin] = output_path

    return written


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Split transactions CSV into one file per ISIN with Date, Price, Units."
    )
    parser.add_argument(
        "input_csv",
        nargs="?",
        default="transactions_extracted.csv",
        help="Input CSV path (default: transactions_extracted.csv)",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        default="trade_groups",
        help="Directory for per-ISIN files (default: trade_groups)",
    )
    parser.add_argument("--date-header", default="Date", help="Output date column header.")
    parser.add_argument("--price-header", default="Price", help="Output price column header.")
    parser.add_argument(
        "--units-header",
        default="Units",
        help="Output units column header (example typo: Uints).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    source_csv = Path(args.input_csv).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve()

    written = split_transactions_by_isin(
        source_csv,
        output_dir,
        date_header=args.date_header,
        price_header=args.price_header,
        units_header=args.units_header,
    )

    print(f"Created {len(written)} file(s) in {output_dir}")
    for isin, path in sorted(written.items()):
        print(f"{isin} -> {path}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        raise SystemExit(130)
