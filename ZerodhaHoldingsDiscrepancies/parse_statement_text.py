#!/usr/bin/env python3
import argparse
import csv
import re
import subprocess
import sys
from bisect import bisect_right
from datetime import datetime
from pathlib import Path


FOLIO_RE = re.compile(
    r"folio\s*no\s*:?\s*(?P<folio>\d+(?:\s*/\s*[A-Za-z0-9]+)?)",
    re.IGNORECASE,
)
FUND_ISIN_RE = re.compile(
    r"(?:(?:[A-Za-z0-9]+)-)?(?P<fund>[A-Za-z][A-Za-z0-9\s\-/&().]+?)\s*-\s*ISIN:\s*(?P<isin>IN[A-Z\d]{10})\b",
    re.IGNORECASE,
)
DATE_TOKEN_RE = re.compile(r"\d{2}-[A-Za-z]{3}-\d{4}")

# Standard line-based format:
# 08-Mar-2022 Purchase 49,997.50 1,058.148 47.25 1,058.148
TXN_LINE_RE = re.compile(
    r"^(?P<date>\d{2}-[A-Za-z]{3}-\d{4})\s+"
    r"(?P<description>.+?)\s+"
    r"(?P<amount>-?\d[\d,]*\.\d+)"
    r"(?:\s+(?P<units>-?\d[\d,]*\.\d+))?"
    r"(?:\s+(?P<nav>-?\d[\d,]*\.\d+))?"
    r"(?:\s+(?P<balance>-?\d[\d,]*\.\d+))?$"
)

# User-provided regex patterns translated to Python named groups (?P<name>...).
USER_TXN_RE = re.compile(
    r"^((?P<d1>([\d]{2}-[A-Za-z]{3}-[\d]{4})))[\n\r\s]+"
    r"("
    r"((?P<amtsign>[(]{0,1})(?P<amt>(([\d,]+(\.\d{0,5}){0,1})))[)]{0,1}[\n\r\s]+"
    r"(?P<nav>(([\d,]+(\.\d{0,5}){0,1})))[\n\r\s]+"
    r"(?P<unitsign>[(]{0,1})(?P<units>(([\d,]+(\.\d{0,5}){0,1})))[)]{0,1}[\n\r\s]+"
    r"(?P<text>((.+\s)+?))(?P<bal>(([\d,]+(\.\d{0,5}){0,1})))"
    r")"
    r"|"
    r"((?P<sttOrTds>(([\d,]+(\.\d{0,5}){0,1})))[\n\r\s]+"
    r"(.*\s*((?P<isStt>STT)|(?P<isTds>TDS)).*\s*?))"
    r")$",
    re.MULTILINE,
)

USER_ISIN_BLOCK_RE = re.compile(
    r"(?P<isin>(IN[A-Z\d]{10}))([\n]+)"
    r"(?P<name>([\w\n\r\s\#\-\/]+))"
    r"(?P<cur>((\d+(\.\d{3}))|[-]{2}))[\n\r\s]+"
    r"(?P<fro>((\d+(\.\d{3}))|[-]{2}))[\n\r\s]+"
    r"(?P<ple>((\d+(\.\d{3}))|[-]{2}))[\n\r\s]+"
    r"(?P<pel2>((\d+(\.\d{3}))|[-]{2}))[\n\r\s]+"
    r"(?P<fre>((\d+(\.\d{3}))|[-]{2}))[\n\r\s]+"
    r"(?P<val>(\d+(\.\d{2})))",
    re.MULTILINE,
)

# Folio numbers captured earlier in this session.
KNOWN_FOLIOS = [
    "547821936405",
    "283746191",
    "916275824",
]

KNOWN_FOLIO_BY_MAIN: dict[str, str] = {}
for _folio in KNOWN_FOLIOS:
    _m = re.match(r"^\s*(\d{6,})(?:\s*/\s*([0-9]{1,4}))?\s*$", _folio)
    if _m:
        KNOWN_FOLIO_BY_MAIN[_m.group(1)] = _folio


def normalize_whitespace(text: str) -> str:
    return " ".join(text.strip().split())


def build_row(
    fund_name: str,
    isin: str,
    folio_no: str,
    txn_date: str,
    description: str,
    amount: str,
    units: str = "",
    nav: str = "",
    balance_units: str = "",
    raw_line: str = "",
) -> dict[str, str]:
    return {
        "fund_name": fund_name,
        "isin": isin,
        "folio_no": folio_no,
        "txn_date": txn_date,
        "description": description,
        "amount": amount,
        "units": units,
        "nav": nav,
        "balance_units": balance_units,
        "raw_line": raw_line,
    }


def normalize_folio_no(raw_folio: str) -> str:
    text = normalize_whitespace(raw_folio or "")
    if not text:
        return ""

    # Find major folio digits with optional sub-folio digits.
    match = re.search(r"(\d{6,})(?:\s*/\s*([0-9]{1,4}))?", text)
    if not match:
        return text

    main_no = match.group(1)
    sub_no = match.group(2) or ""
    if main_no in KNOWN_FOLIO_BY_MAIN:
        canonical = KNOWN_FOLIO_BY_MAIN[main_no]
        return canonical.replace(" ", "")

    if sub_no:
        return f"{main_no}/{sub_no}"
    return main_no


def to_number(value: str) -> float:
    return float(value.replace(",", ""))


def is_number_token(token: str, min_decimals: int = 2, max_decimals: int = 4) -> bool:
    pattern = rf"^-?\d[\d,]*\.\d{{{min_decimals},{max_decimals}}}$"
    return bool(re.match(pattern, token))


def split_compact_nav_units(prefix: str, amount: str) -> tuple[str, str] | None:
    blob = prefix.replace(" ", "")
    candidates: list[tuple[float, float, float, str, str]] = []
    amount_value = to_number(amount)

    for idx in range(1, len(blob)):
        left = blob[:idx]
        right = blob[idx:]
        if not is_number_token(left, min_decimals=2, max_decimals=4):
            continue
        if not is_number_token(right, min_decimals=2, max_decimals=4):
            continue
        try:
            nav_value = to_number(left)
            units_value = to_number(right)
        except ValueError:
            continue
        if nav_value == 0:
            continue

        expected_units = amount_value / nav_value
        abs_err = abs(units_value - expected_units)
        rel_err = abs_err / max(abs(expected_units), 1e-6)
        candidates.append((rel_err, abs_err, nav_value, left, right))

    if not candidates:
        return None

    candidates.sort(key=lambda x: (x[0], x[1]))
    best = candidates[0]
    return best[3], best[4]


def parse_compact_chunk(
    date_token: str,
    chunk: str,
    fund_name: str,
    isin: str,
    folio_no: str,
    include_stamp_duty: bool,
) -> dict[str, str] | None:
    # Compact CAS OCR/text format chunk after a date token.
    # Example:
    # 4,999.7542.745116.967SIP Purchase - via Distributor2,947.100
    compact = normalize_whitespace(chunk)
    if not compact:
        return None

    amount_match = re.match(r"^(?P<amount>-?\d[\d,]*\.\d{2})(?P<rest>.*)$", compact)
    if not amount_match:
        return None

    amount = amount_match.group("amount")
    rest = amount_match.group("rest").strip()
    if not rest:
        return None

    if "stamp duty" in rest.lower():
        if not include_stamp_duty:
            return None
        return build_row(
            fund_name=fund_name,
            isin=isin,
            folio_no=folio_no,
            txn_date=date_token,
            description=rest,
            amount=amount,
            raw_line=f"{date_token}{compact}",
        )

    # Compact numeric-first layout often arrives without separators:
    # <amount><nav><units><description><balance>
    blob_match = re.match(
        r"^(?P<prefix>[\d,.\-]+)(?P<description>[A-Za-z*].*?)(?P<balance>-?\d[\d,]*\.\d{2,4})\s*$",
        rest,
    )
    if blob_match:
        prefix = blob_match.group("prefix")
        nav_units = split_compact_nav_units(prefix, amount)
        if nav_units is None:
            return None
        nav, units = nav_units
        return build_row(
            fund_name=fund_name,
            isin=isin,
            folio_no=folio_no,
            txn_date=date_token,
            description=blob_match.group("description").strip(),
            amount=amount,
            units=units,
            nav=nav,
            balance_units=blob_match.group("balance"),
            raw_line=f"{date_token}{compact}",
        )

    # Space-delimited variant fallback:
    # <amount> <nav> <units> <description> <balance>
    full_match = re.match(
        r"^(?P<nav>-?\d[\d,]*\.\d{2,4})\s+"
        r"(?P<units>-?\d[\d,]*\.\d{2,4})\s+"
        r"(?P<description>.+?)\s+"
        r"(?P<balance>-?\d[\d,]*\.\d{2,4})\s*$",
        rest,
    )
    if full_match:
        return build_row(
            fund_name=fund_name,
            isin=isin,
            folio_no=folio_no,
            txn_date=date_token,
            description=full_match.group("description").strip(),
            amount=amount,
            units=full_match.group("units"),
            nav=full_match.group("nav"),
            balance_units=full_match.group("balance"),
            raw_line=f"{date_token}{compact}",
        )

    # Fallback:
    # <amount><description>
    return build_row(
        fund_name=fund_name,
        isin=isin,
        folio_no=folio_no,
        txn_date=date_token,
        description=rest,
        amount=amount,
        raw_line=f"{date_token}{compact}",
    )


def parse_compact_date_stream(
    line: str,
    fund_name: str,
    isin: str,
    folio_no: str,
    include_stamp_duty: bool,
) -> list[dict[str, str]]:
    matches = list(DATE_TOKEN_RE.finditer(line))
    if not matches:
        return []

    rows: list[dict[str, str]] = []
    for idx, match in enumerate(matches):
        date_token = match.group(0)
        start = match.end()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(line)
        chunk = line[start:end]
        row = parse_compact_chunk(
            date_token=date_token,
            chunk=chunk,
            fund_name=fund_name,
            isin=isin,
            folio_no=folio_no,
            include_stamp_duty=include_stamp_duty,
        )
        if row:
            rows.append(row)
    return rows


def extract_positioned_metadata(text: str) -> tuple[list[int], list[tuple[str, str, str]]]:
    points: list[tuple[int, str, str, str]] = []
    current_folio = ""
    current_fund = ""
    current_isin = ""

    cursor = 0
    for raw_line in text.splitlines(keepends=True):
        line = normalize_whitespace(raw_line)
        folio_match = FOLIO_RE.search(line)
        if folio_match:
            current_folio = folio_match.group("folio").strip()

        fund_match = FUND_ISIN_RE.search(line)
        if fund_match:
            current_fund = fund_match.group("fund").strip(" -")
            current_isin = fund_match.group("isin").strip()

        points.append((cursor, current_fund, current_isin, current_folio))
        cursor += len(raw_line)

    for match in USER_ISIN_BLOCK_RE.finditer(text):
        isin = (match.group("isin") or "").strip()
        name = normalize_whitespace(match.group("name") or "")
        if name and isin:
            points.append((match.start(), name, isin, current_folio))

    points.sort(key=lambda x: x[0])
    positions = [p[0] for p in points]
    values = [(p[1], p[2], p[3]) for p in points]
    return positions, values


def metadata_for_pos(
    positions: list[int], values: list[tuple[str, str, str]], pos: int
) -> tuple[str, str, str]:
    if not positions:
        return "", "", ""
    idx = bisect_right(positions, pos) - 1
    if idx < 0:
        return "", "", ""
    return values[idx]


def parse_text_user_regex(text: str, include_stamp_duty: bool) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    positions, values = extract_positioned_metadata(text)

    for match in USER_TXN_RE.finditer(text):
        fund_name, isin, folio_no = metadata_for_pos(positions, values, match.start())
        txn_date = (match.group("d1") or "").strip()
        amount = (match.group("amt") or match.group("sttOrTds") or "").strip()
        nav = (match.group("nav") or "").strip()
        units = (match.group("units") or "").strip()
        balance = (match.group("bal") or "").strip()
        desc = (match.group("text") or "").strip()

        if not desc:
            if match.group("isStt"):
                desc = "STT"
            elif match.group("isTds"):
                desc = "TDS"
            else:
                desc = "Tax/Duty"

        if not include_stamp_duty and "stamp duty" in desc.lower():
            continue

        rows.append(
            build_row(
                fund_name=fund_name,
                isin=isin,
                folio_no=folio_no,
                txn_date=txn_date,
                description=normalize_whitespace(desc),
                amount=amount,
                units=units,
                nav=nav,
                balance_units=balance,
                raw_line=normalize_whitespace(match.group(0)),
            )
        )
    return rows


def parse_text_heuristic(text: str, include_stamp_duty: bool) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    current_folio = ""
    current_fund = ""
    current_isin = ""

    for raw_line in text.splitlines():
        line = normalize_whitespace(raw_line)
        if not line:
            continue

        folio_match = FOLIO_RE.search(line)
        if folio_match:
            current_folio = folio_match.group("folio").strip()
            continue

        fund_match = FUND_ISIN_RE.search(line)
        if fund_match:
            current_fund = fund_match.group("fund").strip(" -")
            current_isin = fund_match.group("isin").strip()
            continue

        # 1) Try standard one-row-per-line parser first.
        txn_match = TXN_LINE_RE.match(line)
        if txn_match:
            description = txn_match.group("description").strip()
            if include_stamp_duty or "stamp duty" not in description.lower():
                rows.append(
                    build_row(
                        fund_name=current_fund,
                        isin=current_isin,
                        folio_no=current_folio,
                        txn_date=txn_match.group("date"),
                        description=description,
                        amount=txn_match.group("amount") or "",
                        units=txn_match.group("units") or "",
                        nav=txn_match.group("nav") or "",
                        balance_units=txn_match.group("balance") or "",
                        raw_line=line,
                    )
                )
            continue

        # 2) Fallback for compact stream lines with many date tokens.
        compact_rows = parse_compact_date_stream(
            line=line,
            fund_name=current_fund,
            isin=current_isin,
            folio_no=current_folio,
            include_stamp_duty=include_stamp_duty,
        )
        if compact_rows:
            rows.extend(compact_rows)

    return rows


def parse_text(text: str, include_stamp_duty: bool, parser_mode: str = "auto") -> list[dict[str, str]]:
    if parser_mode == "user-regex":
        return parse_text_user_regex(text, include_stamp_duty=include_stamp_duty)
    if parser_mode == "heuristic":
        return parse_text_heuristic(text, include_stamp_duty=include_stamp_duty)

    # auto: prefer user-regex when it produces rows, else fallback
    regex_rows = parse_text_user_regex(text, include_stamp_duty=include_stamp_duty)
    if regex_rows:
        return regex_rows
    return parse_text_heuristic(text, include_stamp_duty=include_stamp_duty)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Parse mutual fund statement text/PDF and extract transaction rows."
    )
    parser.add_argument("input_file", help="Path to input statement file (.txt or .pdf)")
    parser.add_argument(
        "-o",
        "--output",
        default="transactions_extracted.csv",
        help="Output CSV path (default: transactions_extracted.csv)",
    )
    parser.add_argument(
        "--include-stamp-duty",
        action="store_true",
        help="Include rows whose description contains 'Stamp Duty'.",
    )
    parser.add_argument(
        "--parser",
        choices=["auto", "user-regex", "heuristic"],
        default="auto",
        help="Parser backend: auto (default), user-regex, or heuristic.",
    )
    parser.add_argument("--fund-name", default="", help="Fallback fund name if not found in text.")
    parser.add_argument("--isin", default="", help="Fallback ISIN if not found in text.")
    parser.add_argument("--folio-no", default="", help="Fallback folio number if not found in text.")
    parser.add_argument(
        "--allow-incomplete",
        action="store_true",
        help="Keep rows even when units/nav/balance_units are missing.",
    )
    return parser.parse_args()


def normalize_numeric_output(value: str) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    # Convert accounting negatives like (123.45) -> -123.45
    if text.startswith("(") and text.endswith(")"):
        text = f"-{text[1:-1]}"
    return text.replace(",", "")


def normalize_date_output(value: str) -> str:
    text = (value or "").strip()
    if not text:
        return ""
    for fmt in ("%d-%b-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return text


def main() -> int:
    args = parse_args()
    input_path = Path(args.input_file).expanduser().resolve()
    output_path = Path(args.output).expanduser().resolve()

    if not input_path.exists():
        print(f"Input file not found: {input_path}", file=sys.stderr)
        return 1

    try:
        text = read_statement_text(input_path)
    except Exception as exc:
        print(f"Could not read input text from {input_path}: {exc}", file=sys.stderr)
        return 2

    rows = parse_text(
        text,
        include_stamp_duty=args.include_stamp_duty,
        parser_mode=args.parser,
    )
    for row in rows:
        row["folio_no"] = normalize_folio_no(row.get("folio_no", ""))
        if not row["fund_name"] and args.fund_name:
            row["fund_name"] = args.fund_name
        if not row["isin"] and args.isin:
            row["isin"] = args.isin
        if not row["folio_no"] and args.folio_no:
            row["folio_no"] = normalize_folio_no(args.folio_no)
        row["txn_date"] = normalize_date_output(row.get("txn_date", ""))
        row["amount"] = normalize_numeric_output(row.get("amount", ""))
        row["units"] = normalize_numeric_output(row.get("units", ""))
        row["nav"] = normalize_numeric_output(row.get("nav", ""))
        row["balance_units"] = normalize_numeric_output(row.get("balance_units", ""))

    if not args.allow_incomplete:
        rows = [
            row
            for row in rows
            if row.get("units", "").strip()
            and row.get("nav", "").strip()
            and row.get("balance_units", "").strip()
        ]

    fieldnames = [
        "isin",
        "folio_no",
        "txn_date",
        "amount",
        "units",
        "nav",
        "balance_units",
    ]
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=fieldnames,
            extrasaction="ignore",
            quoting=csv.QUOTE_NONE,
            escapechar="\\",
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"Extracted {len(rows)} transactions to {output_path}")
    if len(rows) == 0:
        print(
            "No transactions matched. If this is a scanned PDF/image statement, run OCR first.",
            file=sys.stderr,
        )
    return 0


def extract_text_from_pdf(pdf_path: Path) -> str:
    errors: list[str] = []

    # Prefer layout-preserving extraction for CAS statements.
    try:
        proc = subprocess.run(
            ["pdftotext", "-layout", str(pdf_path), "-"],
            check=True,
            capture_output=True,
            text=True,
        )
        text = proc.stdout.strip()
        if text:
            return text
        errors.append("pdftotext returned empty text")
    except Exception as exc:
        errors.append(f"pdftotext: {exc}")

    try:
        try:
            from pypdf import PdfReader
        except Exception:
            from PyPDF2 import PdfReader  # type: ignore

        reader = PdfReader(str(pdf_path))
        parts: list[str] = []
        for page in reader.pages:
            parts.append(page.extract_text() or "")
        text = "\n\n".join(parts).strip()
        if text:
            return text
        errors.append("pypdf/PyPDF2 returned empty text")
    except Exception as exc:
        errors.append(f"pypdf/PyPDF2: {exc}")

    raise RuntimeError("; ".join(errors))


def read_statement_text(input_path: Path) -> str:
    if input_path.suffix.lower() == ".pdf":
        return extract_text_from_pdf(input_path)
    return input_path.read_text(encoding="utf-8", errors="ignore")


if __name__ == "__main__":
    raise SystemExit(main())
