#!/usr/bin/env python3
import argparse
import re
import subprocess
import sys
from pathlib import Path


def extract_with_pypdf(pdf_path: Path) -> str:
    try:
        from pypdf import PdfReader
    except Exception:
        from PyPDF2 import PdfReader  # type: ignore

    reader = PdfReader(str(pdf_path))
    parts: list[str] = []
    for page in reader.pages:
        parts.append(page.extract_text() or "")
    return "\n\n".join(parts).strip()


def extract_with_pdftotext_layout(pdf_path: Path) -> str:
    proc = subprocess.run(
        ["pdftotext", "-layout", str(pdf_path), "-"],
        check=True,
        capture_output=True,
        text=True,
    )
    return proc.stdout.strip()


def extract_with_pdftotext_raw(pdf_path: Path) -> str:
    proc = subprocess.run(
        ["pdftotext", "-raw", str(pdf_path), "-"],
        check=True,
        capture_output=True,
        text=True,
    )
    return proc.stdout.strip()


def repair_line_separators(text: str) -> str:
    fixed = text
    # Put common section markers on their own lines.
    fixed = re.sub(r"(Folio No:\s*[^\n\r]+)", r"\n\1\n", fixed)
    fixed = re.sub(r"(Opening Unit Balance:\s*[^\n\r]+)", r"\n\1\n", fixed)
    fixed = re.sub(r"(Date\s+Amount\s+Price\s+Units\s+Transaction)", r"\n\1\n", fixed)
    # CAS rows often get merged like "...Balance20-Oct-2022...". Break before date tokens.
    fixed = re.sub(
        r"(?<!\n)(?=(?:\d{2}-[A-Za-z]{3}-\d{4}))",
        "\n",
        fixed,
    )
    # Collapse excessive blank lines introduced by repairs.
    fixed = re.sub(r"\n{3,}", "\n\n", fixed)
    return fixed.strip()


def extract_text(pdf_path: Path, backend: str = "auto") -> str:
    errors: list[str] = []
    backends = {
        "pdftotext-layout": extract_with_pdftotext_layout,
        "pdftotext-raw": extract_with_pdftotext_raw,
        "pypdf": extract_with_pypdf,
    }
    order = (
        ["pdftotext-layout", "pdftotext-raw", "pypdf"]
        if backend == "auto"
        else [backend]
    )

    for backend_name in order:
        fn = backends[backend_name]
        try:
            return fn(pdf_path)
        except Exception as exc:
            errors.append(f"{backend_name}: {exc}")

    raise RuntimeError(
        "Could not extract PDF text. Tried backends:\n- " + "\n- ".join(errors)
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract text content from a PDF.")
    parser.add_argument("pdf", help="Path to input PDF file.")
    parser.add_argument(
        "--backend",
        choices=["auto", "pdftotext-layout", "pdftotext-raw", "pypdf"],
        default="auto",
        help="Extraction backend preference (default: auto).",
    )
    parser.add_argument(
        "--no-repair-lines",
        action="store_true",
        help="Disable line-separator repair pass.",
    )
    parser.add_argument(
        "-o",
        "--output",
        help="Optional output .txt path. If omitted, prints to stdout.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    pdf_path = Path(args.pdf).expanduser().resolve()
    if not pdf_path.exists():
        print(f"PDF not found: {pdf_path}", file=sys.stderr)
        return 1

    try:
        text = extract_text(pdf_path, backend=args.backend)
        if not args.no_repair_lines:
            text = repair_line_separators(text)
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 2

    if args.output:
        output_path = Path(args.output).expanduser().resolve()
        output_path.write_text(text, encoding="utf-8")
        print(f"Text written to {output_path}")
        return 0

    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
