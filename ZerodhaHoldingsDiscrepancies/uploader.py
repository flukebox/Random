import argparse
import csv
import json
import re
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Iterable

from playwright.sync_api import Locator
from playwright.sync_api import Page
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

DEFAULT_HOLDINGS_URL = "https://console.zerodha.com/portfolio/holdings"

FUND_REGISTRY = {
    "sbi": {"csv": "trade_groups/SBI.csv", "display_name": "SBI Focused Equity Fund"},
    "test": {"csv": "trade_groups/INF846K01DP8.csv", "display_name": "Test Fund"},
}

REPORT_COLUMNS = [
    "timestamp",
    "fund",
    "row_number",
    "date",
    "price",
    "quantity",
    "status",
    "reason",
    "url_used",
]


@dataclass(frozen=True)
class Trade:
    row_number: int
    date: datetime
    date_str: str
    price: Decimal
    quantity: Decimal
    price_key: str
    quantity_key: str

    @property
    def duplicate_key(self) -> tuple[str, str, str]:
        return (self.date_str, self.price_key, self.quantity_key)


def parse_bool(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "y"}:
        return True
    if normalized in {"0", "false", "no", "n"}:
        return False
    raise argparse.ArgumentTypeError("Expected true/false.")


def decimal_to_key(value: Decimal) -> str:
    normalized = value.normalize()
    as_text = format(normalized, "f")
    if "." in as_text:
        as_text = as_text.rstrip("0").rstrip(".")
    if as_text in {"", "-0"}:
        return "0"
    return as_text


def parse_decimal(raw_value: str, field_name: str) -> Decimal:
    cleaned = str(raw_value).strip().replace(",", "")
    if not cleaned or cleaned in {"-", "–"}:
        raise ValueError(f"Missing numeric value for {field_name}.")
    try:
        value = Decimal(cleaned)
    except InvalidOperation as exc:
        raise ValueError(f"Invalid decimal for {field_name}: {raw_value!r}") from exc
    if not value.is_finite():
        raise ValueError(f"Non-finite decimal for {field_name}: {raw_value!r}")
    return value


def parse_trade_date(raw_value: str) -> datetime:
    try:
        return datetime.strptime(raw_value.strip(), "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"Invalid Date (expected YYYY-MM-DD): {raw_value!r}") from exc


def load_csv_trades(csv_path: Path) -> list[Trade]:
    if not csv_path.exists():
        raise SystemExit(f"CSV file not found: {csv_path}")

    with csv_path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        required = {"Date", "Price", "Units"}
        available = set(reader.fieldnames or [])
        missing = sorted(required - available)
        if missing:
            raise SystemExit(
                f"Missing required CSV columns {missing} in {csv_path}. Found: {reader.fieldnames}"
            )

        parsed_rows = []
        for source_row_number, row in enumerate(reader, start=2):
            try:
                date_value = parse_trade_date(row["Date"])
                price_value = parse_decimal(row["Price"], "Price")
                quantity_value = parse_decimal(row["Units"], "Units")
            except ValueError as exc:
                raise SystemExit(f"CSV parse error at row {source_row_number}: {exc}") from exc

            parsed_rows.append(
                {
                    "source_row_number": source_row_number,
                    "date_value": date_value,
                    "price_value": price_value,
                    "quantity_value": quantity_value,
                }
            )

    parsed_rows.sort(key=lambda item: (item["date_value"], item["source_row_number"]))

    # Consolidate same-day trades:
    # quantity = sum(units), price = weighted average by units.
    per_date: dict[str, dict[str, Decimal]] = {}
    for item in parsed_rows:
        date_key = item["date_value"].strftime("%Y-%m-%d")
        qty = item["quantity_value"]
        price = item["price_value"]

        bucket = per_date.setdefault(
            date_key, {"total_qty": Decimal("0"), "total_notional": Decimal("0")}
        )
        bucket["total_qty"] += qty
        bucket["total_notional"] += (price * qty)

    trades: list[Trade] = []
    for sorted_row_number, date_key in enumerate(sorted(per_date.keys()), start=1):
        total_qty = per_date[date_key]["total_qty"]
        total_notional = per_date[date_key]["total_notional"]
        if total_qty <= 0:
            raise SystemExit(
                f"Invalid consolidated quantity ({total_qty}) on {date_key} in {csv_path}"
            )

        weighted_price = (total_notional / total_qty).quantize(
            Decimal("0.0001"), rounding=ROUND_HALF_UP
        )
        date_value = datetime.strptime(date_key, "%Y-%m-%d")
        trades.append(
            Trade(
                row_number=sorted_row_number,
                date=date_value,
                date_str=date_key,
                price=weighted_price,
                quantity=total_qty,
                price_key=decimal_to_key(weighted_price),
                quantity_key=decimal_to_key(total_qty),
            )
        )
    return trades


def split_transactions_extracted_by_isin(
    source_csv: Path,
    output_dir: Path,
    *,
    date_header: str = "Date",
    price_header: str = "Price",
    units_header: str = "Units",
) -> dict[str, Path]:
    """
    Split a transactions_extracted-style CSV into one file per ISIN.

    Expected source columns (case-insensitive):
    - isin
    - txn_date or date
    - nav or price
    - units or quantity or qty

    Output files are written as <ISIN>.csv with only Date, Price, Units columns
    (header labels configurable via keyword args).
    """
    if not source_csv.exists():
        raise SystemExit(f"CSV file not found: {source_csv}")

    rows_by_isin: dict[str, list[dict[str, str]]] = {}

    with source_csv.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise SystemExit(f"No CSV header found in {source_csv}")

        normalized_headers = {header.strip().lower() for header in reader.fieldnames if header}
        if "isin" not in normalized_headers:
            raise SystemExit(
                f"Missing required CSV column ['isin'] in {source_csv}. "
                f"Found: {reader.fieldnames}"
            )

        for source_row_number, row in enumerate(reader, start=2):
            normalized_row = {(key or "").strip().lower(): (value or "").strip() for key, value in row.items()}
            isin = normalized_row.get("isin", "")
            date_value = normalized_row.get("txn_date", "") or normalized_row.get("date", "")
            price_value = normalized_row.get("nav", "") or normalized_row.get("price", "")
            units_value = (
                normalized_row.get("units", "")
                or normalized_row.get("quantity", "")
                or normalized_row.get("qty", "")
            )

            if not isin:
                continue

            # Skip incomplete rows (for example: tax/stamp duty lines with empty units).
            if not (date_value and price_value and units_value):
                continue

            # Keep date format consistent for downstream uploader expectations.
            try:
                formatted_date = parse_trade_date(date_value).strftime("%Y-%m-%d")
            except ValueError as exc:
                raise SystemExit(
                    f"CSV parse error at row {source_row_number}: {exc}"
                ) from exc

            rows_by_isin.setdefault(isin, []).append(
                {
                    date_header: formatted_date,
                    price_header: price_value,
                    units_header: units_value,
                }
            )

    output_dir.mkdir(parents=True, exist_ok=True)
    written_files: dict[str, Path] = {}
    output_columns = [date_header, price_header, units_header]

    for isin, rows in rows_by_isin.items():
        rows.sort(key=lambda item: item[date_header])
        output_path = output_dir / f"{isin}.csv"
        with output_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=output_columns)
            writer.writeheader()
            writer.writerows(rows)
        written_files[isin] = output_path

    return written_files


def load_url_config(config_path: Path) -> dict[str, str]:
    if not config_path.exists():
        return {}
    try:
        with config_path.open(encoding="utf-8") as handle:
            data = json.load(handle)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Invalid JSON in {config_path}: {exc}") from exc

    if not isinstance(data, dict):
        raise SystemExit(f"URL config {config_path} must be a JSON object.")

    normalized: dict[str, str] = {}
    for key, value in data.items():
        if not isinstance(key, str):
            continue
        if value is None:
            continue
        if not isinstance(value, str):
            raise SystemExit(f"URL config value for {key!r} must be a string.")
        url = value.strip()
        if url:
            normalized[key.strip().lower()] = url
    return normalized


def prompt_choice(prompt: str, allowed: Iterable[str]) -> str:
    allowed_set = {a.lower() for a in allowed}
    while True:
        try:
            response = input(prompt).strip().lower()
        except EOFError:
            return "a" if "a" in allowed_set else next(iter(allowed_set))
        if response in allowed_set:
            return response
        print(f"Please enter one of: {', '.join(sorted(allowed_set))}")


def safe_locator_count(locator: Locator) -> int:
    try:
        return locator.count()
    except Exception:
        return 0


def wait_for_min_locator_count(page: Page, locator: Locator, min_count: int, timeout_ms: int) -> bool:
    deadline = datetime.now().timestamp() + (timeout_ms / 1000)
    while datetime.now().timestamp() < deadline:
        if safe_locator_count(locator) >= min_count:
            return True
        page.wait_for_timeout(150)
    return safe_locator_count(locator) >= min_count


def is_login_page(page: Page) -> bool:
    url = page.url.lower()
    if "login" in url:
        return True
    if "kite.zerodha.com" in url and "console.zerodha.com" not in url:
        return True
    user_id_candidates = [
        "input[name='user_id']",
        "input#userid",
        "input[autocomplete='username']",
    ]
    for selector in user_id_candidates:
        if safe_locator_count(page.locator(selector)) > 0:
            return True
    return False


def set_input_value(input_el: Locator, value: str) -> None:
    input_el.click()
    input_el.press("ControlOrMeta+A")
    input_el.fill(value)


def fill_date_and_close_picker(page: Page, date_input: Locator, date_value: str) -> None:
    set_input_value(date_input, date_value)
    # On Zerodha, Escape can close the entire "Add trade" dialog.
    # Use blur/tab only so the date picker closes without dismissing the modal.
    date_input.press("Tab")
    page.wait_for_timeout(100)


def select_trade_type_others(page: Page, dialog: Locator) -> None:
    # Native <select> path.
    type_select = dialog.locator("select").first
    if safe_locator_count(type_select) > 0:
        try:
            type_select.select_option(label="Others")
            return
        except Exception:
            options = type_select.locator("option")
            option_count = safe_locator_count(options)
            for idx in range(option_count):
                label = (options.nth(idx).inner_text() or "").strip().lower()
                value = (options.nth(idx).get_attribute("value") or "").strip()
                if "other" in label and value:
                    type_select.select_option(value=value)
                    return

    # Custom dropdown path (matches Zerodha modal).
    type_trigger_candidates = [
        dialog.locator("label:has-text('Type')").first.locator("xpath=following::div[1]"),
        dialog.locator("text=Select type").first,
        dialog.locator("div:has-text('Select type')").first,
    ]
    for trigger in type_trigger_candidates:
        if safe_locator_count(trigger) == 0:
            continue
        try:
            trigger.click(timeout=3000)
            break
        except Exception:
            continue

    option_candidates = [
        page.get_by_role("option", name=re.compile(r"^others$", re.I)).first,
        page.locator("li:has-text('Others')").first,
        page.locator("div:has-text('Others')").first,
        page.get_by_text(re.compile(r"^Others$", re.I)).first,
    ]
    for option in option_candidates:
        if safe_locator_count(option) == 0:
            continue
        try:
            option.wait_for(state="visible", timeout=3000)
            option.click(timeout=3000)
            return
        except Exception:
            continue

    raise RuntimeError("Could not select trade type 'Others'.")


def wait_for_open_trade_form(page: Page, timeout_ms: int) -> Locator | None:
    # Primary anchor: modal title contains "Add a trade".
    titled_form = page.locator(
        "xpath=(//*[self::h1 or self::h2 or self::h3]"
        "[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'add a trade')]"
        "/ancestor::*[.//button[normalize-space()='Add'] and .//input[@placeholder='YYYY-MM-DD']][1])[last()]"
    ).first
    try:
        titled_form.wait_for(state="visible", timeout=timeout_ms)
        return titled_form
    except Exception:
        pass

    # Fallback if heading structure changes.
    fallback_form = page.locator(
        "xpath=(//*[.//input[@placeholder='YYYY-MM-DD' and not(@type='hidden')] "
        "and count(.//input[not(@type='hidden')]) >= 3 "
        "and .//button[normalize-space()='Add']])[last()]"
    ).first
    try:
        fallback_form.wait_for(state="visible", timeout=timeout_ms)
        return fallback_form
    except Exception:
        return None


def open_add_trade_form(page: Page) -> Locator:
    existing_form = wait_for_open_trade_form(page, 400)
    if existing_form is not None:
        return existing_form

    # Keep first strategy identical to the earlier working approach.
    direct_role_button = page.get_by_role("button", name=re.compile(r"^\+?\s*Add trade$", re.I)).first
    if safe_locator_count(direct_role_button) > 0:
        try:
            direct_role_button.click(timeout=5000)
            opened_form = wait_for_open_trade_form(page, 7000)
            if opened_form is not None:
                return opened_form
        except Exception:
            pass

    candidates = [
        page.locator("h2.add-trade-container button.add-external-trade:visible"),
        page.locator("button.add-external-trade:visible"),
        page.get_by_role("button", name=re.compile(r"add\s*trade", re.I)),
        page.locator("button:has-text('Add trade')"),
        page.locator("[role='button']:has-text('Add trade')"),
        page.locator("a:has-text('Add trade')"),
        page.get_by_text(re.compile(r"\+?\s*Add trade", re.I)),
    ]

    for locator in candidates:
        count = safe_locator_count(locator)
        if count == 0:
            continue
        for idx in range(count):
            control = locator.nth(idx)
            try:
                control.scroll_into_view_if_needed(timeout=2000)
            except Exception:
                pass

            clicked = False
            try:
                control.click(timeout=4000)
                clicked = True
            except Exception:
                try:
                    control.click(timeout=4000, force=True)
                    clicked = True
                except Exception:
                    try:
                        control.evaluate("el => el.click()")
                        clicked = True
                    except Exception:
                        clicked = False

            if not clicked:
                continue

            opened_form = wait_for_open_trade_form(page, 5000)
            if opened_form is not None:
                return opened_form

    raise RuntimeError(
        "Could not open Add trade form. '+ Add trade' control was not clickable or did not open the form."
    )


def fill_and_submit_add_trade_form(page: Page, form: Locator, trade: Trade) -> None:
    date_input = form.locator("input[placeholder='YYYY-MM-DD']:visible").first
    date_input.wait_for(state="visible", timeout=6000)

    fill_date_and_close_picker(page, date_input, trade.date_str)
    form.wait_for(state="visible", timeout=3000)

    # Use label-driven selectors to match the earlier known-working script intent.
    avg_price_input = form.locator(
        "xpath=.//label[contains(normalize-space(.), 'Avg. price')]/following::input[1]"
    ).first
    qty_input = form.locator(
        "xpath=.//label[contains(normalize-space(.), 'Qty.') or contains(normalize-space(.), 'Qty')]/following::input[1]"
    ).first

    has_label_inputs = safe_locator_count(avg_price_input) > 0 and safe_locator_count(qty_input) > 0
    if has_label_inputs:
        avg_price_input.wait_for(state="visible", timeout=4000)
        qty_input.wait_for(state="visible", timeout=4000)
        set_input_value(avg_price_input, decimal_to_key(trade.price))
        set_input_value(qty_input, decimal_to_key(trade.quantity))
    else:
        # Fallback to visible non-date inputs when label association is unavailable.
        non_date_inputs = form.locator(
            "input:visible:not([type='hidden']):not([placeholder='YYYY-MM-DD'])"
        )
        if not wait_for_min_locator_count(page, non_date_inputs, 2, 6000):
            raise RuntimeError(
                "Add trade form opened, but could not locate Avg. price and Qty inputs."
            )
        set_input_value(non_date_inputs.nth(0), decimal_to_key(trade.price))
        set_input_value(non_date_inputs.nth(1), decimal_to_key(trade.quantity))

    select_trade_type_others(page, form)
    page.wait_for_timeout(2000)
    # Match exact submit button from Zerodha markup:
    # <button type="submit" class="btn-blue">Add</button>
    add_button = form.locator("button[type='submit'].btn-blue:visible").first
    if safe_locator_count(add_button) == 0:
        add_button = form.locator("button[type='submit']:visible").first
    if safe_locator_count(add_button) == 0:
        add_button = form.locator("button:visible", has_text=re.compile(r"^Add$")).first

    clicked = False
    try:
        add_button.click(timeout=5000)
        clicked = True
    except Exception:
        try:
            add_button.click(timeout=5000, force=True)
            clicked = True
        except Exception:
            try:
                add_button.evaluate("el => el.click()")
                clicked = True
            except Exception:
                clicked = False

    if not clicked:
        raise RuntimeError("Could not click submit Add button.")

    try:
        form.wait_for(state="hidden", timeout=9000)
    except PlaywrightTimeoutError:
        add_button.click(timeout=5000)
        form.wait_for(state="hidden", timeout=9000)


def parse_date_from_text(value: str) -> datetime | None:
    text = value.strip()
    if not text:
        return None
    match_yyyy_mm_dd = re.search(r"\b\d{4}-\d{2}-\d{2}\b", text)
    if match_yyyy_mm_dd:
        return datetime.strptime(match_yyyy_mm_dd.group(0), "%Y-%m-%d")
    match_dd_mm_yyyy = re.search(r"\b\d{2}/\d{2}/\d{4}\b", text)
    if match_dd_mm_yyyy:
        return datetime.strptime(match_dd_mm_yyyy.group(0), "%d/%m/%Y")
    match_dd_mm_yy = re.search(r"\b\d{2}/\d{2}/\d{2}\b", text)
    if match_dd_mm_yy:
        return datetime.strptime(match_dd_mm_yy.group(0), "%d/%m/%y")
    return None


def parse_decimal_from_text(value: str) -> Decimal | None:
    text = value.replace(",", "")
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if not match:
        return None
    try:
        return Decimal(match.group(0))
    except InvalidOperation:
        return None


def find_header_index(headers: list[str], candidates: list[str]) -> int | None:
    for idx, header in enumerate(headers):
        normalized = re.sub(r"\s+", " ", header.strip().lower())
        for candidate in candidates:
            if candidate in normalized:
                return idx
    return None


def choose_target_table(page: Page) -> Locator:
    tables = page.locator("table")
    table_count = safe_locator_count(tables)
    if table_count == 0:
        raise RuntimeError("No table found on the page.")

    best_index = 0
    best_score = -1
    for idx in range(table_count):
        score = safe_locator_count(tables.nth(idx).locator("tbody tr"))
        if score > best_score:
            best_score = score
            best_index = idx
    return tables.nth(best_index)


def key_from_row_cells_with_heuristics(cell_texts: list[str]) -> tuple[str, str, str] | None:
    date_value: datetime | None = None
    numeric_values: list[Decimal] = []

    for text in cell_texts:
        parsed_date = parse_date_from_text(text)
        if parsed_date is not None and date_value is None:
            date_value = parsed_date
            # Avoid treating date parts like 2020/03/13 as numeric trade values.
            continue

        parsed_decimal = parse_decimal_from_text(text)
        if parsed_decimal is not None:
            numeric_values.append(parsed_decimal)

    if date_value is None or len(numeric_values) < 2:
        return None

    price_value = numeric_values[-2]
    quantity_value = numeric_values[-1]
    return (
        date_value.strftime("%Y-%m-%d"),
        decimal_to_key(price_value),
        decimal_to_key(quantity_value),
    )


def scrape_existing_trade_keys(page: Page) -> set[tuple[str, str, str]]:
    add_trade_button = page.locator(
        "button.add-external-trade, h2.add-trade-container button.add-external-trade"
    ).first
    try:
        add_trade_button.wait_for(timeout=15000)
    except PlaywrightTimeoutError:
        page.get_by_role("button", name=re.compile(r"add trade", re.I)).first.wait_for(
            timeout=15000
        )

    target_table = choose_target_table(page)
    header_cells = target_table.locator("thead th")
    header_count = safe_locator_count(header_cells)
    headers = [header_cells.nth(i).inner_text().strip() for i in range(header_count)] if header_count else []
    date_index = find_header_index(headers, ["date"]) if headers else None
    price_index = find_header_index(headers, ["price", "nav"]) if headers else None
    quantity_index = find_header_index(headers, ["qty", "quantity", "units", "unit"]) if headers else None
    use_header_mapping = (
        date_index is not None
        and price_index is not None
        and quantity_index is not None
        and len({date_index, price_index, quantity_index}) == 3
    )

    rows = target_table.locator("tbody tr")
    row_count = safe_locator_count(rows)
    keys: set[tuple[str, str, str]] = set()

    if row_count == 0:
        return keys

    for row_idx in range(row_count):
        cells = rows.nth(row_idx).locator("td")
        cell_count = safe_locator_count(cells)
        if cell_count == 0:
            continue

        key: tuple[str, str, str] | None = None
        if use_header_mapping and max(date_index, price_index, quantity_index) < cell_count:
            date_text = cells.nth(date_index).inner_text().strip()
            price_text = cells.nth(price_index).inner_text().strip()
            quantity_text = cells.nth(quantity_index).inner_text().strip()

            date_value = parse_date_from_text(date_text)
            price_value = parse_decimal_from_text(price_text)
            quantity_value = parse_decimal_from_text(quantity_text)

            if date_value is not None and price_value is not None and quantity_value is not None:
                key = (
                    date_value.strftime("%Y-%m-%d"),
                    decimal_to_key(price_value),
                    decimal_to_key(quantity_value),
                )

        if key is None:
            cell_texts = [cells.nth(i).inner_text().strip() for i in range(cell_count)]
            key = key_from_row_cells_with_heuristics(cell_texts)

        if key is None:
            continue
        keys.add(key)
    return keys


def get_existing_trade_keys_with_prompt(page: Page) -> set[tuple[str, str, str]]:
    while True:
        try:
            return scrape_existing_trade_keys(page)
        except Exception as exc:
            print(f"\nCould not read existing trades: {exc}")
            action = prompt_choice(
                "Choose action: [r]etry snapshot, [s]kip duplicate snapshot, [a]bort: ",
                {"r", "s", "a"},
            )
            if action == "r":
                continue
            if action == "s":
                return set()
            raise SystemExit("Aborted by user before upload.")


def add_trade(page: Page, trade: Trade) -> None:
    # Task 1: open "+ Add trade" dialog.
    form = open_add_trade_form(page)
    # Task 2: fill dialog and submit with "Add".
    fill_and_submit_add_trade_form(page, form, trade)


def ensure_logged_in_and_ready(
    page: Page,
    context,
    state_file: Path,
    initial_url: str,
    headless: bool,
) -> None:
    page.goto(initial_url, wait_until="domcontentloaded")
    if not is_login_page(page):
        return

    if headless:
        raise SystemExit(
            "Session is not valid and --headless=true cannot complete manual login. "
            "Run with --headless=false once to refresh storage state."
        )

    print("\nSession is missing/expired. Please login in the opened browser window.")
    input("After login and the page is fully loaded, press Enter to continue...")
    context.storage_state(path=str(state_file))
    page.goto(initial_url, wait_until="domcontentloaded")
    if is_login_page(page):
        raise SystemExit("Still on login page after manual login. Aborting.")


def write_report_row(writer: csv.DictWriter, fund: str, trade: Trade, status: str, reason: str, url_used: str):
    writer.writerow(
        {
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "fund": fund,
            "row_number": trade.row_number,
            "date": trade.date_str,
            "price": trade.price_key,
            "quantity": trade.quantity_key,
            "status": status,
            "reason": reason,
            "url_used": url_used,
        }
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Upload Zerodha MF trades for one fund.")
    parser.add_argument(
        "--fund",
        required=True,
        choices=sorted(FUND_REGISTRY.keys()),
        help="Fund key to upload (single-fund run).",
    )
    parser.add_argument(
        "--csv",
        default=None,
        help="Optional CSV file path override. Defaults to mapped trade_groups/<Fund>.csv.",
    )
    parser.add_argument(
        "--headless",
        type=parse_bool,
        default=False,
        help="Run browser in headless mode (true/false). Default: false.",
    )
    parser.add_argument(
        "--state-file",
        default="storage_state.json",
        help="Path to Playwright storage state file.",
    )
    parser.add_argument(
        "--url-config",
        default="fund_urls.json",
        help="Path to fund URL JSON config.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    fund_key = args.fund.lower()
    fund_config = FUND_REGISTRY[fund_key]

    default_csv = Path(fund_config["csv"])
    csv_path = Path(args.csv) if args.csv else default_csv
    state_file = Path(args.state_file)
    url_config_path = Path(args.url_config)
    url_config = load_url_config(url_config_path)
    configured_url = url_config.get(fund_key)

    trades = load_csv_trades(csv_path)
    if not trades:
        raise SystemExit(f"No trade rows found in {csv_path}")

    report_dir = Path("run_reports")
    report_dir.mkdir(exist_ok=True)
    report_name = f"upload_{fund_key}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    report_path = report_dir / report_name

    print(f"Fund: {fund_key} ({fund_config['display_name']})")
    print(f"Input CSV: {csv_path}")
    print(f"Trades to process: {len(trades)}")
    print(f"Report: {report_path}")

    counters: Counter[str] = Counter()
    total_input_quantity = sum((trade.quantity for trade in trades), Decimal("0"))
    uploaded_quantity = Decimal("0")
    skipped_duplicate_quantity = Decimal("0")
    skipped_manual_quantity = Decimal("0")
    aborted_at_row: int | None = None

    with report_path.open("w", newline="", encoding="utf-8") as report_handle:
        writer = csv.DictWriter(report_handle, fieldnames=REPORT_COLUMNS)
        writer.writeheader()
        report_handle.flush()

        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=args.headless)

            context_kwargs = {}
            if state_file.exists():
                context_kwargs["storage_state"] = str(state_file)

            context = browser.new_context(**context_kwargs)
            page = context.new_page()

            initial_url = configured_url or DEFAULT_HOLDINGS_URL
            ensure_logged_in_and_ready(page, context, state_file, initial_url, args.headless)

            if configured_url:
                page.goto(configured_url, wait_until="domcontentloaded")
                print(f"Using configured fund URL from {url_config_path}.")
            else:
                print(
                    "\nNo URL configured for this fund. "
                    "Please open the correct MF discrepancy page in the browser window."
                )
                input("After the correct page is open, press Enter to continue...")

            url_used = page.url
            existing_keys = get_existing_trade_keys_with_prompt(page)
            print(f"Loaded {len(existing_keys)} existing trade keys for duplicate checks.")

            for trade in trades:
                duplicate_key = trade.duplicate_key
                if duplicate_key in existing_keys:
                    write_report_row(
                        writer,
                        fund_key,
                        trade,
                        status="skipped_duplicate",
                        reason="Exact Date+Price+Qty already exists",
                        url_used=url_used,
                    )
                    report_handle.flush()
                    counters["skipped_duplicate"] += 1
                    skipped_duplicate_quantity += trade.quantity
                    print(
                        f"[{trade.row_number}/{len(trades)}] "
                        f"Skipped duplicate {trade.date_str} qty={trade.quantity_key} price={trade.price_key}"
                    )
                    continue

                while True:
                    try:
                        add_trade(page, trade)
                        existing_keys.add(duplicate_key)
                        write_report_row(
                            writer,
                            fund_key,
                            trade,
                            status="uploaded",
                            reason="Submitted successfully",
                            url_used=url_used,
                        )
                        report_handle.flush()
                        counters["uploaded"] += 1
                        uploaded_quantity += trade.quantity
                        print(
                            f"[{trade.row_number}/{len(trades)}] "
                            f"Uploaded {trade.date_str} qty={trade.quantity_key} price={trade.price_key}"
                        )
                        break
                    except Exception as exc:
                        print(
                            f"\n[{trade.row_number}/{len(trades)}] Upload failed for "
                            f"{trade.date_str} qty={trade.quantity_key} price={trade.price_key}"
                        )
                        print(f"Reason: {exc}")
                        action = prompt_choice(
                            "Choose action: [r]etry, [s]kip row, [a]bort run: ",
                            {"r", "s", "a"},
                        )
                        if action == "r":
                            continue
                        if action == "s":
                            write_report_row(
                                writer,
                                fund_key,
                                trade,
                                status="skipped_manual",
                                reason=f"User skipped after error: {exc}",
                                url_used=url_used,
                            )
                            report_handle.flush()
                            counters["skipped_manual"] += 1
                            skipped_manual_quantity += trade.quantity
                            break

                        write_report_row(
                            writer,
                            fund_key,
                            trade,
                            status="aborted",
                            reason=f"User aborted after error: {exc}",
                            url_used=url_used,
                        )
                        report_handle.flush()
                        counters["failed"] += 1
                        aborted_at_row = trade.row_number
                        raise SystemExit("Aborted by user during upload.")

            context.close()
            browser.close()

    print("\nRun Summary")
    print(f"uploaded={counters['uploaded']}")
    print(f"skipped_duplicate={counters['skipped_duplicate']}")
    print(f"skipped_manual={counters['skipped_manual']}")
    print(f"failed={counters['failed']}")
    print(f"total_input_quantity={decimal_to_key(total_input_quantity)}")
    print(f"uploaded_quantity={decimal_to_key(uploaded_quantity)}")
    print(f"skipped_duplicate_quantity={decimal_to_key(skipped_duplicate_quantity)}")
    print(f"skipped_manual_quantity={decimal_to_key(skipped_manual_quantity)}")
    print(f"aborted_at_row={aborted_at_row if aborted_at_row is not None else ''}")
    print(f"Report file: {report_path}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nInterrupted by user.")
        sys.exit(130)
