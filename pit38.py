#!/usr/bin/env python3
"""
Process Interactive Brokers Activity Statement CSV for Polish PIT-38 tax filing.
Converts trades, dividends, and withholding tax to PLN using NBP exchange rates.

Usage: python pit38.py ibkr/2025/activity.csv
"""

import csv
import re
import sys
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta
from io import StringIO
from pathlib import Path

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# NBP rate fetching with caching
# ---------------------------------------------------------------------------

_nbp_cache: dict[tuple[str, str], tuple[float, str]] = {}


def get_nbp_rate(currency: str, date: str) -> tuple[float, str]:
    """
    Get NBP mid exchange rate for currency on the given date.
    If the date falls on a weekend/holiday (API 404), try up to 7 days back.
    Returns (rate, actual_date_used).
    PLN returns (1.0, date).
    """
    if currency == "PLN":
        return 1.0, date

    for days_back in range(8):
        d = datetime.strptime(date, "%Y-%m-%d") - timedelta(days=days_back)
        d_str = d.strftime("%Y-%m-%d")
        key = (currency, d_str)

        if key in _nbp_cache:
            return _nbp_cache[key]

        url = f"https://api.nbp.pl/api/exchangerates/rates/a/{currency}/{d_str}/?format=json"
        try:
            resp = requests.get(url, timeout=10)
            if resp.status_code == 200:
                rate = resp.json()["rates"][0]["mid"]
                _nbp_cache[key] = (rate, d_str)
                time.sleep(0.2)
                return rate, d_str
            elif resp.status_code == 404:
                time.sleep(0.1)
                continue
            else:
                print(f"  WARNING: NBP API returned {resp.status_code} for {currency}/{d_str}")
                time.sleep(0.2)
                continue
        except requests.RequestException as e:
            print(f"  WARNING: NBP API error for {currency}/{d_str}: {e}")
            time.sleep(0.2)
            continue

    raise ValueError(f"Could not fetch NBP rate for {currency} near {date} (tried 8 days back)")


def get_nbp_rate_day_before(currency: str, date_str: str) -> tuple[float, str]:
    """Get NBP rate for the day BEFORE the given date (Polish tax law requirement)."""
    if currency == "PLN":
        return 1.0, date_str
    day_before = (datetime.strptime(date_str, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
    return get_nbp_rate(currency, day_before)


# ---------------------------------------------------------------------------
# CSV parsing
# ---------------------------------------------------------------------------

def parse_sections(filepath: str) -> dict[str, list[list[str]]]:
    """Parse IB Activity Statement CSV into sections keyed by section name."""
    sections: dict[str, list[list[str]]] = {}
    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) < 2:
                continue
            section_name = row[0]
            sections.setdefault(section_name, []).append(row)
    return sections


def parse_section_data(rows: list[list[str]]) -> list[dict]:
    """
    Given rows for a section, find the Header row to get column names,
    then return Data rows as list of dicts.
    """
    headers = None
    data = []
    for row in rows:
        if len(row) < 2:
            continue
        if row[1] == "Header":
            headers = row[2:]  # skip section name and "Header"
        elif row[1] == "Data" and headers:
            values = row[2:]
            # Pad values to match headers length
            while len(values) < len(headers):
                values.append("")
            data.append(dict(zip(headers, values)))
    return data


# ---------------------------------------------------------------------------
# Processing
# ---------------------------------------------------------------------------

def process_trades(sections: dict) -> tuple[float, float, float, list[dict]]:
    """
    Process stock trades with FIFO matching.
    Returns (total_proceeds_pln, total_basis_pln, total_commission_pln, details).

    NBP rate: last working day BEFORE the trade date (art. 11a ustawy o PIT).
    Basis on sell rows: converted at the original BUY-date NBP rate via FIFO.
    Proceeds and sell commission: converted at the SELL-date NBP rate.
    """
    if "Trades" not in sections:
        print("No Trades section found.")
        return 0.0, 0.0, 0.0, []

    rows = sections["Trades"]

    # --- Pass 1: parse all stock Order trades ---
    raw_trades: list[dict] = []
    current_headers = None

    for row in rows:
        if len(row) < 2:
            continue
        if row[1] == "Header":
            current_headers = row[2:]
            continue
        if row[1] != "Data" or current_headers is None:
            continue

        values = row[2:]
        while len(values) < len(current_headers):
            values.append("")
        rec = dict(zip(current_headers, values))

        if rec.get("DataDiscriminator") != "Order":
            continue
        if rec.get("Asset Category") != "Stocks":
            continue

        try:
            currency = rec["Currency"]
            symbol = rec["Symbol"]
            datetime_str = rec["Date/Time"].strip().strip('"')
            trade_date = datetime_str.split(",")[0].strip()

            quantity = float(rec["Quantity"].replace(",", ""))
            proceeds = float(rec["Proceeds"].replace(",", ""))
            comm = float(rec["Comm/Fee"].replace(",", ""))
            basis = float(rec["Basis"].replace(",", ""))
            t_price = float(rec["T. Price"].replace(",", "")) if rec.get("T. Price") else 0.0

            raw_trades.append({
                "trade_date": trade_date,
                "symbol": symbol,
                "currency": currency,
                "quantity": quantity,
                "proceeds": proceeds,
                "comm": comm,
                "basis": basis,
                "t_price": t_price,
            })
        except Exception as e:
            print(f"  ERROR parsing trade: {e} | {row}")

    # --- Pass 2: fetch NBP rates (day before trade date) for every trade ---
    for t in raw_trades:
        rate, nbp_date = get_nbp_rate_day_before(t["currency"], t["trade_date"])
        t["nbp_rate"] = rate
        t["nbp_date"] = nbp_date
        print(f"  {t['trade_date']} {t['symbol']:6s} qty={t['quantity']:>10.4f} {t['currency']}"
              f"  NBP {t['currency']}/PLN={rate:.4f} ({nbp_date})")

    # --- Pass 3: FIFO matching ---
    # Each buy lot stores: qty, cost_per_unit (basis/qty, includes buy comm),
    # trade_date, nbp_rate, nbp_date
    fifo: dict[tuple[str, str], deque[dict]] = defaultdict(deque)

    total_proceeds_pln = 0.0
    total_basis_pln = 0.0
    total_commission_pln = 0.0
    details: list[dict] = []

    for t in raw_trades:
        symbol = t["symbol"]
        currency = t["currency"]
        key = (symbol, currency)
        quantity = t["quantity"]

        if quantity > 0:
            # --- BUY: push lot onto FIFO queue ---
            cost_per_unit = t["basis"] / quantity  # includes buy commission
            fifo[key].append({
                "qty": quantity,
                "cost_per_unit": cost_per_unit,
                "trade_date": t["trade_date"],
                "nbp_rate": t["nbp_rate"],
                "nbp_date": t["nbp_date"],
            })

            details.append({
                "date": t["trade_date"],
                "symbol": symbol,
                "quantity": quantity,
                "currency": currency,
                "t_price": t["t_price"],
                "proceeds": t["proceeds"],
                "basis": t["basis"],
                "comm": t["comm"],
                "nbp_rate": t["nbp_rate"],
                "nbp_date": t["nbp_date"],
                "proceeds_pln": t["proceeds"] * t["nbp_rate"],
                "basis_pln": t["basis"] * t["nbp_rate"],
                "comm_pln": t["comm"] * t["nbp_rate"],
                "realized_pln": 0.0,
                "buy_date": "",
                "buy_nbp_rate": "",
                "buy_nbp_date": "",
            })

        else:
            # --- SELL: consume FIFO lots, convert basis at buy-date rate ---
            sell_rate = t["nbp_rate"]
            sell_nbp_date = t["nbp_date"]
            sell_qty = abs(quantity)

            proceeds_pln = t["proceeds"] * sell_rate
            comm_pln = t["comm"] * sell_rate  # sell commission at sell rate

            basis_pln = 0.0
            remaining = sell_qty
            buy_dates: list[str] = []
            buy_rates: list[str] = []
            buy_nbp_dates: list[str] = []

            while remaining > 1e-10 and fifo[key]:
                lot = fifo[key][0]
                matched = min(remaining, lot["qty"])

                # Basis portion: negative (cost), converted at the BUY-date rate
                basis_pln -= matched * lot["cost_per_unit"] * lot["nbp_rate"]

                buy_dates.append(lot["trade_date"])
                buy_rates.append(f"{lot['nbp_rate']:.4f}")
                buy_nbp_dates.append(lot["nbp_date"])

                lot["qty"] -= matched
                remaining -= matched

                if lot["qty"] < 1e-10:
                    fifo[key].popleft()

            if remaining > 0.001:
                print(f"  WARNING: FIFO underflow for {symbol} — {remaining:.4f} shares unmatched")

            realized_pln = proceeds_pln + basis_pln + comm_pln

            total_proceeds_pln += proceeds_pln
            total_basis_pln += abs(basis_pln)
            total_commission_pln += abs(comm_pln)

            print(f"    SELL {t['trade_date']} {symbol:6s} proceeds_pln={proceeds_pln:>12.2f}"
                  f"  basis_pln={basis_pln:>12.2f}  comm_pln={comm_pln:>8.2f}"
                  f"  realized={realized_pln:>10.2f}  (buy: {', '.join(buy_dates)})")

            details.append({
                "date": t["trade_date"],
                "symbol": symbol,
                "quantity": t["quantity"],
                "currency": currency,
                "t_price": t["t_price"],
                "proceeds": t["proceeds"],
                "basis": t["basis"],
                "comm": t["comm"],
                "nbp_rate": sell_rate,
                "nbp_date": sell_nbp_date,
                "proceeds_pln": proceeds_pln,
                "basis_pln": basis_pln,
                "comm_pln": comm_pln,
                "realized_pln": realized_pln,
                "buy_date": " / ".join(buy_dates),
                "buy_nbp_rate": " / ".join(buy_rates),
                "buy_nbp_date": " / ".join(buy_nbp_dates),
            })

    return total_proceeds_pln, total_basis_pln, total_commission_pln, details


# ---------------------------------------------------------------------------
# Country helpers
# ---------------------------------------------------------------------------

CURRENCY_TO_COUNTRY = {
    "GBP": "GB",
    "USD": "US",
    "EUR": "IE",
    "CHF": "CH",
}

COUNTRY_NAMES = {
    "IE": "Irlandia",
    "US": "Stany Zjednoczone",
    "GB": "Wielka Brytania",
    "LU": "Luksemburg",
    "DE": "Niemcy",
    "CH": "Szwajcaria",
    "FR": "Francja",
    "NL": "Holandia",
}


def extract_isin_country(description: str) -> str:
    """Extract country code from ISIN in description, e.g. 'VGOV(IE00B42WWV65)' -> 'IE'."""
    m = re.search(r"\(([A-Z]{2}[A-Z0-9]{10})\)", description)
    if m:
        return m.group(1)[:2]
    return ""


# ---------------------------------------------------------------------------
# Dividends, interest, WHT — with per-country breakdown
# ---------------------------------------------------------------------------

def process_dividends_detailed(sections: dict) -> tuple[float, list[dict]]:
    """
    Process dividends with country extraction from ISIN.
    Returns (total_pln, detail_rows).
    Each detail row: date, symbol, description, currency, amount, country,
                     nbp_rate, nbp_date, amount_pln, type.
    """
    if "Dividends" not in sections:
        print("No Dividends section found.")
        return 0.0, []

    data = parse_section_data(sections["Dividends"])
    total_pln = 0.0
    details: list[dict] = []

    for rec in data:
        if rec.get("Currency") == "Total":
            continue
        try:
            currency = rec["Currency"]
            date_str = rec["Date"]
            amount = float(rec["Amount"].replace(",", ""))
            desc = rec.get("Description", "")

            country = extract_isin_country(desc)
            symbol_match = re.match(r"(\w+)\(", desc)
            symbol = symbol_match.group(1) if symbol_match else ""
            flag = ""
            if not country:
                country = "??"
                flag = " *** COUNTRY UNKNOWN"

            rate, nbp_date = get_nbp_rate_day_before(currency, date_str)
            amount_pln = amount * rate
            total_pln += amount_pln

            print(f"  {date_str} {symbol:6s} {country} {currency} {amount:>10.2f}"
                  f"  NBP={rate:.4f} ({nbp_date}) => {amount_pln:>10.2f} PLN{flag}")

            details.append({
                "date": date_str,
                "symbol": symbol,
                "description": desc,
                "currency": currency,
                "amount": amount,
                "country": country,
                "nbp_rate": rate,
                "nbp_date": nbp_date,
                "amount_pln": round(amount_pln, 2),
                "type": "Dywidenda",
            })
        except Exception as e:
            print(f"  ERROR processing dividend: {e} | {rec}")

    return total_pln, details


def process_interest_detailed(sections: dict) -> tuple[float, list[dict]]:
    """
    Process interest with country derived from currency.
    Returns (total_pln, detail_rows).
    """
    if "Interest" not in sections:
        print("No Interest section found.")
        return 0.0, []

    data = parse_section_data(sections["Interest"])
    total_pln = 0.0
    details: list[dict] = []

    for rec in data:
        if rec.get("Currency") == "Total":
            continue
        try:
            currency = rec["Currency"]
            date_str = rec["Date"]
            amount = float(rec["Amount"].replace(",", ""))
            desc = rec.get("Description", "")

            country = CURRENCY_TO_COUNTRY.get(currency, currency)

            rate, nbp_date = get_nbp_rate_day_before(currency, date_str)
            amount_pln = amount * rate
            total_pln += amount_pln

            print(f"  {date_str} {country} {currency} {amount:>10.2f}"
                  f"  NBP={rate:.4f} ({nbp_date}) => {amount_pln:>10.2f} PLN  {desc[:50]}")

            details.append({
                "date": date_str,
                "symbol": "",
                "description": desc,
                "currency": currency,
                "amount": amount,
                "country": country,
                "nbp_rate": rate,
                "nbp_date": nbp_date,
                "amount_pln": round(amount_pln, 2),
                "type": "Odsetki",
            })
        except Exception as e:
            print(f"  ERROR processing interest: {e} | {rec}")

    return total_pln, details


def process_wht_detailed(
    sections: dict,
    dividend_details: list[dict],
    interest_details: list[dict],
) -> tuple[float, float, list[dict]]:
    """
    Process WHT with country assignment.
    Interest WHT -> country from currency mapping.
    Dividend WHT -> country from matched dividend (closest date, same currency, within 14 days).
    Returns (dividend_wht_pln, interest_wht_pln, detail_rows).
    """
    if "Withholding Tax" not in sections:
        print("No Withholding Tax section found.")
        return 0.0, 0.0, []

    data = parse_section_data(sections["Withholding Tax"])
    dividend_wht_pln = 0.0
    interest_wht_pln = 0.0
    details: list[dict] = []

    for rec in data:
        if rec.get("Currency") == "Total":
            continue
        try:
            currency = rec["Currency"]
            date_str = rec["Date"]
            amount = abs(float(rec["Amount"].replace(",", "")))
            desc = rec.get("Description", "")

            is_interest = "interest" in desc.lower()

            if is_interest:
                country = CURRENCY_TO_COUNTRY.get(currency, currency)
                bucket = "WHT Odsetki"
            else:
                # Match to closest dividend by currency and date
                country = _match_wht_to_dividend_country(
                    date_str, currency, dividend_details
                )
                bucket = "WHT Dywidenda"

            rate, nbp_date = get_nbp_rate_day_before(currency, date_str)
            amount_pln = amount * rate

            if is_interest:
                interest_wht_pln += amount_pln
            else:
                dividend_wht_pln += amount_pln

            label = "INT" if is_interest else "DIV"
            print(f"  [{label}] {date_str} {country} {currency} {amount:>10.2f}"
                  f"  NBP={rate:.4f} ({nbp_date}) => {amount_pln:>10.2f} PLN")

            details.append({
                "date": date_str,
                "symbol": "",
                "description": desc,
                "currency": currency,
                "amount": -amount,  # WHT is negative by convention
                "country": country,
                "nbp_rate": rate,
                "nbp_date": nbp_date,
                "amount_pln": round(-amount_pln, 2),  # negative in detail
                "type": bucket,
            })
        except Exception as e:
            print(f"  ERROR processing WHT: {e} | {rec}")

    return dividend_wht_pln, interest_wht_pln, details


def _match_wht_to_dividend_country(
    wht_date_str: str, wht_currency: str, dividend_details: list[dict]
) -> str:
    """Find the country of the closest dividend matching currency within 14 days."""
    wht_date = datetime.strptime(wht_date_str, "%Y-%m-%d")
    best_country = "??"
    best_gap = timedelta(days=15)

    for div in dividend_details:
        if div["currency"] != wht_currency:
            continue
        div_date = datetime.strptime(div["date"], "%Y-%m-%d")
        gap = abs(wht_date - div_date)
        if gap < best_gap:
            best_gap = gap
            best_country = div["country"]

    if best_country == "??":
        best_country = CURRENCY_TO_COUNTRY.get(wht_currency, "??")

    return best_country


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if len(sys.argv) < 2:
        print("Usage: python pit38.py <activity.csv>")
        sys.exit(1)

    filepath = sys.argv[1]
    if not Path(filepath).exists():
        print(f"File not found: {filepath}")
        sys.exit(1)

    print(f"Processing: {filepath}")
    print("=" * 70)

    sections = parse_sections(filepath)
    print(f"Sections found: {', '.join(sections.keys())}")
    print()

    # --- Trades ---
    print("--- TRADES (Stocks) ---")
    proceeds_pln, basis_pln, commission_pln, trade_details = process_trades(sections)
    print()

    # --- Dividends ---
    print("--- DIVIDENDS ---")
    dividends_pln, dividend_details = process_dividends_detailed(sections)
    print()

    # --- Interest ---
    print("--- INTEREST ---")
    interest_pln, interest_details = process_interest_detailed(sections)
    print()

    # --- Withholding Tax ---
    print("--- WITHHOLDING TAX ---")
    dividend_wht_pln, interest_wht_pln, wht_details = process_wht_detailed(
        sections, dividend_details, interest_details
    )
    print()

    # --- Summary ---
    cost_pln = basis_pln + commission_pln
    income_pln = proceeds_pln - cost_pln

    print("=" * 70)
    print()
    print("=== PIT-38 SUMMARY ===")
    print()

    dochod = max(0, income_pln)
    strata = max(0, -income_pln)

    print("CZESC C - Zbycie papierow wartosciowych (wiersz 2):")
    print(f"  poz. 22 Przychod:           {proceeds_pln:>12,.2f} PLN")
    print(f"  poz. 23 Koszty:             {cost_pln:>12,.2f} PLN")
    print(f"  poz. 26 Suma przychodow:    {proceeds_pln:>12,.2f} PLN")
    print(f"  poz. 27 Suma kosztow:       {cost_pln:>12,.2f} PLN")
    print(f"  poz. 28 Dochod:             {dochod:>12,.2f} PLN")
    print(f"  poz. 29 Strata:             {strata:>12,.2f} PLN")
    print()

    total_income_30a = dividends_pln + interest_pln
    total_wht_30a = dividend_wht_pln + interest_wht_pln

    # --- PIT/ZG per-country aggregation (needed before poz. 49) ---
    all_income_details = dividend_details + interest_details + wht_details
    country_data: dict[str, dict[str, float]] = {}
    for row in all_income_details:
        c = row["country"]
        if c not in country_data:
            country_data[c] = {
                "dividends_pln": 0.0,
                "interest_pln": 0.0,
                "wht_div_pln": 0.0,
                "wht_int_pln": 0.0,
            }
        if row["type"] == "Dywidenda":
            country_data[c]["dividends_pln"] += row["amount_pln"]
        elif row["type"] == "Odsetki":
            country_data[c]["interest_pln"] += row["amount_pln"]
        elif row["type"] == "WHT Dywidenda":
            country_data[c]["wht_div_pln"] += abs(row["amount_pln"])
        elif row["type"] == "WHT Odsetki":
            country_data[c]["wht_int_pln"] += abs(row["amount_pln"])

    # Section G: poz. 47 = calculated tax (19% × income), not income itself
    # Round 47 and 48 first, then derive 49 from rounded values
    poz47_val = round(total_income_30a * 0.19, 2)
    poz48_val = round(total_wht_30a, 2)
    poz49_val = round(max(0, poz47_val - poz48_val), 2)

    print("CZESC G - Przychody z art. 30a (zagraniczne):")
    print(f"  Podstawa (przychod brutto):     {total_income_30a:>12,.2f} PLN")
    print(f"  poz. 47 Podatek obliczony (19%): {poz47_val:>11,.2f} PLN")
    print(f"  poz. 48 WHT zaplacony:          {poz48_val:>12,.2f} PLN")
    print(f"  poz. 49 Do zaplaty (47-48):     {poz49_val:>12,.2f} PLN")
    print()

    print("=== PIT/ZG — rozliczenie per kraj ===")
    print()

    kontrola_poz47 = 0.0
    kontrola_poz48 = 0.0
    kontrola_poz49 = 0.0

    for c in sorted(country_data.keys()):
        cd = country_data[c]
        name = COUNTRY_NAMES.get(c, c)
        przychod = cd["dividends_pln"] + cd["interest_pln"]
        wht = cd["wht_div_pln"] + cd["wht_int_pln"]
        podatek_19 = round(przychod * 0.19, 2)
        wht_r = round(wht, 2)
        do_zaplaty = round(max(0, podatek_19 - wht_r), 2)
        nadwyzka = wht_r > podatek_19

        print(f"  Kraj: {c} ({name})")
        print(f"    Dywidendy:               {cd['dividends_pln']:>10.2f} PLN")
        print(f"    Odsetki:                 {cd['interest_pln']:>10.2f} PLN")
        print(f"    Przychod brutto:         {przychod:>10.2f} PLN")
        print(f"    poz. 47 Podatek 19%:     {podatek_19:>10.2f} PLN")
        print(f"    poz. 48 WHT zaplacony:   {wht_r:>10.2f} PLN")
        print(f"    poz. 49 Do zaplaty:      {do_zaplaty:>10.2f} PLN"
              + ("  *** WHT przekracza podatek nalezny — nadwyzka przepada" if nadwyzka else ""))
        print()

    print("  KONTROLA (PIT38_Summary jest zrodlem prawdy):")
    print(f"    poz. 47: {poz47_val:>10.2f} PLN")
    print(f"    poz. 48: {poz48_val:>10.2f} PLN")
    print(f"    poz. 49: {poz49_val:>10.2f} PLN")
    print()

    # --- Export Excel ---
    if trade_details:
        output_dir = Path(filepath).parent
        xlsx_path = output_dir / "pit38_details.xlsx"

        df = pd.DataFrame(trade_details)
        col_order = [
            "date", "symbol", "quantity", "currency",
            "t_price", "proceeds", "basis", "comm",
            "nbp_rate", "nbp_date",
            "proceeds_pln", "basis_pln", "comm_pln", "realized_pln",
            "buy_date", "buy_nbp_rate", "buy_nbp_date",
        ]
        df = df[col_order]

        with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="Trades", index=False)

            # Summary sheet — detailed breakdown
            summary_data = {
                "Item": [
                    "--- Zyski kapitalowe ---",
                    "Przychod (Proceeds) PLN",
                    "Koszty (Basis + Prowizje) PLN",
                    "Dochod/Strata PLN",
                    "",
                    "--- Dywidendy ---",
                    "Dywidendy brutto PLN",
                    "WHT zaplacony (dywidendy) PLN",
                    "",
                    "--- Odsetki ---",
                    "Odsetki brutto PLN",
                    "WHT zaplacony (odsetki) PLN",
                ],
                "Value": [
                    "",
                    round(proceeds_pln, 2),
                    round(cost_pln, 2),
                    round(income_pln, 2),
                    "",
                    "",
                    round(dividends_pln, 2),
                    round(dividend_wht_pln, 2),
                    "",
                    "",
                    round(interest_pln, 2),
                    round(interest_wht_pln, 2),
                ],
            }
            pd.DataFrame(summary_data).to_excel(writer, sheet_name="Summary", index=False)

            # PIT-38 form sheet — Section C wiersz 2 (foreign broker, no PIT-8C)
            poz22 = round(proceeds_pln, 2)
            poz23 = round(cost_pln, 2)
            poz26 = poz22  # = poz.22 since poz.20 = 0
            poz27 = poz23  # = poz.23 since poz.21 = 0
            poz28 = round(max(0, income_pln), 2)   # dochod
            poz29 = round(max(0, -income_pln), 2)   # strata

            # Section G — art. 30a (foreign dividends + interest)
            podstawa_30a = round(dividends_pln + interest_pln, 2)
            poz47 = round(poz47_val, 2)
            poz48 = round(poz48_val, 2)
            poz49 = round(poz49_val, 2)

            # Control checks
            chk_26 = poz26 == poz22
            chk_27 = poz27 == poz23
            chk_28_29 = (poz28 == 0) != (poz29 == 0) or (poz28 == 0 and poz29 == 0)

            pit38_rows = [
                ("CZESC C - Zbycie papierow wartosciowych (wiersz 2 — broker zagraniczny)", ""),
                ("poz. 22 — Przychod", poz22),
                ("poz. 23 — Koszty", poz23),
                ("poz. 26 — Suma przychodow (= poz.20 + poz.22)", poz26),
                ("poz. 27 — Suma kosztow (= poz.21 + poz.23)", poz27),
                ("poz. 28 — Dochod", poz28),
                ("poz. 29 — Strata", poz29),
                ("", ""),
                ("Kontrola:", ""),
                ("poz. 26 = poz. 22 (gdy poz. 20 = 0)", chk_26),
                ("poz. 27 = poz. 23 (gdy poz. 21 = 0)", chk_27),
                ("tylko jedno z poz. 28 / poz. 29 jest niezerowe", chk_28_29),
                ("", ""),
                ("CZESC G — Przychody z art. 30a (zagraniczne)", ""),
                ("Podstawa (przychod brutto: dywidendy + odsetki)", podstawa_30a),
                ("poz. 47 — Podatek obliczony od przychodow z art. 30a uzyskanych za granica (19% x przychod)", poz47),
                ("poz. 48 — Podatek zaplacony za granica (WHT)", poz48),
                ("poz. 49 — Do zaplaty (poz. 47 minus poz. 48)", poz49),
                ("", ""),
                ("WYMAGA POTWIERDZENIA PRZED ZLOZENIEM:", ""),
                ("[ ] PIT/ZG dla czesci C — podzial zyskow/strat per kraj (zakladka PIT_ZG)", ""),
                ("[ ] Weryfikacja stawek WHT per umowa UPO dla kazdego kraju", ""),
                ("[ ] Sprawdzenie kompletnosci danych (Dividend Report / Form 1042-S z IBKR)", ""),
            ]

            pit38_df = pd.DataFrame(pit38_rows, columns=["Pozycja", "PLN"])
            pit38_df.to_excel(writer, sheet_name="PIT38_Summary", index=False)

            # PIT/ZG(8) sheet — per-country capital gains (art. 30b, Section C)
            # Map symbols to country of instrument
            SYMBOL_COUNTRY = {
                "VGOV": "IE", "VAGS": "IE", "IUSN": "IE", "IGWD": "IE",
                "UC48": "IE", "AGAC": "IE", "EIMI": "IE", "IWDA": "IE",
            }

            # Collect sell rows from trade_details, assign country
            sell_rows_by_country: dict[str, list[dict]] = defaultdict(list)
            for td in trade_details:
                if td["proceeds"] <= 0:
                    continue  # skip buy rows
                sym = td["symbol"]
                country = SYMBOL_COUNTRY.get(sym, "??")
                sell_rows_by_country[country].append(td)

            pitzg_rows: list[dict] = []
            sum_przychod_raw = 0.0
            sum_koszty_raw = 0.0

            for c in sorted(sell_rows_by_country.keys()):
                sells = sell_rows_by_country[c]
                name = COUNTRY_NAMES.get(c, "KRAJ NIEZNANY — wymaga weryfikacji")

                # Country header
                pitzg_rows.append({
                    "Pozycja": f"=== {c} — {name} ===",
                })

                # Detail rows — sum raw values, round only the totals
                c_przychod_raw = 0.0
                c_koszty_raw = 0.0
                for s in sorted(sells, key=lambda x: x["date"]):
                    p = round(s["proceeds_pln"], 2)
                    b = round(abs(s["basis_pln"]), 2)
                    cm = round(abs(s["comm_pln"]), 2)
                    r = round(s["realized_pln"], 2)
                    c_przychod_raw += s["proceeds_pln"]
                    c_koszty_raw += abs(s["basis_pln"]) + abs(s["comm_pln"])
                    pitzg_rows.append({
                        "Pozycja": "",
                        "Data": s["date"],
                        "Symbol": s["symbol"],
                        "Ilosc": round(abs(s["quantity"]), 4),
                        "Przychod PLN": p,
                        "Koszty PLN": b + cm,
                        "Wynik PLN": r,
                    })

                c_przychod = round(c_przychod_raw, 2)
                c_koszty = round(c_koszty_raw, 2)
                c_income_raw = c_przychod_raw - c_koszty_raw
                c_dochod = round(max(0, c_income_raw), 2)
                c_strata = round(max(0, -c_income_raw), 2)

                sum_przychod_raw += c_przychod_raw
                sum_koszty_raw += c_koszty_raw

                # Country summary
                pitzg_rows.append({
                    "Pozycja": "  Przychod lacznie (poz. 29/31)", "Przychod PLN": c_przychod,
                })
                pitzg_rows.append({
                    "Pozycja": "  Koszty lacznie (poz. 30/32)", "Koszty PLN": c_koszty,
                })
                pitzg_rows.append({
                    "Pozycja": "  Dochod / Strata", "Wynik PLN": round(c_income_raw, 2),
                })
                pitzg_rows.append({"Pozycja": ""})
                pitzg_rows.append({
                    "Pozycja": "  poz. 29 — Przychod (wiersz 2, broker zagraniczny)",
                    "Przychod PLN": c_przychod,
                })
                pitzg_rows.append({
                    "Pozycja": "  poz. 30 — Koszty (wiersz 2)",
                    "Koszty PLN": c_koszty,
                })
                pitzg_rows.append({
                    "Pozycja": "  poz. 31 — Dochod",
                    "Wynik PLN": c_dochod,
                })
                pitzg_rows.append({
                    "Pozycja": "  poz. 32 — Strata",
                    "Wynik PLN": c_strata,
                })
                pitzg_rows.append({})  # blank row

            # KONTROLA block — cross-check vs PIT38_Summary
            sum_przychod = round(sum_przychod_raw, 2)
            sum_koszty = round(sum_koszty_raw, 2)
            sum_income_raw = sum_przychod_raw - sum_koszty_raw
            sum_dochod = round(max(0, sum_income_raw), 2)
            sum_strata = round(max(0, -sum_income_raw), 2)

            pitzg_rows.append({"Pozycja": "=== KONTROLA (vs PIT38_Summary) ==="})
            for label, zg_val, pit38_val in [
                ("Suma poz. 29 (przychod)", sum_przychod, poz22),
                ("Suma poz. 30 (koszty)", sum_koszty, poz23),
                ("Suma poz. 31 (dochod)", sum_dochod, poz28),
                ("Suma poz. 32 (strata)", sum_strata, poz29),
            ]:
                ok = abs(zg_val - pit38_val) < 0.015
                pitzg_rows.append({
                    "Pozycja": label,
                    "Przychod PLN": zg_val,
                    "Koszty PLN": pit38_val,
                    "Wynik PLN": ok,
                })

            pitzg_cols = ["Pozycja", "Data", "Symbol", "Ilosc",
                          "Przychod PLN", "Koszty PLN", "Wynik PLN"]
            pitzg_df = pd.DataFrame(pitzg_rows, columns=pitzg_cols)
            pitzg_df.to_excel(writer, sheet_name="PIT_ZG", index=False)

        print(f"Excel exported: {xlsx_path}")

    print("Done.")


if __name__ == "__main__":
    main()
