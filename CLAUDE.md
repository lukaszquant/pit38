# PIT-38 Tax Calculator

## What this is
Python script that processes Interactive Brokers Activity Statement CSV exports for Polish PIT-38 tax filing. Single file: `pit38.py`.

## How to run
```bash
.venv/bin/python pit38.py ibkr/2025/activity.csv
```
Requires Python venv at `.venv/` with: `pandas`, `requests`, `openpyxl`.

## Key tax rules (Polish PIT-38)
- **NBP exchange rate**: last working day BEFORE the trade/income date (art. 11a ustawy o PIT) — not the date itself, not settlement date
- **FIFO cost basis**: sell trades use the original BUY-date FX rate for cost basis, not the sell-date rate
- **Section C wiersz 2** (poz. 22-29): capital gains from foreign broker (no PIT-8C)
- **Section G** (poz. 47-49): art. 30a income (dividends + interest) — poz. 47 is the calculated 19% tax, NOT gross income
- **PIT/ZG(8)**: per-country attachment for Section C capital gains only. Dividends/interest do NOT go in PIT/ZG.
- **Rounding**: always compute dochod/strata from raw (unrounded) values, then round. Never subtract two independently rounded values.

## Architecture
Everything is in `pit38.py`. Main processing functions:
- `process_trades()` — FIFO matching with buy-date FX rates
- `process_dividends_detailed()` — with ISIN-based country extraction
- `process_interest_detailed()` — country from currency mapping
- `process_wht_detailed()` — splits WHT between dividends and interest

Output: console summary + Excel workbook with 4 sheets (Trades, Summary, PIT38_Summary, PIT_ZG).

## Sensitive data
The `ibkr/` directory contains real financial data and is gitignored. Never commit CSV files or generated `.xlsx` files.

## Known limitations
See GitHub issue #1. The main one: FIFO only sees trades within a single CSV file — no carry-over of open lots across tax years.
