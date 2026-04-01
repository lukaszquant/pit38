# Workbook specification for PIT-38 / IBKR

## Purpose

This workbook is a tax working paper for Polish PIT-38 based on IBKR data.

It has three separate responsibilities:

1. calculation engine
2. tax-form mapping
3. validation

These responsibilities must remain separated.

## Non-goals

This workbook is not required to visually mimic the official tax forms.
Any helper sheet must not pretend to be an official form if it is not one.

## Sheets and responsibilities

### 1. Trades
Purpose: raw transaction-level and lot-level calculation sheet.

Must contain:
- source transactions
- matched lots used for disposal calculations
- tax dates
- NBP dates
- NBP rates
- PLN conversions
- proceeds
- costs
- control columns

Rules:
- revenue-side FX conversion must use the NBP average rate from the last working day before the relevant revenue date
- cost-side FX conversion must use the NBP average rate from the last working day before the relevant cost date
- sale proceeds must not reuse buy-side FX rates
- historical acquisition cost for sold lots must not be translated using sale-date FX rates
- when one sale closes multiple acquisition lots, each matched lot must preserve its own acquisition-date FX logic
- realized PnL may exist only as a control value, not as the primary PIT-38 input

### 2. Summary
Purpose: neutral working summary of calculated totals.

Must contain:
- total proceeds in PLN from taxable disposals
- total tax costs in PLN from taxable disposals
- resulting income or loss
- separate totals for dividends
- separate totals for interest
- separate totals for foreign withholding tax
- separate totals by country where relevant

Rules:
- Summary must be arithmetic-only
- Summary must not contain official PIT field labels unless it is explicitly mapping to the official form
- Summary totals must reconcile exactly to Trades

### 3. PIT38_Summary
Purpose: mapping of workbook results to PIT-38 fields.

Rules:
- labels must match the official PIT-38 form numbering used for the target filing year
- do not use approximate or guessed field numbers
- part C must map only the relevant disposal totals
- part G must use the official semantic meaning of fields, not internal shorthand
- field labels must be verified against the official form PDF, not inferred from prior workbook versions
- amounts shown as ready for filing must use the correct rounding rule for that field

### 4. PIT_ZG_Helper
Purpose: helper sheet only, not the official PIT/ZG form.

Rules:
- this sheet may be used to break down foreign-source items by country
- it must not be named PIT_ZG unless it mirrors the real official attachment structure
- if it is only a helper, its labels must clearly say helper / working sheet
- all country totals must reconcile exactly to PIT38_Summary where applicable

### 5. Validation
Purpose: hard pass/fail acceptance checks.

Must contain explicit checks for:
- Trades revenue FX dates
- Trades cost FX dates
- matched-lot historical cost treatment
- Summary reconciliation to Trades
- PIT38_Summary part C mapping
- PIT38_Summary part G mapping
- country-helper reconciliation
- zero unresolved rounding differences
- no misleading sheet names

Validation output must be binary:
- PASS
- FAIL

## Naming rules

Allowed sheet names:
- Trades
- Summary
- PIT38_Summary
- PIT_ZG_Helper
- Validation

Disallowed:
- PIT_ZG for a helper sheet that is not the real PIT/ZG mapping
- any title implying official-form fidelity where that fidelity is not present

## Required acceptance tests

### A. Trades FX date tests
1. Every revenue-side NBP date equals the last working day before the relevant revenue date
2. Every cost-side NBP date equals the last working day before the relevant cost date
3. No sale row uses sale-date FX for historical acquisition cost
4. Multi-lot sales preserve per-lot acquisition FX logic

### B. Arithmetic reconciliation tests
5. Summary proceeds exactly equal the sum of relevant Trades proceeds
6. Summary costs exactly equal the sum of relevant Trades costs
7. Summary income/loss exactly reconciles to proceeds minus costs
8. Dividend totals exactly reconcile to underlying source rows
9. Interest totals exactly reconcile to underlying source rows
10. Foreign withholding totals exactly reconcile to underlying source rows

### C. PIT-38 mapping tests
11. Every PIT38_Summary field label matches the official PIT-38 form numbering for the target year
12. Part C uses the correct workbook totals
13. Part G uses the correct semantic meaning for each field
14. Any rounding applied in PIT38_Summary is consistent with the documented rule for that field

### D. Country helper tests
15. PIT_ZG_Helper country totals reconcile exactly to the corresponding PIT38_Summary totals
16. No unresolved difference may remain at 0.01 or any other amount
17. PIT_ZG_Helper is clearly marked as a helper, not an official attachment

## Change-control rules for the agent

The agent must not:
- broadly refactor unrelated parts of the workbook
- rename sheets unless explicitly required by this spec
- change formulas outside the requested failure scope
- change field numbering without citing the official form structure being followed
- claim the workbook is final unless every Validation test passes

The agent must:
- first diagnose without editing
- then apply only the explicitly requested fixes
- then produce a cell-by-cell changelog
- then report which acceptance tests pass and which still fail

## Definition of done

The workbook is done only when:
- all Validation tests pass
- all sheet names are semantically correct
- all totals reconcile exactly
- all PIT38_Summary labels and meanings are aligned with the official target form
- no unresolved 0.01 rounding discrepancy remains