# Customer Funds Reconciliation Platform

Automated reconciliation pipeline across multiple payment sources — built with dbt, Snowflake, and Python.

## Problem

Finance teams at e-commerce companies manually reconcile payment data across multiple processors using spreadsheets. This is slow, error-prone, and creates audit risk. A single missed transaction can cascade into a compliance breach or customer funds reporting error.

## Solution

An end-to-end automated pipeline that:
- Ingests raw transactional data from 3 payment sources (Stripe, Adyen, internal GL)
- Classifies every transaction as `MATCHED`, `AMOUNT_MISMATCH`, `MISSING_IN_LEDGER`, or `DUPLICATE`
- Flags anomalous settlement days using z-score analysis
- Surfaces breaks to Finance via a structured break report

## Results

| Status | Transactions | Total Amount |
|---|---|---|
| MATCHED | 9,499 | $23,649,640 |
| MISSING_IN_LEDGER | 400 | $997,705 |
| AMOUNT_MISMATCH | 293 | $767,263 |

- **94.99% match rate** across 10,000 transactions
- **693 breaks surfaced** automatically
- **3 anomalous settlement days** flagged by z-score detection

## Tech Stack

- **Warehouse:** Snowflake
- **Transformation:** dbt Core
- **Language:** Python, SQL
- **Data Sources:** Stripe (simulated), Adyen (simulated), Internal GL

## Project Structure

```
├── data/
│   └── generate_data.py         # synthetic data generator
├── recon_dbt/
│   └── models/
│       ├── staging/             # stg_stripe, stg_adyen, stg_ledger
│       ├── intermediate/        # int_transactions_unioned
│       └── marts/               # fct_reconciliation
├── scripts/
│   └── anomaly_detection.py     # z-score settlement flagging
└── README.md
```

## Architecture

```
Stripe API → RAW.raw_stripe  ─┐
Adyen API  → RAW.raw_adyen   ─┼─► dbt staging ─► dbt marts ─► fct_reconciliation
Internal GL → RAW.raw_ledger ─┘                               ► anomaly_detection.py
```

## How to Run

**1. Generate synthetic data**
```bash
python data/generate_data.py
```

**2. Run dbt models**
```bash
cd recon_dbt
dbt run
dbt test
```

**3. Run anomaly detection**
```bash
python scripts/anomaly_detection.py
```

## dbt Tests

All models are covered by schema tests:
- `not_null` on all key columns
- `unique` on transaction IDs
- `accepted_values` on `recon_status`

13/13 tests passing.

## Key SQL — Reconciliation Logic

```sql
CASE
    WHEN l.transaction_id IS NULL        THEN 'MISSING_IN_LEDGER'
    WHEN ABS(s.amount - l.amount) > 0.01 THEN 'AMOUNT_MISMATCH'
    WHEN l.ledger_entry_id IN (
        SELECT ledger_entry_id FROM stg_ledger
        GROUP BY ledger_entry_id HAVING COUNT(*) > 1
    )                                    THEN 'DUPLICATE'
    ELSE                                      'MATCHED'
END AS recon_status
```