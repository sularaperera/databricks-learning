# 02 · Data Quality: Quarantine Instead of Failing

**Scenario:** a card processor delivers daily transaction files. Some rows are wrong (negative amounts, unknown
accounts, invalid currency, future dates, duplicates), and some are valid but suspicious (over the credit limit,
frozen account). Finance needs the good data on time, and nothing may be silently lost.

## Contents

```
02_data_quality_expectations/
├── notebooks/
│   ├── 01_data_quality_framework.py      # plain-Spark framework, run cell by cell
│   └── 02_inspect_pipeline_results.py    # read the Lakeflow pipeline output + expectation metrics
├── pipeline/
│   └── transactions_dq_pipeline.py       # Lakeflow Declarative Pipeline with @dp.expect*
└── sample_data/
    ├── transactions/transactions_2026-10-01.json   # day 1: 20 rows, 8 bad, some with warnings
    ├── later/transactions_2026-10-02.json          # day 2: for the incremental run
    └── accounts/accounts.csv                       # reference data
```

## What you will learn

| Topic | Where |
|---|---|
| Standardise before validating (fix only what's unambiguous) | notebook 01 §2 |
| Referential-integrity and duplicate checks | notebook 01 §3 |
| Rules as data with **warn / drop / fail** severities, NULL-safe evaluation | notebook 01 §4 |
| Circuit breaker: stop when too many rows are bad, *before* writing anything | notebook 01 §5 |
| Silver + quarantine (with reasons) + per-rule metrics table | notebook 01 §6 |
| Delta `NOT NULL` / `CHECK` constraints | notebook 01 §7 |
| Replaying fixed quarantine rows | notebook 01 §8 |
| `@dp.expect`, `@dp.expect_all_or_drop`, `@dp.expect_or_fail`, a quarantine table in a pipeline | pipeline |
| Expectation metrics from `event_log()` | notebook 02 |

## Run

1. Run all of `notebooks/01_data_quality_framework` (this also uploads the sample data).
2. Deploy and run the pipeline from the repo root:
   ```bash
   databricks bundle deploy -p <profile>
   databricks bundle run dq_transactions_pipeline -p <profile>
   ```
   Or in the UI: **Jobs & Pipelines → Create → ETL pipeline**, add `pipeline/transactions_dq_pipeline.py` as source,
   catalog `workspace`, schema `dq_demo`, and the configuration
   `dq_demo.source_path = /Volumes/workspace/dq_demo/raw/dq_demo/sample_data`.
3. Run `notebooks/02_inspect_pipeline_results`. Its section 3 lands day 2, so you can re-run the pipeline and watch it
   process only the new file.
