# 03 · Slowly Changing Dimensions (SCD Type 1 & 2)

**Scenario:** a CRM sends customer change records (CDC) with `op` (INSERT/UPDATE/DELETE) and a source sequence
number `change_seq`. Batch 3 is messy: two changes for one customer, an exact duplicate, an out-of-order change from
January arriving in March, and a delete.

## Contents

```
03_scd_type1_type2/
├── notebooks/
│   └── 01_scd_with_merge.py              # SCD1 + SCD2 with hand-written MERGE, then foreachBatch streaming
├── pipeline/
│   └── customers_auto_cdc_pipeline.py    # the same with AUTO CDC (formerly APPLY CHANGES)
└── sample_data/
    ├── customers_cdc_batch_01_initial.json
    ├── customers_cdc_batch_02_changes.json
    └── customers_cdc_batch_03_messy.json
```

## What you will learn

| Topic | Where |
|---|---|
| Why the naive MERGE fails (multiple source rows match one target row) and how to dedup the source | §1 |
| Sequence guard (`s.change_seq > t.change_seq`) for out-of-order data and idempotent reruns | §1 |
| The hard-delete trap and tombstones | §1 |
| The SCD2 staged-MERGE trick, several versions per key in one batch, hash-based no-op detection | §2 |
| Logging skipped (out-of-order / already applied) changes | §2 |
| SCD2 integrity checks (one current row, no gaps or overlaps) and point-in-time queries | §2 |
| Streaming SCD2 with Auto Loader + `foreachBatch` (and `batch_df.sparkSession`) | §3 |
| `AUTO CDC` with `sequence_by`, `apply_as_deletes`, `track_history_column_list` | pipeline + §4 |

## Run

1. Run all of `notebooks/01_scd_with_merge` (this also uploads the sample data).
2. Optional, to compare with AUTO CDC:
   ```bash
   databricks bundle deploy -p <profile>
   databricks bundle run customers_auto_cdc_pipeline -p <profile>
   ```
   Then re-run §4 of the notebook.
