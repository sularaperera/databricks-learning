# 04 · Deduplication, Late-Arriving Data & Idempotent Reprocessing

**Scenario:** delivery vans send parcel scan events over a flaky mobile network. Messages are double-sent, retried
with new `received_at` timestamps, and a van that was offline uploads its events over an hour late.

## Contents

```
04_dedup_late_arriving_data/
├── notebooks/
│   └── 01_dedup_and_late_data.py
└── sample_data/
    ├── scan_events_01.json   # includes an exact duplicate (E003)
    ├── scan_events_02.json   # retry of E005 from file 01
    ├── scan_events_03.json   # E013: late but within the watermark
    └── scan_events_04.json   # E017: beyond the watermark; E018: late but within; E012 retry
```

## What you will learn

| Topic | Where |
|---|---|
| Exact vs key-based vs **deterministic** dedup (`row_number`, `QUALIFY`) | §1 |
| Why append creates duplicates; insert-only MERGE for idempotent loads | §2 |
| Watermarks step by step, `dropDuplicatesWithinWatermark`, windowed aggregation in append mode | §3 |
| What a watermark silently drops (E017) | §3 |
| "Correct by design": idempotent silver + recompute only the affected windows with `foreachBatch` + MERGE | §4 |
| Safe replay / backfill after deleting the checkpoint | §4 |
| `txnAppId` / `txnVersion` idempotent appends | §4 |
| Lateness metrics (p50 / p99) for choosing a watermark | §5 |

## Run

Run all cells. Defaults: catalog `workspace`, schema `dedup_demo`, volume `raw`.
