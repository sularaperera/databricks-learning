# 01 · Schema Evolution with Auto Loader

**Scenario:** store POS systems drop daily JSON sales files. The POS vendor changes the format without warning:
new columns, values with the wrong type, a column in different casing, a new nested struct, and a renamed column.

## Contents

```
01_autoloader_schema_evolution/
├── notebooks/
│   └── 01_autoloader_schema_evolution.py
└── sample_data/
    ├── batch_01_initial.json               # baseline
    ├── batch_02_new_columns.json           # + loyalty_id, payment_method
    ├── batch_03_type_changes.json          # qty "3", unit_price "N/A" / "6.25"
    ├── batch_04_case_and_nested.json       # Store_ID (wrong case), + promo {code, pct}
    └── batch_05_renamed_column.json        # unit_price -> price
```

## What you will learn

| Section | Topic |
|---|---|
| 1 | `addNewColumns`: the stream stops once per change, then restarts with the evolved schema; job retries; `mergeSchema` on the Delta sink |
| 1 | Type mismatches and case mismatches → `_rescued_data`; schema history in `_schemas/` |
| 2 | `rescue`: never fail, never change the schema |
| 3 | `failOnNewColumns`: stop until a human decides |
| 4 | `none`: silent data loss, and how `rescuedDataColumn` prevents it |
| 5 | `cloudFiles.schemaHints` to pin money/timestamp types and pre-declare nested structs |
| 6 | Silver: recover renamed/re-cased/mistyped fields from `_rescued_data`; a drift monitor query |
| 7 | `rescuedDataColumn` in plain batch reads |

## Why are "start" and "wait" in separate cells?

When a stream stops with an error **inside the cell that started it**, Databricks marks that cell as failed
(`Some streams terminated before this command could finish!`), even if your Python code catches the exception.
That would stop **Run all** at the first intended schema change. So the notebook starts those streams with
`start_autoloader()` in one cell and checks them with `wait_for()` in the next.

In production you don't restart by hand: a **Job task with retries** (or a Lakeflow pipeline) restarts the stream,
and it picks up the evolved schema.

## Run

**Run all** interactively. Defaults: catalog `workspace`, schema `autoloader_demo`, volume `raw`.

> If you run this notebook *as a Job*, the run is reported as **failed** even though every cell completes: the job
> runner sees that some streams died. That's exactly the signal that triggers job retries in production.
