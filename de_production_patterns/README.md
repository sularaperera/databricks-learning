# Production Data Engineering Patterns on Databricks

Five hands-on demos that continue from [`json_handling_demo`](../json_handling_demo/Readme.md). Each one takes a
problem you will meet in a real production pipeline, gives you **realistic messy sample data** for a different
industry, and walks from the naive approach to the production-grade one.

| # | Demo | Industry / scenario | You will learn |
|---|---|---|---|
| 01 | [Auto Loader schema evolution](01_autoloader_schema_evolution/README.md) | Retail POS sales files | `schemaEvolutionMode` (addNewColumns / rescue / failOnNewColumns / none), `_rescued_data`, `schemaHints`, schema history, drift monitoring |
| 02 | [Data quality expectations](02_data_quality_expectations/README.md) | Banking card transactions | warn / drop / fail rules, quarantine tables, circuit breakers, DQ metrics, Delta constraints, Lakeflow `@dp.expect*`, event-log metrics |
| 03 | [SCD Type 1 & Type 2](03_scd_type1_type2/README.md) | CRM customer CDC feed | Safe `MERGE` for SCD1/SCD2, duplicates, out-of-order changes, deletes, idempotency, `foreachBatch`, `AUTO CDC` |
| 04 | [Dedup & late-arriving data](04_dedup_late_arriving_data/README.md) | Logistics parcel scans | Deterministic dedup, insert-only MERGE, watermarks, `dropDuplicatesWithinWatermark`, recomputing affected windows, `txnAppId`/`txnVersion` |
| 05 | [VARIANT vs from_json](05_variant_vs_from_json/README.md) | Payment gateway webhooks | `from_json` pitfalls, `VARIANT` paths, `try_variant_get`, `variant_explode`, `schema_of_variant_agg`, performance, the hybrid pattern |

## Layout

Every demo has the same structure, so you always know where to look:

```
de_production_patterns/
├── README.md                     ← you are here
├── resources/pipelines.yml       ← bundle definition of the two Lakeflow pipelines (demos 02 and 03)
└── NN_demo_name/
    ├── README.md                 ← what the demo covers + how to run it
    ├── notebooks/*.py            ← Databricks notebooks (source format)
    ├── pipeline/*.py             ← Lakeflow pipeline source (demos 02 and 03 only)
    └── sample_data/              ← realistic messy input files
```

## How to run

1. In Databricks: **Workspace → Create → Git folder** and clone this repo (or `databricks bundle deploy`).
2. Open a demo's notebook, keep the default widgets (`workspace` catalog), and **Run all**. Each notebook creates its
   own schema (`autoloader_demo`, `dq_demo`, `scd_demo`, `dedup_demo`, `variant_demo`) and copies its sample
   data into a volume, so the demos never interfere with each other.
3. For the pipeline parts of demos 02 and 03, run the notebook first (it uploads the data), then:
   ```bash
   databricks bundle deploy -p <your-profile>
   databricks bundle run dq_transactions_pipeline -p <your-profile>
   databricks bundle run customers_auto_cdc_pipeline -p <your-profile>
   ```
   (`-p` is needed because your `~/.databrickscfg` has two profiles for the same host.)

## Requirements

- Unity Catalog workspace. **Databricks Free Edition works.**
- Serverless compute (or DBR 16.x+ for `VARIANT`, `variant_explode` and `try_variant_get`)

## Suggested order

01 → 02 → 04 → 03 → 05. Schema evolution and data quality are about **getting data in safely**; dedup and SCD
are about **keeping it correct over time**; VARIANT ties it back to the JSON demo at scale.
