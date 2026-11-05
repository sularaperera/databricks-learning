# Handling Messy JSON in Databricks

A hands-on demo of how real-world, messy JSON is handled with Databricks / Spark JSON options.
The same e-commerce **orders** data gets messier file by file, and the notebook fixes each problem.

## Contents

```
json_handling_demo/
├── notebooks/
│   └── 01_handling_messy_json.py   # Databricks notebook (source format)
└── sample_data/
    ├── 01_clean.json               # flat JSON Lines, the baseline
    ├── 02_nested.json              # nested structs + arrays
    ├── 03_multiline.json           # pretty-printed JSON array
    ├── 04_schema_drift.json        # added / renamed / re-cased fields
    ├── 05_type_mismatch.json       # mixed types, date formats, NaN, leading zeros
    ├── 06_corrupt_records.json     # truncated lines, comments, unquoted keys, bad escapes
    └── 07_json_string_column.csv   # JSON payload stored in a string column
```

## What you will learn

| Level | Problem | Key techniques |
|---|---|---|
| 1 | Clean JSON | `spark.read.json`, explicit schema, `prefersDecimal` |
| 2 | Nested JSON | dot notation, `explode_outer`, `posexplode_outer`, `inline_outer`, flatten helper |
| 3 | Multi-line JSON | `multiLine` |
| 4 | Schema drift | `dropFieldIfAllNull`, `spark.sql.caseSensitive`, `VARIANT` + `coalesce` |
| 5 | Type mismatches | `primitivesAsString`, `allowNumericLeadingZeros`, `allowNonNumericNumbers`, `try_cast`, `try_to_timestamp` |
| 6 | Corrupt records | `mode` (PERMISSIVE / DROPMALFORMED / FAILFAST), `columnNameOfCorruptRecord`, `badRecordsPath`, `allow*` leniency options, quarantine table |
| 7 | JSON in a string column | `get_json_object`, `:` path syntax, `from_json`, `schema_of_json`, `to_json`, `try_parse_json` |
| 8 | Production ingestion | Auto Loader with `schemaHints`, `schemaEvolutionMode=rescue`, `_rescued_data` |

## How to run

1. **Import the notebook**
   - Easiest: in Databricks go to **Workspace → Create → Git folder** and clone this repo. The notebook copies `sample_data/` into the volume automatically.
   - Or import `notebooks/01_handling_messy_json.py` manually (**Workspace → Import**).
2. **Upload sample data** (only needed if you imported manually). Upload the `sample_data/` files to
   `/Volumes/<catalog>/<schema>/<volume>/json_demo/sample_data/` via Catalog Explorer, or run:
   ```bash
   databricks fs cp -r ./sample_data dbfs:/Volumes/workspace/json_demo/raw/json_demo/sample_data
   ```
3. Set the `catalog` / `schema` / `volume` widgets (defaults: `workspace` / `json_demo` / `raw`) and **Run all**.

## Requirements

- Unity Catalog enabled workspace (Databricks **Free Edition** works)
- Serverless compute or DBR **15.3+** (needed for `VARIANT`, `parse_json`, `try_parse_json`)
- Some cells are compute-dependent and wrapped in `try/except`:
  - `spark.sql.caseSensitive` may not be settable on serverless
  - `badRecordsPath` may not be supported on every compute type
