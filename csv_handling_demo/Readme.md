# Handling Messy CSV in Databricks 🇳🇿

A hands-on demo of how real-world, messy CSV is handled with Databricks / Spark CSV options.
It follows **Kiwi Outdoors Ltd**, a New Zealand online retailer, as its **orders** data arrives from more and more
systems (Excel reports, a German supplier, a legacy courier system, MySQL dumps, a mainframe...) and gets messier
file by file. The notebook fixes each problem.

## Contents

```
csv_handling_demo/
├── notebooks/
│   └── 01_handling_messy_csv.py        # Databricks notebook (source format)
└── sample_data/
    ├── 01_clean.csv                    # the baseline
    ├── 02a_semicolon_european.csv      # ; separator, decimal comma 1.249,00, dd.MM.yyyy
    ├── 02b_pipe_no_header.txt          # | separator, no header row
    ├── 02c_tab_separated.tsv           # tabs (product names contain commas)
    ├── 03a_quotes_rfc.csv              # Excel style "" escaping + newlines inside values
    ├── 03b_quotes_backslash.csv        # MySQL style \" escaping
    ├── 04_report_export.csv            # Excel report: sep=, title lines, comments, TOTAL footer, CRLF
    ├── 05_nulls_whitespace.csv         # NULL, null, N/A, NA, #N/A, -, \N, NaN, padded spaces
    ├── 06_type_mess.csv                # NZ postcodes/phones with leading 0, dd/MM dates, NZ$1,249.50, +12:00
    ├── 07_bad_headers.csv              # spaces, symbols, dots, duplicate and empty column names
    ├── 08_monthly_drift/               # 3 monthly files: column added, reordered, renamed
    ├── 09_malformed_rows.csv           # too few / too many columns, bad type, unclosed quote
    ├── 10a_windows1252.csv             # Windows-1252, Māori macrons already lost as '?'
    ├── 10b_utf8_bom.csv                # UTF-8 with BOM (Excel "CSV UTF-8")
    ├── 10c_utf16_excel_unicode.txt     # UTF-16 LE (Excel "Unicode Text")
    └── 11_fixed_width.txt              # mainframe fixed-width with header/trailer records
```

## What you will learn

| Level | Problem | Key techniques |
|---|---|---|
| 1 | Clean CSV | `header`, `inferSchema` (and why to avoid it), explicit DDL schema, `read_files` |
| 2 | Delimiters | `sep`, `locale` for decimal commas, `dateFormat`, schema/`toDF` for headerless files |
| 3 | Quotes & newlines | `quote`, `escape` (Excel vs MySQL style), `multiLine` |
| 4 | Report junk lines | `skipRows`, `comment`, `try_cast` filtering, `spark.read.text` + `from_csv`, control totals |
| 5 | Null tokens & spaces | `nullValue`, `ignoreLeading/TrailingWhiteSpace`, `NaN` gotcha, reusable `normalise_nulls()` |
| 6 | Type mess (NZ) | leading zeros lost by inference, `try_cast`, `try_to_timestamp` with many formats, dd/MM vs MM/dd, NZ phone → E.164, money strings, `Pacific/Auckland` timezone |
| 7 | Bad headers | duplicate/empty header handling, backticks for dots, Delta invalid characters, `clean_column_names()` |
| 8 | Drift across files | **folder reads map by position**, `enforceSchema=false`, `unionByName(allowMissingColumns)`, `read_files` |
| 9 | Malformed rows | `mode` (PERMISSIVE / DROPMALFORMED / FAILFAST), `_corrupt_record`, `count()` gotcha, unclosed quote + `multiLine`, quarantine table, `badRecordsPath` |
| 10 | Encodings | `encoding` (`windows-1252`, `UTF-16` + `multiLine`), BOM, unrecoverable macron loss |
| 11 | Fixed width | `spark.read.text` + `substring`, trailer record count check |
| 12 | Production ingestion | Auto Loader for CSV: by-name matching, `schemaHints`, `addNewColumns` restart, `_rescued_data` |

## How to run

1. **Import the notebook**
   - Easiest: in Databricks go to **Workspace → Create → Git folder** and clone this repo. The notebook copies
     `sample_data/` (including the `08_monthly_drift/` folder) into the volume automatically.
   - Or import `notebooks/01_handling_messy_csv.py` manually (**Workspace → Import**).
2. **Upload sample data** (only needed if you imported manually). From the `databricks-learning` folder:
   ```bash
   databricks fs cp -r --overwrite ./csv_handling_demo/sample_data dbfs:/Volumes/workspace/csv_demo/raw/csv_demo/sample_data --profile DEFAULT
   ```
3. Set the `catalog` / `schema` / `volume` widgets (defaults: `workspace` / `csv_demo` / `raw`) and **Run all**.

## Requirements

- Unity Catalog enabled workspace (Databricks **Free Edition** works)
- Serverless compute (tested end to end on serverless, DBR 19) or a recent DBR
- `badRecordsPath` is wrapped in `try/except` because it may not be supported on every compute type

## Notes

- `sample_data/.gitattributes` stops git from "fixing" line endings and encodings. The mess is the point!
- Several cells **intentionally** show wrong data or errors (marked ❌). Read the markdown above each one.
