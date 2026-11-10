# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Handling Real-World Messy CSV in Databricks 🇳🇿
# MAGIC
# MAGIC CSV looks like the simplest format there is. It isn't. There is **no single CSV standard**, no types,
# MAGIC no schema, and every system that exports it (Excel, ERPs, MySQL dumps, mainframes...) does it a little differently.
# MAGIC
# MAGIC This notebook follows **Kiwi Outdoors Ltd**, a New Zealand online retailer, as its `orders` data arrives from
# MAGIC more and more systems and gets messier file by file. For each problem you'll see the ❌ naive read and the ✅ fix.
# MAGIC
# MAGIC | # | File | Problem |
# MAGIC |---|------|---------|
# MAGIC | 1 | `01_clean.csv` | Nothing. The baseline. Header, inference, explicit schema |
# MAGIC | 2 | `02a/02b/02c` | `;` + decimal comma (German supplier), `\|` with no header, tab separated |
# MAGIC | 3 | `03a/03b` | Commas, quotes and **newlines inside values**, two different escape styles |
# MAGIC | 4 | `04_report_export.csv` | Excel report: title lines, blank lines, comments, TOTAL footer |
# MAGIC | 5 | `05_nulls_whitespace.csv` | `NULL`, `null`, `N/A`, `-`, `\N`, `NaN`, padded spaces |
# MAGIC | 6 | `06_type_mess.csv` | NZ postcodes & phones losing leading zeros, `dd/MM` dates, `NZ$1,249.50`, timezones |
# MAGIC | 7 | `07_bad_headers.csv` | Spaces, symbols, dots, duplicates, empty column names |
# MAGIC | 8 | `08_monthly_drift/` | Columns added / reordered / renamed between monthly files |
# MAGIC | 9 | `09_malformed_rows.csv` | Too few / too many columns, bad types, unclosed quote |
# MAGIC | 10 | `10a/10b/10c` | Encodings: Windows-1252 (and lost Māori macrons), UTF-8 BOM, UTF-16 |
# MAGIC | 11 | `11_fixed_width.txt` | Not CSV at all: mainframe fixed-width with header/trailer records |
# MAGIC | 12 | `read_files` + Auto Loader | Production pattern: schema evolution + `_rescued_data` |
# MAGIC
# MAGIC **Requirements:** Unity Catalog workspace (Databricks Free Edition works), serverless or a recent DBR.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Setup: catalog, schema, volume
# MAGIC
# MAGIC Change the widgets at the top if you want a different location.
# MAGIC On Databricks Free Edition the default catalog is `workspace`.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "csv_demo", "Schema")
dbutils.widgets.text("volume", "raw", "Volume")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")
spark.sql(f"USE {catalog}.{schema}")

base_path = f"/Volumes/{catalog}/{schema}/{volume}/csv_demo"
data_path = f"{base_path}/sample_data"
print(f"Sample data path: {data_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get the sample files into the volume
# MAGIC
# MAGIC **Option A (automatic):** if you cloned this repo as a **Databricks Git folder**, the cell below copies
# MAGIC `../sample_data/` (including the `08_monthly_drift/` sub-folder) from the repo into the volume.
# MAGIC
# MAGIC **Option B (manual):** use the Databricks CLI from the `databricks-learning` folder:
# MAGIC
# MAGIC `databricks fs cp -r --overwrite ./csv_handling_demo/sample_data dbfs:/Volumes/workspace/csv_demo/raw/csv_demo/sample_data --profile DEFAULT`
# MAGIC
# MAGIC (Or Catalog Explorer → your volume → **Upload to this volume**.)

# COMMAND ----------

import os
import shutil

repo_sample_dir = os.path.abspath(os.path.join(os.getcwd(), "..", "sample_data"))

if os.path.isdir(repo_sample_dir):
    shutil.copytree(repo_sample_dir, data_path, dirs_exist_ok=True)
    print(f"copied {repo_sample_dir} -> {data_path}")
else:
    print(f"{repo_sample_dir} not found, so upload the files manually (Option B).")

display(dbutils.fs.ls(data_path))

# COMMAND ----------

import re
from functools import reduce
from pyspark.sql import functions as F

def p(file_name):
    """Full path of a sample file in the volume."""
    return f"{data_path}/{file_name}"

def show_raw(file_name, n=20):
    """Print the raw lines of a file so we can SEE the mess before parsing it.
    repr() makes hidden characters visible: tabs (\\t), trailing spaces, \\r, BOM (\\ufeff)."""
    with open(p(file_name), "rb") as f:
        for line in f.read().decode("utf-8", errors="replace").splitlines()[:n]:
            print(repr(line))

def show_bytes(file_name, n=120):
    """Print the first raw bytes. Useful when the problem is the encoding itself."""
    with open(p(file_name), "rb") as f:
        print(f.read(n))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. Clean CSV (the baseline)

# COMMAND ----------

show_raw("01_clean.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ### ❌ Mistake #1: forgetting `header`
# MAGIC Spark's CSV reader defaults to `header = false`, so the header becomes a **data row** and the columns are `_c0, _c1...`

# COMMAND ----------

display(spark.read.csv(p("01_clean.csv")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### `header = true` → good names, but **every column is a STRING**
# MAGIC CSV has no types. Without help, Spark treats everything as text.

# COMMAND ----------

df = spark.read.option("header", True).csv(p("01_clean.csv"))
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### `inferSchema = true` → types, but at a cost
# MAGIC * Spark **reads the whole file an extra time** just to guess types (slow on big data, `samplingRatio` makes it faster but riskier)
# MAGIC * `price` becomes **double**: `39.90` shows as `39.9`, and floating point is wrong for money
# MAGIC * Guesses change when the data changes, so your pipeline's schema is **not stable**
# MAGIC * (In section 6 you'll see inference **destroy** NZ postcodes and phone numbers 😱)

# COMMAND ----------

df_inferred = spark.read.option("header", True).option("inferSchema", True).csv(p("01_clean.csv"))
df_inferred.printSchema()
display(df_inferred)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ In production: declare the schema (DDL string)
# MAGIC One read pass, exact types, `DECIMAL` for money.

# COMMAND ----------

orders_schema = """
    order_id      INT,
    customer_id   STRING,
    customer_name STRING,
    city          STRING,
    order_date    DATE,
    product       STRING,
    quantity      INT,
    price         DECIMAL(10,2),
    status        STRING
"""

df_clean = (
    spark.read
    .option("header", True)
    .option("dateFormat", "yyyy-MM-dd")
    .schema(orders_schema)
    .csv(p("01_clean.csv"))
)
df_clean.printSchema()
display(df_clean)

# COMMAND ----------

# MAGIC %md
# MAGIC 💡 **SQL alternative:** `read_files` does the same in pure SQL (and we will use it again in section 12).

# COMMAND ----------

display(spark.sql(f"""
    SELECT * FROM read_files('{p("01_clean.csv")}', format => 'csv', header => true, schemaHints => 'price DECIMAL(10,2)')
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. Delimiters, missing headers, decimal commas
# MAGIC
# MAGIC "CSV" really means "**C**haracter **S**eparated **V**alues". The separator can be anything.
# MAGIC
# MAGIC ### 2a. A German supplier's invoice: `;` separator, `1.249,00` decimal comma, `dd.MM.yyyy` dates
# MAGIC Europe uses `,` as the decimal mark, so they separate columns with `;` instead.

# COMMAND ----------

show_raw("02a_semicolon_european.csv")

# COMMAND ----------

# ❌ Default separator is ',' → one giant column, and every "89,99" got split in half
display(spark.read.option("header", True).csv(p("02a_semicolon_european.csv")))

# COMMAND ----------

# ✅ Fix 1: sep=';' fixes the columns, BUT price "1.249,00" is still a string
display(spark.read.option("header", True).option("sep", ";").csv(p("02a_semicolon_european.csv")))

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ **Fix 2:** the `locale` option teaches the parser how numbers are written there.
# MAGIC With `de-DE`, `1.249,00` becomes `1249.00` when the column is declared `DECIMAL`.

# COMMAND ----------

df_eu = (
    spark.read
    .option("header", True)
    .option("sep", ";")
    .option("locale", "de-DE")
    .option("dateFormat", "dd.MM.yyyy")
    .schema("order_id INT, supplier_sku STRING, order_date DATE, product STRING, quantity INT, price DECIMAL(10,2), status STRING")
    .csv(p("02a_semicolon_european.csv"))
)
display(df_eu)

# COMMAND ----------

# The portable alternative (works anywhere): land as string, then clean in silver
display(
    spark.read.option("header", True).option("sep", ";").csv(p("02a_semicolon_european.csv"))
    .select(
        "order_id",
        "price",
        F.regexp_replace(F.regexp_replace("price", r"\.", ""), ",", ".").cast("decimal(10,2)").alias("price_clean"),
        F.to_date("order_date", "dd.MM.yyyy").alias("order_date"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2b. A legacy courier system: `|` separated and **no header row**

# COMMAND ----------

show_raw("02b_pipe_no_header.txt")

# COMMAND ----------

# ✅ With a schema, columns are named BY POSITION, and no header is needed
courier_schema = """
    order_id INT, customer_id STRING, customer_name STRING, city STRING,
    order_date DATE, product STRING, quantity INT, price DECIMAL(10,2), status STRING
"""
display(spark.read.option("sep", "|").schema(courier_schema).csv(p("02b_pipe_no_header.txt")))

# COMMAND ----------

# Or read as strings and rename with toDF(...)
display(
    spark.read.option("sep", "|").csv(p("02b_pipe_no_header.txt"))
    .toDF("order_id", "customer_id", "customer_name", "city", "order_date", "product", "quantity", "price", "status")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2c. Tab separated (TSV): chosen because product names contain commas
# MAGIC `repr()` shows the tabs as `\t`. Multi-character separators (e.g. `sep="||"` or `"~|~"`) are also supported.

# COMMAND ----------

show_raw("02c_tab_separated.tsv")
display(spark.read.option("header", True).option("sep", "\t").csv(p("02c_tab_separated.tsv")))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. Quotes, commas and newlines inside values
# MAGIC
# MAGIC Customer reviews are free text, so they contain everything: commas, quotes, line breaks.
# MAGIC The CSV way to protect them is to wrap the value in `"..."`. But **how a `"` inside a quoted value is escaped
# MAGIC depends on who wrote the file**:
# MAGIC
# MAGIC | Style | Example | Who writes it |
# MAGIC |---|---|---|
# MAGIC | RFC 4180 (doubled quotes) | `"She said ""best gift ever"""` | Excel, pandas, most tools |
# MAGIC | Backslash escape | `"She said \"best gift ever\""` | MySQL / Postgres dumps, some apps |
# MAGIC
# MAGIC ⚠️ Spark's default `escape` is `\` (backslash), so **Excel-style files are parsed wrongly by default**.

# COMMAND ----------

show_raw("03a_quotes_rfc.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ### ❌ Default read
# MAGIC * Commas inside `"Ngata, Aroha"` are fine ✅
# MAGIC * The **newline** inside order 3003's review splits it into 2 broken rows (`Would buy again` lands in `order_id`!)
# MAGIC * The doubled quotes in `"Pounamu ""Toki"" Pendant"` are left in the value

# COMMAND ----------

display(spark.read.option("header", True).csv(p("03a_quotes_rfc.csv")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### ❌ Only `multiLine = true`
# MAGIC 3003 is fixed, but **3006 is still broken**. Its review has a `""` right before the line break, and with the wrong
# MAGIC escape character the parser loses track of where the quoted value ends.

# COMMAND ----------

display(spark.read.option("header", True).option("multiLine", True).csv(p("03a_quotes_rfc.csv")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ Fix: `multiLine = true` + `escape = '"'`
# MAGIC ⚠️ Trade-off: a `multiLine` file **cannot be split** across tasks (Spark can't know where a row starts in the
# MAGIC middle of a file), so one huge file = one task. Ask the source to strip newlines from free text if files are big.
# MAGIC
# MAGIC Also notice order 3005: an empty quoted value `""` is read as **NULL**, the same as a missing value.

# COMMAND ----------

df_quotes = (
    spark.read
    .option("header", True)
    .option("multiLine", True)
    .option("quote", '"')     # the default, shown for clarity
    .option("escape", '"')    # RFC 4180 / Excel style: "" inside quotes
    .csv(p("03a_quotes_rfc.csv"))
)
display(df_quotes)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3b. Backslash-escaped file (MySQL dump)
# MAGIC Here the **default works**, and setting `escape='"'` would actually **break** it.
# MAGIC Lesson: there is no universal setting. **Look at the raw file** and set `escape` to match the source.

# COMMAND ----------

show_raw("03b_quotes_backslash.csv")

# COMMAND ----------

print("✅ default escape (backslash):")
display(spark.read.option("header", True).csv(p("03b_quotes_backslash.csv")))

print("❌ escape='\"' on a backslash file (review is cut at the comma, ' 5 stars' lands in price):")
display(spark.read.option("header", True).option("escape", '"').csv(p("03b_quotes_backslash.csv")))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. Excel reports: junk lines above and below the data
# MAGIC
# MAGIC Business users "export to CSV" from a report, so you get:
# MAGIC * a `sep=,` hint line (Excel-only trick), a title, a generated date and a region line
# MAGIC * a blank line, then the real header
# MAGIC * a `# comment` line, a blank line in the middle
# MAGIC * an empty `,,,,` row, a `TOTAL` row and a `Rows: 4` footer
# MAGIC * Windows line endings (`\r\n`)

# COMMAND ----------

with open(p("04_report_export.csv"), "rb") as f:
    for line in f.read().decode("utf-8").split("\n"):
        print(repr(line))

# COMMAND ----------

# ❌ The first line "sep=," became the header, and everything is garbage
display(spark.read.option("header", True).csv(p("04_report_export.csv")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ Fix 1 (Databricks): `skipRows` + `comment`
# MAGIC * `skipRows = 5` skips the first 5 physical lines (blank lines count). The next line becomes the header
# MAGIC * `comment = '#'` drops lines starting with `#`
# MAGIC * Blank lines are skipped automatically
# MAGIC
# MAGIC The footer rows are still there, so filter them in silver: **a row is real only if its key parses**.

# COMMAND ----------

df_report = (
    spark.read
    .option("header", True)
    .option("skipRows", 5)
    .option("comment", "#")
    .csv(p("04_report_export.csv"))
)
display(df_report)   # note the all-NULL row, the TOTAL row and the "Rows: 4" row

# COMMAND ----------

df_report_clean = (
    df_report
    .filter("try_cast(order_id AS INT) IS NOT NULL")      # drops ',,,,', 'TOTAL', 'Rows: 4'
    .selectExpr(
        "CAST(order_id AS INT)                 AS order_id",
        "customer_id",
        "CAST(order_date AS DATE)              AS order_date",
        "CAST(amount_nzd AS DECIMAL(10,2))     AS amount_nzd",
        "status",
    )
)
display(df_report_clean)

# COMMAND ----------

# MAGIC %md
# MAGIC 💡 **Control total check:** the report gave us a TOTAL. Use it! If the sum doesn't match, the file is incomplete.

# COMMAND ----------

report_total = df_report.filter("order_id = 'TOTAL'").select(F.col("amount_nzd").cast("decimal(10,2)")).first()[0]
our_total = df_report_clean.agg(F.sum("amount_nzd")).first()[0]
print(f"report says {report_total}, we loaded {our_total} -> {'✅ match' if report_total == our_total else '❌ MISMATCH'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ Fix 2 (works on any Spark): read as text, keep only data lines, parse with `from_csv`
# MAGIC `from_csv` is the CSV twin of `from_json`: it parses a **string column** using a schema.
# MAGIC Great when the file layout is so weird that no reader option fits.

# COMMAND ----------

df_report_text = (
    spark.read.text(p("04_report_export.csv"))
    .filter(F.col("value").rlike(r"^[0-9]+,"))     # data lines start with a numeric order_id
    .select(F.from_csv("value", "order_id INT, customer_id STRING, order_date DATE, amount_nzd DECIMAL(10,2), status STRING").alias("r"))
    .select("r.*")
)
display(df_report_text)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5. NULLs in disguise & whitespace
# MAGIC
# MAGIC Different systems write "nothing" in different ways: empty, `NULL`, `null`, `N/A`, `NA`, `#N/A` (Excel), `-`,
# MAGIC `\N` (MySQL/Hive), `NaN`, `none`, or just spaces.

# COMMAND ----------

show_raw("05_nulls_whitespace.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ### ❌ Default read
# MAGIC Only a truly **empty** value (`,,`) becomes NULL. Everything else is a "real" string, and the padded spaces stay.
# MAGIC (Brackets added so you can see the spaces.)

# COMMAND ----------

def bracket_all(df):
    """Wrap every value in [ ] so leading/trailing spaces are visible."""
    return df.select([F.concat(F.lit("["), F.col(c), F.lit("]")).alias(c) for c in df.columns])

df_nulls_raw = spark.read.option("header", True).csv(p("05_nulls_whitespace.csv"))
display(bracket_all(df_nulls_raw))

# COMMAND ----------

# MAGIC %md
# MAGIC ### The reader options (and their limits)
# MAGIC | Option | What it does | Limit |
# MAGIC |---|---|---|
# MAGIC | `nullValue` | this string → NULL | only **one** value |
# MAGIC | `ignoreLeadingWhiteSpace` / `ignoreTrailingWhiteSpace` | trim **unquoted** values | quoted `"  x  "` is kept as is |
# MAGIC | `nanValue`, `positiveInf`, `negativeInf` | how NaN / ±Infinity are written | for float/double columns |
# MAGIC | `emptyValue` | what an empty quoted `""` means | |

# COMMAND ----------

display(bracket_all(
    spark.read
    .option("header", True)
    .option("nullValue", "NULL")                 # only 'NULL' is handled; 'null', 'N/A', '-' are still strings
    .option("ignoreLeadingWhiteSpace", True)
    .option("ignoreTrailingWhiteSpace", True)
    .csv(p("05_nulls_whitespace.csv"))
))

# COMMAND ----------

# MAGIC %md
# MAGIC ### ⚠️ Gotcha: with a schema, PERMISSIVE mode silently turns bad values into NULL, and `NaN` is NOT null
# MAGIC * `price = NaN` → a real **NaN** double (it will poison every `SUM`/`AVG`!)
# MAGIC * `price = \N` and `quantity = NA` → NULL, **with no warning at all**

# COMMAND ----------

df_nan = (
    spark.read.option("header", True)
    .schema("order_id INT, customer_id STRING, city STRING, quantity INT, price DOUBLE, notes STRING")
    .csv(p("05_nulls_whitespace.csv"))
)
display(df_nan)
display(df_nan.select(F.sum("price").alias("sum_price_is_NaN"), F.count_if(F.isnan("price")).alias("nan_rows")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ Fix: land as strings, normalise **all** null tokens in one place
# MAGIC A small reusable function beats a dozen reader options.

# COMMAND ----------

NULL_TOKENS = ["", "null", "none", "n/a", "na", "#n/a", "-", "\\n", "nan", "nil"]

def normalise_nulls(df, tokens=NULL_TOKENS):
    """Trim every string column and turn any 'null-looking' token into a real NULL."""
    out = []
    for field in df.schema.fields:
        c = F.trim(F.col(field.name))
        if field.dataType.simpleString() == "string":
            out.append(F.when(F.lower(c).isin(tokens), None).otherwise(c).alias(field.name))
        else:
            out.append(F.col(field.name))
    return df.select(out)

df_nulls_clean = normalise_nulls(df_nulls_raw).selectExpr(
    "try_cast(order_id AS INT)            AS order_id",
    "customer_id",
    "city",
    "try_cast(quantity AS INT)            AS quantity",
    "try_cast(price AS DECIMAL(10,2))     AS price",
    "notes",
)
display(bracket_all(df_nulls_clean))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 6. Type mess: the New Zealand edition 🥝
# MAGIC
# MAGIC | Column | Mess |
# MAGIC |---|---|
# MAGIC | `order_id` | `00123`: leading zeros matter (it's an ID, not a number) |
# MAGIC | `postcode` | NZ postcodes are 4 digits and can **start with 0**: `0110` Whangārei, `0632` Albany |
# MAGIC | `phone` | NZ numbers start with `0` (`021` mobile, `09` Auckland, `03` South Island) or `64` (international) |
# MAGIC | `order_date` | `2026-06-01`, NZ style `02/06/2026` (dd/MM!), `2026/06/03`, `4 Jun 2026`, `20260605`, invalid `31/13/2026` |
# MAGIC | `created_at` | local time, `+12:00` (NZST) offset, epoch seconds, NZ style `05/06/2026 2:30 pm`, invalid `25:61:00` |
# MAGIC | `quantity` | `2`, `1.0`, `1e2`, `three` |
# MAGIC | `price` | `$25.99`, `NZ$1,249.50`, `119`, `(15.00)` (accounting negative), `NZD 39.99`, `N/A` |
# MAGIC | `discount_pct` | `10%`, `12.5%`, empty, `abc` |
# MAGIC | `is_gift` | `Y`, `N`, `true`, `FALSE`, `1`, `yes` |
# MAGIC | `tracking_no` | `1.23457E+11`: Excel already destroyed this number before we got it |

# COMMAND ----------

show_raw("06_type_mess.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ### ❌ `inferSchema` silently destroys data
# MAGIC * `order_id` `00123` → `123`
# MAGIC * `postcode` `0110` → `110` (no longer a valid NZ postcode!)
# MAGIC * `phone` `0211234567` → `211234567`
# MAGIC * `tracking_no` → `double`, shown as `1.23456789013E11`
# MAGIC
# MAGIC No error, no warning. 💀 **Never use `inferSchema` for identifiers.**

# COMMAND ----------

df_types_inferred = spark.read.option("header", True).option("inferSchema", True).csv(p("06_type_mess.csv"))
df_types_inferred.printSchema()
display(df_types_inferred)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ❌ `dateFormat` accepts only ONE format
# MAGIC Only the row that matches `dd/MM/yyyy` parses. The other 5 dates silently become NULL.

# COMMAND ----------

display(
    spark.read.option("header", True).option("dateFormat", "dd/MM/yyyy")
    .schema("order_id STRING, customer_id STRING, postcode STRING, phone STRING, order_date DATE")
    .csv(p("06_type_mess.csv"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ⚠️ The dd/MM vs MM/dd trap
# MAGIC `05/06/2026` is **5 June** in NZ and **May 6** in the US. Both formats parse it **successfully**, so a wrong
# MAGIC format gives **wrong dates, not errors**. Decide the format **per source system**, and document it.

# COMMAND ----------

display(spark.sql("""
    SELECT '05/06/2026'                                 AS raw,
           to_date('05/06/2026', 'dd/MM/yyyy')          AS nz_reading,
           to_date('05/06/2026', 'MM/dd/yyyy')          AS us_reading
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ Fix: bronze as STRING, then type defensively in silver
# MAGIC * `try_cast` / `try_to_timestamp` return **NULL instead of failing** the job (serverless runs in ANSI mode,
# MAGIC   where a plain `CAST('N/A' AS INT)` throws an error)
# MAGIC * `coalesce(try_to_timestamp(x, fmt1), try_to_timestamp(x, fmt2), ...)`: the first format that parses wins
# MAGIC * Keep the raw value next to the clean value while you validate

# COMMAND ----------

df_types_raw = spark.read.option("header", True).csv(p("06_type_mess.csv"))   # everything STRING

df_types_clean = (
    df_types_raw
    .withColumn("price_digits", F.regexp_replace("price", "[^0-9.]", ""))
    .withColumn("phone_digits", F.regexp_replace("phone", "[^0-9]", ""))
    .selectExpr(
        # identifiers stay STRING: leading zeros preserved
        "order_id",
        "customer_id",
        "postcode",
        "postcode RLIKE '^[0-9]{4}$'                                   AS postcode_is_valid",

        # NZ phone numbers -> E.164 (+64...)
        """CASE WHEN phone_digits LIKE '0%'  THEN concat('+64', substr(phone_digits, 2))
                WHEN phone_digits LIKE '64%' THEN concat('+', phone_digits)
           END                                                          AS phone_e164""",

        # many date formats. NZ source = dd/MM, never MM/dd
        """to_date(coalesce(
                try_to_timestamp(order_date, 'yyyy-MM-dd'),
                try_to_timestamp(order_date, 'dd/MM/yyyy'),
                try_to_timestamp(order_date, 'yyyy/MM/dd'),
                try_to_timestamp(order_date, 'd MMM yyyy'),
                try_to_timestamp(order_date, 'yyyyMMdd')
           ))                                                           AS order_date""",

        # epoch seconds / NZ am-pm style / ISO (with or without offset)
        """CASE WHEN created_at RLIKE '^[0-9]{10}$'
                THEN timestamp_seconds(try_cast(created_at AS BIGINT))
                ELSE coalesce(
                    try_to_timestamp(created_at, 'dd/MM/yyyy h:mm a'),
                    try_cast(created_at AS TIMESTAMP))
           END                                                          AS created_at_utc""",

        # "1.0" -> 1, "1e2" -> 100, "three" -> NULL
        "try_cast(try_cast(quantity AS DOUBLE) AS INT)                 AS quantity",

        # strip $, NZ$, NZD, commas; (15.00) means -15.00 in accounting
        """CASE WHEN price LIKE '(%)' THEN -1 ELSE 1 END
           * try_cast(nullif(price_digits, '') AS DECIMAL(18,2))       AS price_nzd""",

        "try_cast(replace(discount_pct, '%', '') AS DECIMAL(5,2)) / 100 AS discount",

        """CASE WHEN lower(is_gift) IN ('true', 'y', 'yes', '1')  THEN true
                WHEN lower(is_gift) IN ('false', 'n', 'no', '0') THEN false
           END                                                          AS is_gift""",

        "tracking_no",
        "tracking_no RLIKE '[eE]'                                      AS tracking_no_corrupted_by_excel",

        # raw values kept for validation
        "order_date AS order_date_raw",
        "created_at AS created_at_raw",
        "price      AS price_raw",
    )
)
df_types_clean.printSchema()
display(df_types_clean)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🕐 Timezones: store UTC, show NZ time
# MAGIC `2026-06-02T11:42:10+12:00` was converted to **UTC** (`2026-06-01 23:42:10`, the previous day!).
# MAGIC Serverless sessions run in UTC. Store UTC, and convert to `Pacific/Auckland` for reporting.
# MAGIC Use the **region name**, not a fixed `+12:00`, so daylight saving (NZDT `+13:00`, late Sep to early Apr) is handled.

# COMMAND ----------

display(
    df_types_clean.select(
        "order_id",
        "created_at_raw",
        "created_at_utc",
        F.from_utc_timestamp("created_at_utc", "Pacific/Auckland").alias("created_at_nz"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC 💡 **Data-quality metrics:** count what failed to parse and send it back to the source team.

# COMMAND ----------

display(
    df_types_clean.select(
        F.count_if(F.col("order_date").isNull() & F.col("order_date_raw").isNotNull()).alias("bad_dates"),
        F.count_if(F.col("created_at_utc").isNull() & F.col("created_at_raw").isNotNull()).alias("bad_timestamps"),
        F.count_if(F.col("price_nzd").isNull() & F.col("price_raw").isNotNull()).alias("bad_prices"),
        F.count_if(~F.col("postcode_is_valid")).alias("bad_postcodes"),
        F.count_if("tracking_no_corrupted_by_excel").alias("tracking_nos_lost_to_excel"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 7. Bad column names
# MAGIC
# MAGIC Headers typed by humans: `Order ID`, `Customer-ID`, ` Customer Name ` (padded), `Unit Price (NZ$)`,
# MAGIC `Qty` **and** `qty`, `Ship.Date`, an **empty** name, `% Discount`, `Region / Rohe`.

# COMMAND ----------

show_raw("07_bad_headers.csv")

# COMMAND ----------

df_headers = spark.read.option("header", True).csv(p("07_bad_headers.csv"))
print(df_headers.columns)
display(df_headers)

# COMMAND ----------

# MAGIC %md
# MAGIC What Spark did on its own:
# MAGIC * `Qty` / `qty` are duplicates (Spark is case-insensitive) → renamed to **`Qty4`, `qty5`** (column position appended)
# MAGIC * the empty header → **`_c7`**
# MAGIC * spaces, `(`, `)`, `%`, `/` are kept as they are
# MAGIC
# MAGIC ### ❌ Problem 1: dots in names. `col("Ship.Date")` means "field `Date` of struct `Ship`"

# COMMAND ----------

try:
    df_headers.select("Ship.Date").show()
except Exception as e:
    print("ERROR:", str(e)[:200])

display(df_headers.select("`Ship.Date`"))   # backticks work... but you'll forget them somewhere

# COMMAND ----------

# MAGIC %md
# MAGIC ### ❌ Problem 2: Delta rejects spaces and `,;{}()=` in column names

# COMMAND ----------

try:
    df_headers.write.mode("overwrite").saveAsTable("orders_bad_headers")
except Exception as e:
    print("ERROR:", str(e)[:400])

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ Fix: sanitise every header into `snake_case` in bronze
# MAGIC (The alternative is Delta **column mapping**, `TBLPROPERTIES ('delta.columnMapping.mode' = 'name')`, which allows
# MAGIC any name. But clean names are easier for everyone downstream: SQL users, BI tools, ML.)

# COMMAND ----------

def clean_column_names(df):
    """'Unit Price (NZ$)' -> 'unit_price_nz', '% Discount' -> 'pct_discount', '' -> 'col_7'. Duplicates get a suffix."""
    seen, new_names = {}, []
    for i, name in enumerate(df.columns):
        n = name.strip().lower().replace("%", "pct")
        n = re.sub(r"[^0-9a-z]+", "_", n).strip("_")
        if not n or re.fullmatch(r"c\d+", n):        # empty header (Spark named it _c7)
            n = f"col_{i}"
        if n in seen:
            seen[n] += 1
            n = f"{n}_{seen[n]}"
        else:
            seen[n] = 0
        new_names.append(n)
    return df.toDF(*new_names)       # toDF renames BY POSITION, so weird names are never parsed

df_headers_clean = clean_column_names(df_headers)
print(list(zip(df_headers.columns, df_headers_clean.columns)))

df_headers_clean.write.mode("overwrite").saveAsTable("orders_clean_headers")
display(spark.table("orders_clean_headers"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 8. Schema drift across monthly files 📅
# MAGIC
# MAGIC Kiwi Outdoors drops one file per month into the same folder. The source team "just made small changes":
# MAGIC
# MAGIC | File | Header |
# MAGIC |---|---|
# MAGIC | `orders_2026_01.csv` | `order_id, customer_id, order_date, amount, status` |
# MAGIC | `orders_2026_02.csv` | `order_id, customer_id, `**`channel`**`, order_date, amount, status` (new column **in the middle**) |
# MAGIC | `orders_2026_03.csv` | `order_id, `**`order_date, customer_id`**`, `**`total_amount_nzd`**`, status, `**`region`** (reordered + renamed + added) |

# COMMAND ----------

for f in dbutils.fs.ls(p("08_monthly_drift")):
    print(f"--- {f.name}")
    show_raw(f"08_monthly_drift/{f.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### ❌ The most dangerous CSV bug: reading the folder in one go
# MAGIC Spark takes the header from **one** file and then maps every file's values **by position**, not by name.
# MAGIC Look at February: `web` landed in `customer_id`, the date landed in `total_amount_nzd`...
# MAGIC **No error. Completely wrong data.** 💀

# COMMAND ----------

display(
    spark.read.option("header", True).csv(p("08_monthly_drift"))
    .withColumn("_source_file", F.col("_metadata.file_name"))     # always capture lineage!
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🛡️ Guard rail: `enforceSchema = false`
# MAGIC By default (`enforceSchema = true`) Spark **ignores the header names** when you give it a schema.
# MAGIC With `false`, it checks every file's header against your schema and **fails** when they don't match.
# MAGIC Failing loudly beats loading garbage.

# COMMAND ----------

try:
    (spark.read.option("header", True)
     .option("enforceSchema", False)
     .schema("order_id INT, customer_id STRING, order_date DATE, amount DECIMAL(10,2), status STRING")
     .csv(p("08_monthly_drift"))
     .collect())
except Exception as e:
    print("FAILED (good!):", str(e)[:300])

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ Fix 1 (plain Spark): read each file on its own, then `unionByName(allowMissingColumns=True)`
# MAGIC Each file is matched **by its own header**. Fine for a handful of files. For thousands, use fix 2 or Auto Loader.

# COMMAND ----------

monthly_files = sorted(f.path for f in dbutils.fs.ls(p("08_monthly_drift")) if f.name.endswith(".csv"))

monthly_dfs = [
    spark.read.option("header", True).csv(path).withColumn("_source_file", F.col("_metadata.file_name"))
    for path in monthly_files
]
df_union = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), monthly_dfs)
display(df_union)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ Fix 2 (Databricks): `read_files` matches columns by header name
# MAGIC It also merges the schemas of all files and adds a `_rescued_data` column for anything it can't fit.

# COMMAND ----------

df_read_files = spark.sql(f"""
    SELECT *, _metadata.file_name AS _source_file
    FROM read_files('{p("08_monthly_drift")}', format => 'csv', header => true)
""")
df_read_files.printSchema()
display(df_read_files)

# COMMAND ----------

# Silver: reconcile the renamed column and fill the gaps
display(
    df_read_files.selectExpr(
        "order_id",
        "customer_id",
        "order_date",
        "CAST(coalesce(amount, total_amount_nzd) AS DECIMAL(10,2))   AS amount_nzd",
        "status",
        "coalesce(channel, 'unknown')                                 AS channel",
        "region",
        "_source_file",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 9. Malformed rows & parse modes
# MAGIC
# MAGIC | order | problem |
# MAGIC |---|---|
# MAGIC | 9001 | ✅ fine |
# MAGIC | 9002 | **too few** columns (status missing) |
# MAGIC | 9003 | **too many** columns |
# MAGIC | 9004 | `amount = not_a_number` |
# MAGIC | 9005 | **unclosed quote** `"C005,...` |
# MAGIC | 9006, 9007 | ✅ fine |

# COMMAND ----------

show_raw("09_malformed_rows.csv")

# COMMAND ----------

malformed_schema = """
    order_id        INT,
    customer_id     STRING,
    amount          DECIMAL(10,2),
    status          STRING,
    _corrupt_record STRING
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mode 1: `PERMISSIVE` (default)
# MAGIC Bad rows are **kept**: what can be parsed is filled in, and the raw line goes into `_corrupt_record`.
# MAGIC
# MAGIC ⚠️ `_corrupt_record` only exists if **you put it in the schema**. Without it (next cell), 9002 and 9003 look like
# MAGIC perfectly normal rows: the missing status is NULL and the extra values are silently **thrown away**.

# COMMAND ----------

df_permissive = (
    spark.read.option("header", True)
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .schema(malformed_schema)
    .csv(p("09_malformed_rows.csv"))
)
display(df_permissive)

# COMMAND ----------

# The same read WITHOUT _corrupt_record in the schema: you can't tell anything is wrong
display(
    spark.read.option("header", True)
    .schema("order_id INT, customer_id STRING, amount DECIMAL(10,2), status STRING")
    .csv(p("09_malformed_rows.csv"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mode 2: `DROPMALFORMED`
# MAGIC Bad rows are **silently dropped**. ⚠️ Dangerous: you lose data and nobody knows.
# MAGIC
# MAGIC 🤯 **Plus a famous gotcha:** `count()` says **7**, but there are only **3** rows!
# MAGIC Spark doesn't need to parse any columns to count lines, so it never finds out which rows are malformed.

# COMMAND ----------

df_drop = (
    spark.read.option("header", True)
    .option("mode", "DROPMALFORMED")
    .schema("order_id INT, customer_id STRING, amount DECIMAL(10,2), status STRING")
    .csv(p("09_malformed_rows.csv"))
)
display(df_drop)
print("df_drop.count()      =", df_drop.count())
print("len(df_drop.collect()) =", len(df_drop.collect()))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mode 3: `FAILFAST`
# MAGIC The job **fails** at the first bad row. Right for critical feeds (finance, payroll) where bad data must stop the pipeline.

# COMMAND ----------

try:
    spark.read.option("header", True).option("mode", "FAILFAST").schema(malformed_schema).csv(p("09_malformed_rows.csv")).collect()
except Exception as e:
    print("FAILED FAST:", str(e)[:400])

# COMMAND ----------

# MAGIC %md
# MAGIC ### ⚠️ Unclosed quote + `multiLine` = rows swallowed
# MAGIC In `multiLine` mode the parser believes the quote on 9005 is still open, so it **eats every line until the end
# MAGIC of the file**. 9006 and 9007 disappear into 9005's `customer_id`.
# MAGIC Only use `multiLine` when the file really has newlines inside values (section 3).

# COMMAND ----------

display(
    spark.read.option("header", True).option("multiLine", True)
    .schema(malformed_schema)
    .csv(p("09_malformed_rows.csv"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ Production pattern: land in bronze, split into **good** and **quarantine**
# MAGIC ⚠️ Spark won't run a query on a *raw file* that references **only** `_corrupt_record` (e.g. counting bad rows).
# MAGIC Persist to a Delta table first, then query.

# COMMAND ----------

df_permissive.withColumn("_source_file", F.col("_metadata.file_name")) \
    .write.mode("overwrite").saveAsTable("orders_bronze_csv")

good = spark.table("orders_bronze_csv").filter("_corrupt_record IS NULL").drop("_corrupt_record")
bad = spark.table("orders_bronze_csv").filter("_corrupt_record IS NOT NULL").select(
    "_corrupt_record", "_source_file", F.current_timestamp().alias("quarantined_at")
)

good.write.mode("overwrite").saveAsTable("orders_silver_csv")
bad.write.mode("overwrite").saveAsTable("orders_quarantine_csv")

print("good rows:", spark.table("orders_silver_csv").count())
print("bad rows :", spark.table("orders_quarantine_csv").count())
display(spark.table("orders_quarantine_csv"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Databricks-only: `badRecordsPath`
# MAGIC Databricks writes bad records **with the reason** to a separate location automatically.
# MAGIC When `badRecordsPath` is set, the `mode` option is ignored.

# COMMAND ----------

bad_records_path = f"{base_path}/bad_records"

try:
    dbutils.fs.rm(bad_records_path, True)
    df_badpath = (
        spark.read.option("header", True)
        .option("badRecordsPath", bad_records_path)
        .schema("order_id INT, customer_id STRING, amount DECIMAL(10,2), status STRING")
        .csv(p("09_malformed_rows.csv"))
    )
    display(df_badpath)                                                # only good rows
    display(spark.read.json(f"{bad_records_path}/*/bad_records/"))     # path, record, reason
except Exception as e:
    print("badRecordsPath not supported on this compute:", str(e)[:300])

# COMMAND ----------

# MAGIC %md
# MAGIC Other safety limits worth knowing:
# MAGIC * `maxColumns` (default 20480): protects against a file with no line breaks
# MAGIC * `maxCharsPerColumn` (default unlimited): protects against one runaway quoted value
# MAGIC * `unescapedQuoteHandling`: what to do with a stray `"` (`STOP_AT_DELIMITER` default, `RAISE_ERROR`, `SKIP_VALUE`...)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 10. Encodings: when `é` becomes `�` and `ā` becomes `?`
# MAGIC
# MAGIC Spark reads CSV as **UTF-8** by default. Old Windows systems and Excel often write something else.
# MAGIC
# MAGIC ### 10a. Legacy Windows ERP export in **Windows-1252**

# COMMAND ----------

show_bytes("10a_windows1252.csv")     # é = \xe9, ë = \xeb, – = \x96 (single bytes, NOT valid UTF-8)

# COMMAND ----------

# ❌ Read as UTF-8: every accented character becomes the replacement character
display(spark.read.option("header", True).csv(p("10a_windows1252.csv")))

# COMMAND ----------

# ✅ Tell Spark the real encoding
df_1252 = spark.read.option("header", True).option("encoding", "windows-1252").csv(p("10a_windows1252.csv"))
display(df_1252)

# COMMAND ----------

# MAGIC %md
# MAGIC 💀 **But look at row 10004:** `Whang?rei` and `M?ori Kete Basket`.
# MAGIC
# MAGIC Windows-1252 has `é`, `ë`, `ü`, but **no Māori macrons** (`ā ē ī ō ū`). The source system replaced them
# MAGIC with `?` **when it exported the file**. The information is gone, and **no reader option can bring it back**.
# MAGIC
# MAGIC For NZ data this matters: place names and personal names with macrons are official spellings.
# MAGIC ✅ The real fix is upstream: **require UTF-8 exports** from source systems. Meanwhile, detect and report it:

# COMMAND ----------

display(
    df_1252.select(
        "order_id", "customer_name", "city", "product",
        (F.concat_ws("|", "customer_name", "city", "product").contains("?")).alias("possible_lost_macrons"),
    )
)

# Data-quality check on the UTF-8 mistake: count replacement characters (U+FFFD)
df_wrong_enc = spark.read.option("header", True).csv(p("10a_windows1252.csv"))
print("rows with U+FFFD when read as UTF-8:",
      df_wrong_enc.filter(F.concat_ws("|", *df_wrong_enc.columns).contains("�")).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10b. UTF-8 with a BOM (Excel "CSV UTF-8")
# MAGIC Excel puts 3 invisible bytes `\xef\xbb\xbf` (the BOM) at the start of the file.
# MAGIC Databricks strips it for you ✅. But other tools (pandas without `encoding='utf-8-sig'`, older Spark) turn the
# MAGIC first column into `﻿order_id`, and then `col("order_id")` "does not exist". If you ever see that, this is why.

# COMMAND ----------

show_bytes("10b_utf8_bom.csv", 40)
df_bom = spark.read.option("header", True).csv(p("10b_utf8_bom.csv"))
print([repr(c) for c in df_bom.columns])
display(df_bom)     # macrons survive in UTF-8 ✅

# COMMAND ----------

# Defensive one-liner if you ever get a BOM in a column name
df_bom_safe = df_bom.toDF(*[c.replace("﻿", "") for c in df_bom.columns])

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10c. UTF-16 (Excel "Save as Unicode Text")
# MAGIC 2 bytes per character, tab separated. You need **both** `encoding` **and** `multiLine`. With line-by-line
# MAGIC reading Spark splits the file on the `\n` byte, which cuts UTF-16 characters in half.

# COMMAND ----------

show_bytes("10c_utf16_excel_unicode.txt", 60)

# COMMAND ----------

print("❌ default:")
display(spark.read.option("header", True).option("sep", "\t").csv(p("10c_utf16_excel_unicode.txt")))

print("❌ encoding only:")
display(spark.read.option("header", True).option("sep", "\t").option("encoding", "UTF-16").csv(p("10c_utf16_excel_unicode.txt")))

# COMMAND ----------

print("✅ encoding + multiLine:")
display(
    spark.read.option("header", True).option("sep", "\t")
    .option("encoding", "UTF-16")
    .option("multiLine", True)
    .csv(p("10c_utf16_excel_unicode.txt"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 11. Bonus: fixed-width files (not CSV at all)
# MAGIC
# MAGIC Banks, insurers and old ERPs still send **fixed-width** files: no separator, every field lives at a fixed
# MAGIC character position. They usually come with a **header (`H`)** and a **trailer (`T`)** record. The trailer
# MAGIC carries a record count you should check.
# MAGIC
# MAGIC | Field | Start | Length |
# MAGIC |---|---|---|
# MAGIC | record type | 1 | 1 |
# MAGIC | order_id | 2 | 6 |
# MAGIC | customer_id | 8 | 6 |
# MAGIC | order_date (yyyyMMdd) | 14 | 8 |
# MAGIC | product | 22 | 20 |
# MAGIC | quantity | 42 | 5 |
# MAGIC | price in **cents** | 47 | 9 |
# MAGIC | status code | 56 | 1 |

# COMMAND ----------

show_raw("11_fixed_width.txt")

# COMMAND ----------

layout = {                          # name: (start, length)  (1-based, like the spec document)
    "order_id":    (2, 6),
    "customer_id": (8, 6),
    "order_date":  (14, 8),
    "product":     (22, 20),
    "quantity":    (42, 5),
    "price_cents": (47, 9),
    "status_code": (56, 1),
}

df_fw_text = spark.read.text(p("11_fixed_width.txt"))
detail = df_fw_text.filter(F.col("value").startswith("D"))

df_fixed = detail.select(
    *[F.trim(F.substring("value", start, length)).alias(name) for name, (start, length) in layout.items()]
).selectExpr(
    "CAST(order_id AS INT)                                AS order_id",
    "customer_id",
    "to_date(order_date, 'yyyyMMdd')                      AS order_date",
    "product",
    "CAST(quantity AS INT)                                AS quantity",
    "CAST(price_cents AS DECIMAL(12,0)) / 100             AS price_nzd",
    """CASE status_code WHEN 'D' THEN 'DELIVERED' WHEN 'S' THEN 'SHIPPED'
                        WHEN 'C' THEN 'CANCELLED' WHEN 'P' THEN 'PENDING' END AS status""",
)
display(df_fixed)

# COMMAND ----------

# Control check: the trailer says how many detail records there should be
expected = int(df_fw_text.filter(F.col("value").startswith("T")).first()["value"][1:])
actual = df_fixed.count()
print(f"trailer says {expected}, we parsed {actual} -> {'✅ complete' if expected == actual else '❌ INCOMPLETE FILE'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 12. The production way: `read_files` and Auto Loader
# MAGIC
# MAGIC New files keep arriving and the columns keep changing. **Auto Loader** (`cloudFiles`) handles this:
# MAGIC * matches CSV columns **by header name** (unlike the plain folder read in section 8!)
# MAGIC * `cloudFiles.schemaLocation`: remembers the schema and tracks how it evolves
# MAGIC * `cloudFiles.inferColumnTypes` + `cloudFiles.schemaHints`: real types, and force the ones you care about
# MAGIC * `cloudFiles.schemaEvolutionMode = addNewColumns` (default): on a **new column** the stream **stops once on
# MAGIC   purpose** with `UNKNOWN_FIELD_EXCEPTION`, updates the schema, and picks it up on restart. (In a Databricks Job,
# MAGIC   just set retries on the task.)
# MAGIC * `_rescued_data`: values that don't fit the schema (e.g. wrong types) are kept here instead of being lost
# MAGIC
# MAGIC Demo: the three monthly files arrive **one at a time**.

# COMMAND ----------

landing_path = f"{base_path}/landing/orders"
schema_loc = f"{base_path}/_autoloader/schema"
checkpoint = f"{base_path}/_autoloader/checkpoint"
bronze_table = "orders_bronze_autoloader_csv"

# reset the demo
for path in [landing_path, schema_loc, checkpoint]:
    dbutils.fs.rm(path, True)
spark.sql(f"DROP TABLE IF EXISTS {bronze_table}")

def run_autoloader(max_restarts=3):
    """Process all new files. Restart automatically when the schema evolves, like a Job with retries would."""
    for _ in range(max_restarts):
        try:
            (
                spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "csv")
                .option("header", True)
                .option("cloudFiles.schemaLocation", schema_loc)
                .option("cloudFiles.inferColumnTypes", True)
                .option("cloudFiles.schemaHints", "order_id INT, amount DECIMAL(10,2), order_date DATE")
                .load(landing_path)
                .withColumn("_source_file", F.col("_metadata.file_name"))
                .withColumn("_ingested_at", F.current_timestamp())
                .writeStream
                .option("checkpointLocation", checkpoint)
                .option("mergeSchema", True)            # let the Delta table accept the new columns
                .trigger(availableNow=True)
                .toTable(bronze_table)
                .awaitTermination()
            )
            print("✅ batch processed")
            return
        except Exception as e:
            if "UNKNOWN_FIELD" in str(e):
                print("🔄 new columns found, schema updated, restarting the stream...")
            else:
                raise

# COMMAND ----------

# January arrives: this file defines the starting schema
dbutils.fs.cp(p("08_monthly_drift/orders_2026_01.csv"), f"{landing_path}/orders_2026_01.csv")
run_autoloader()
display(spark.table(bronze_table))

# COMMAND ----------

# February arrives with a new `channel` column IN THE MIDDLE: matched by name, nothing misaligned
dbutils.fs.cp(p("08_monthly_drift/orders_2026_02.csv"), f"{landing_path}/orders_2026_02.csv")
run_autoloader()
display(spark.table(bronze_table).orderBy("order_id"))

# COMMAND ----------

# March: columns reordered, `amount` renamed to `total_amount_nzd`, `region` added
dbutils.fs.cp(p("08_monthly_drift/orders_2026_03.csv"), f"{landing_path}/orders_2026_03.csv")
run_autoloader()
display(spark.table(bronze_table).orderBy("order_id"))

# COMMAND ----------

# Silver view over bronze: one clean amount column, whatever the source called it
display(spark.sql(f"""
    SELECT order_id,
           customer_id,
           order_date,
           CAST(coalesce(amount, total_amount_nzd) AS DECIMAL(10,2)) AS amount_nzd,
           status,
           coalesce(channel, 'unknown')                             AS channel,
           region,
           _rescued_data,
           _source_file
    FROM {bronze_table}
    ORDER BY order_id
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 13. Cheat sheet 📝
# MAGIC
# MAGIC | Problem | Option / function |
# MAGIC |---|---|
# MAGIC | Header row | `header = true` (default is **false**!) |
# MAGIC | Slow / wrong inference | `.schema(ddl)`. Avoid `inferSchema` in production, **never** for IDs/postcodes/phones |
# MAGIC | Other separators | `sep` = `;` `\|` `\t` or multi-char `\|\|` |
# MAGIC | Decimal comma `1.249,00` | `locale = 'de-DE'` + `DECIMAL` schema, or `regexp_replace` |
# MAGIC | No header | `.schema(...)` (by position) or `.toDF(...)` |
# MAGIC | Commas / quotes in values | `quote`, `escape` (`'"'` for Excel, `\` default for MySQL dumps) |
# MAGIC | Newlines in values | `multiLine = true` (not splittable) |
# MAGIC | Title / footer junk | `skipRows`, `comment`, filter on `try_cast(key)`, or `text` + `from_csv` |
# MAGIC | Many null tokens | `nullValue` (one value), better: `normalise_nulls()` in silver |
# MAGIC | Padded spaces | `ignoreLeadingWhiteSpace`, `ignoreTrailingWhiteSpace`, `trim` |
# MAGIC | NaN poisoning sums | `nanValue`, `isnan`, or `DECIMAL` + `try_cast` |
# MAGIC | Many date formats | `coalesce(try_to_timestamp(x, fmt1), ...)`; dd/MM vs MM/dd is **per source** |
# MAGIC | Timezones | store UTC, `from_utc_timestamp(ts, 'Pacific/Auckland')` |
# MAGIC | Money strings `NZ$1,249.50`, `(15.00)` | `regexp_replace` + `try_cast(... AS DECIMAL)` |
# MAGIC | Bad column names | `clean_column_names()` + `toDF`, or Delta column mapping; backticks for dots |
# MAGIC | Columns differ between files | ❌ folder read is **by position**! `enforceSchema=false`, `unionByName(allowMissingColumns)`, `read_files`, Auto Loader |
# MAGIC | Malformed rows | `mode`, `columnNameOfCorruptRecord` (**must be in schema**), `badRecordsPath`, quarantine |
# MAGIC | `count()` lies | with `DROPMALFORMED`, `count()` doesn't parse rows. Persist first |
# MAGIC | Encoding | `encoding = 'windows-1252'`; UTF-16 needs `encoding` + `multiLine`; macrons lost at source can't be recovered |
# MAGIC | Fixed width | `spark.read.text` + `substring`, check trailer counts |
# MAGIC | Continuous ingestion + drift | `read_files`, Auto Loader (`schemaLocation`, `schemaHints`, `addNewColumns`, `_rescued_data`) |
# MAGIC | Lineage | `_metadata.file_name`, `_metadata.file_path` |
# MAGIC
# MAGIC **Golden rules**
# MAGIC 1. **Look at the raw file first** (`show_raw`). Separators, quotes, escapes and encodings are guesses until you do.
# MAGIC 2. Land everything as **STRING** in bronze, plus `_source_file` and `_corrupt_record` / `_rescued_data`.
# MAGIC 3. Type and clean in **silver** with `try_*` functions. Keep raw columns while validating.
# MAGIC 4. Identifiers (IDs, postcodes, phone numbers) are **strings**, never numbers.
# MAGIC 5. **Quarantine** bad rows, never silently drop them. Reconcile against control totals and trailer counts.
# MAGIC 6. Match columns **by name**, never by position, when files can change.
# MAGIC 7. Push back on the source: **UTF-8, ISO dates, one header row, RFC 4180 quoting**.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleanup (optional)

# COMMAND ----------

# for t in ["orders_clean_headers", "orders_bronze_csv", "orders_silver_csv", "orders_quarantine_csv",
#           "orders_bronze_autoloader_csv"]:
#     spark.sql(f"DROP TABLE IF EXISTS {t}")
# dbutils.fs.rm(base_path, True)
