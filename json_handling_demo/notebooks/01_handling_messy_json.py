# Databricks notebook source
# MAGIC %md
# MAGIC # Handling Real-World Messy JSON in Databricks
# MAGIC
# MAGIC In tutorials JSON is always clean. In real projects it never is. 😅
# MAGIC
# MAGIC This notebook walks through the **same e-commerce `orders` data** getting messier step by step,
# MAGIC and shows which **Databricks / Spark JSON options and functions** fix each problem.
# MAGIC
# MAGIC | # | File | Problem |
# MAGIC |---|------|---------|
# MAGIC | 1 | `01_clean.json` | Nothing. Flat JSON Lines (baseline) |
# MAGIC | 2 | `02_nested.json` | Nested structs + arrays |
# MAGIC | 3 | `03_multiline.json` | Pretty-printed JSON array spanning many lines |
# MAGIC | 4 | `04_schema_drift.json` | Fields added / renamed / different casing |
# MAGIC | 5 | `05_type_mismatch.json` | Same field arrives as int, string, null, "N/A", NaN... |
# MAGIC | 6 | `06_corrupt_records.json` | Broken lines, comments, unquoted keys, bad escapes |
# MAGIC | 7 | `07_json_string_column.csv` | JSON payload stored as a **string column** (API logs) |
# MAGIC | 8 | Auto Loader | Production pattern: schema evolution + `_rescued_data` |
# MAGIC
# MAGIC **Requirements:** Unity Catalog workspace (Databricks Free Edition works), serverless or DBR 15.3+ (for `VARIANT`).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Setup: catalog, schema, volume
# MAGIC
# MAGIC Change the widgets at the top if you want a different location.
# MAGIC On Databricks Free Edition the default catalog is `workspace`.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "json_demo", "Schema")
dbutils.widgets.text("volume", "raw", "Volume")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")
spark.sql(f"USE {catalog}.{schema}")

base_path = f"/Volumes/{catalog}/{schema}/{volume}/json_demo"
data_path = f"{base_path}/sample_data"
print(f"Sample data path: {data_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get the sample files into the volume
# MAGIC
# MAGIC **Option A (automatic):** if you cloned this repo as a **Databricks Git folder**, the cell below copies
# MAGIC `../sample_data/*` from the repo into the volume.
# MAGIC
# MAGIC **Option B (manual):** Catalog Explorer → your volume → **Upload to this volume** → upload the files into
# MAGIC `json_demo/sample_data/`. Or use the CLI:
# MAGIC `databricks fs cp -r ./sample_data dbfs:/Volumes/<catalog>/<schema>/<volume>/json_demo/sample_data`

# COMMAND ----------

import os
import shutil

repo_sample_dir = os.path.abspath(os.path.join(os.getcwd(), "..", "sample_data"))
os.makedirs(data_path, exist_ok=True)

if os.path.isdir(repo_sample_dir):
    for f in sorted(os.listdir(repo_sample_dir)):
        shutil.copy(os.path.join(repo_sample_dir, f), os.path.join(data_path, f))
        print(f"copied {f}")
else:
    print(f"{repo_sample_dir} not found, so upload the files manually (Option B).")

display(dbutils.fs.ls(data_path))

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, ArrayType

def p(file_name):
    """Full path of a sample file in the volume."""
    return f"{data_path}/{file_name}"

def show_raw(file_name, n=20):
    """Print the raw text of a file so we can SEE the mess before parsing it."""
    for row in spark.read.text(p(file_name)).limit(n).collect():
        print(row.value)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. Clean JSON (the baseline)
# MAGIC
# MAGIC Spark's default JSON format is **JSON Lines**: one complete JSON object per line.

# COMMAND ----------

show_raw("01_clean.json")

# COMMAND ----------

df_clean = spark.read.json(p("01_clean.json"))
df_clean.printSchema()
display(df_clean)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema inference vs. explicit schema
# MAGIC
# MAGIC Schema inference looks easy, but notice:
# MAGIC * `order_date` was inferred as **string**, not date
# MAGIC * `price` became **double** (floating point, so it rounds badly for money)
# MAGIC * Inference **reads the data twice** (once to infer, once to load), which is slow on big data
# MAGIC * `samplingRatio` makes it faster, but fields that only exist in unsampled rows are **silently missed**
# MAGIC
# MAGIC ✅ In production, **declare the schema**. A DDL string is the easiest way.

# COMMAND ----------

orders_schema = """
    order_id    INT,
    customer_id STRING,
    order_date  DATE,
    product     STRING,
    quantity    INT,
    price       DECIMAL(10,2),
    status      STRING
"""

df_clean_typed = (
    spark.read
    .schema(orders_schema)
    .option("dateFormat", "yyyy-MM-dd")
    .json(p("01_clean.json"))
)
df_clean_typed.printSchema()
display(df_clean_typed)

# COMMAND ----------

# `prefersDecimal` = infer floating numbers as DECIMAL instead of DOUBLE (when they fit)
spark.read.option("prefersDecimal", True).json(p("01_clean.json")).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. Nested JSON: structs and arrays
# MAGIC
# MAGIC Real API responses are nested: a `customer` object containing an `address` object, an `items` array of
# MAGIC objects, and so on.

# COMMAND ----------

show_raw("02_nested.json")

# COMMAND ----------

df_nested = spark.read.json(p("02_nested.json"))
df_nested.printSchema()
display(df_nested)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Structs → use dot notation, or `struct.*` to expand all fields

# COMMAND ----------

display(
    df_nested.select(
        "order_id",
        F.col("customer.name").alias("customer_name"),
        F.col("customer.address.city").alias("city"),
        F.col("payment.card.brand").alias("card_brand"),   # null-safe: NULL card -> NULL brand
        "customer.address.*",                               # expand every field of the struct
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Arrays → `explode` vs `explode_outer`
# MAGIC
# MAGIC ⚠️ Order **2004** has an **empty `items` array**.
# MAGIC * `explode` **drops** that order completely, so you silently lose data
# MAGIC * `explode_outer` keeps it with NULL item columns

# COMMAND ----------

exploded = df_nested.select("order_id", F.explode("items").alias("item"))
exploded_outer = df_nested.select("order_id", F.explode_outer("items").alias("item"))

print("explode       -> orders:", exploded.select("order_id").distinct().count())
print("explode_outer -> orders:", exploded_outer.select("order_id").distinct().count())

# COMMAND ----------

# posexplode_outer also gives you the line number of each item in the order
df_items = (
    df_nested
    .select("order_id", F.posexplode_outer("items").alias("line_no", "item"))
    .select(
        "order_id",
        (F.col("line_no") + 1).alias("line_no"),
        "item.sku",
        "item.name",
        "item.qty",
        "item.unit_price",
        (F.col("item.qty") * F.col("item.unit_price")).alias("line_total"),
    )
)
display(df_items)

# COMMAND ----------

# Same thing in SQL with inline_outer (explodes an array of structs straight into columns)
display(df_nested.selectExpr("order_id", "inline_outer(items)"))

# COMMAND ----------

# Arrays of primitives: often better kept as arrays and queried with array functions
display(
    df_nested.select(
        "order_id",
        "tags",
        F.size("tags").alias("tag_count"),
        F.array_contains("tags", "promo").alias("is_promo"),
        F.array_join("tags", ",").alias("tags_csv"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reusable helper: flatten ALL nested structs automatically
# MAGIC
# MAGIC Handy when an API returns 5+ levels of nesting. Arrays are left as-is (explode them deliberately,
# MAGIC because exploding changes the grain of your table).

# COMMAND ----------

def flatten_df(df, sep="_"):
    """Recursively flatten struct columns: customer.address.city -> customer_address_city."""
    def _fields(schema, prefix=None):
        cols = []
        for field in schema.fields:
            name = f"{prefix}.{field.name}" if prefix else field.name
            if isinstance(field.dataType, StructType):
                cols += _fields(field.dataType, name)
            else:
                cols.append(F.col(name).alias(name.replace(".", sep)))
        return cols
    return df.select(_fields(df.schema))

df_flat = flatten_df(df_nested)
df_flat.printSchema()
display(df_flat)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. Multi-line JSON
# MAGIC
# MAGIC Many systems export **one big pretty-printed JSON array** instead of JSON Lines.

# COMMAND ----------

show_raw("03_multiline.json", n=15)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ❌ Reading it with the default settings
# MAGIC Spark tries to parse **every line** as its own JSON document. `[`, `{`, `"order_id": 3001,` are not valid
# MAGIC JSON on their own, so every line is corrupt and the inferred schema is only `_corrupt_record`.

# COMMAND ----------

df_wrong = spark.read.json(p("03_multiline.json"))
df_wrong.printSchema()

try:
    display(df_wrong)
except Exception as e:
    # Spark refuses queries that only reference the internal _corrupt_record column of a raw file
    print("ERROR:", str(e)[:400])

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ Fix: `multiLine = true`
# MAGIC Spark now reads each **file** as a whole JSON document, and a top-level array becomes one row per element.
# MAGIC
# MAGIC ⚠️ Trade-off: a multiLine file **cannot be split**, so one huge file = one task. Ask the source
# MAGIC for JSON Lines when the files are large.

# COMMAND ----------

df_multi = spark.read.option("multiLine", True).json(p("03_multiline.json"))
df_multi.printSchema()
display(df_multi.select("order_id", "customer.name", F.size("items").alias("item_count"), "notes"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. Schema drift: fields added, renamed, and re-cased
# MAGIC
# MAGIC The source team "just made a small change"... several times:
# MAGIC * `currency`, `channel`, `discount` were **added** later
# MAGIC * `customer_id` → `customerId` → `CUSTOMER_ID` (three spellings!)
# MAGIC * `amount` → `total_amount`, `order_id` → `orderId`, `status` → `Status`
# MAGIC * `legacy_flag` is always null

# COMMAND ----------

show_raw("04_schema_drift.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ### ❌ Default read
# MAGIC JSON inference **unions all fields** it sees (missing fields become NULL, which is good).
# MAGIC But Spark is **case-insensitive** by default, so `customer_id` and `CUSTOMER_ID` (and `status` /
# MAGIC `Status`) clash, and you will likely get a `COLUMN_ALREADY_EXISTS` / duplicate-column error.

# COMMAND ----------

try:
    df_drift = spark.read.json(p("04_schema_drift.json"))
    df_drift.printSchema()
    display(df_drift)
except Exception as e:
    print("ERROR:", str(e)[:400])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option A: make Spark case-sensitive, then `coalesce` the variants
# MAGIC Works on classic clusters. On serverless, this config may not be settable, so use Option B.

# COMMAND ----------

try:
    spark.conf.set("spark.sql.caseSensitive", "true")
    df_drift_cs = (
        spark.read
        .option("dropFieldIfAllNull", True)       # drops `legacy_flag` (always null)
        .json(p("04_schema_drift.json"))
    )
    df_drift_cs.printSchema()
    display(
        df_drift_cs.select(
            F.coalesce("order_id", "orderId").alias("order_id"),
            F.coalesce("customer_id", "customerId", "CUSTOMER_ID").alias("customer_id"),
            F.coalesce("amount", "total_amount").alias("amount"),
            F.upper(F.coalesce("status", "Status")).alias("status"),
        )
    )
except Exception as e:
    print("Option A not available here:", str(e)[:300])
finally:
    try:
        spark.conf.set("spark.sql.caseSensitive", "false")
    except Exception:
        pass

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option B (recommended): the `VARIANT` type
# MAGIC Parse each line into a **`VARIANT`**, a semi-structured type that stores **any** JSON shape with no schema
# MAGIC up front. Field lookups with `:` are **case-sensitive**, so every spelling is reachable.
# MAGIC Then pick and cast what you need.

# COMMAND ----------

df_drift_v = spark.read.text(p("04_schema_drift.json")).selectExpr("parse_json(value) AS v")

df_drift_clean = df_drift_v.selectExpr(
    "coalesce(v:order_id, v:orderId)::INT                            AS order_id",
    "coalesce(v:customer_id, v:customerId, v:CUSTOMER_ID)::STRING    AS customer_id",
    "coalesce(v:amount, v:total_amount)::DECIMAL(10,2)               AS amount",
    "upper(coalesce(v:status, v:Status)::STRING)                     AS status",
    "upper(coalesce(v:currency::STRING, 'UNKNOWN'))                  AS currency",
    "coalesce(v:channel::STRING, 'unknown')                          AS channel",
    "v:discount.code::STRING                                         AS discount_code",
    "v:discount.pct::INT                                             AS discount_pct",
    "v                                                               AS raw_json",  # keep the original for audit
)
display(df_drift_clean)

# COMMAND ----------

# Bonus: discover every field (and its type) that appeared across all records
display(df_drift_v.selectExpr("schema_of_variant_agg(v) AS all_fields_seen"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5. Type mismatches: same field, many types
# MAGIC
# MAGIC Look at the mess:
# MAGIC * `order_id`: `5001` vs `"5002"`
# MAGIC * `quantity`: `2`, `"1"`, `1.0`, `null`, `"three"`
# MAGIC * `price`: `25.99`, `"49.50"`, `119`, `"N/A"`, `NaN`
# MAGIC * `is_gift`: `true`, `"false"`, `"Y"`, `0`
# MAGIC * `order_date`: 5 different date formats + empty string
# MAGIC * `created_at`: ISO, space-separated, **epoch seconds**, with timezone offset, empty
# MAGIC * `zip`: string vs number, and `00501` has a **leading zero** (invalid JSON number!)

# COMMAND ----------

show_raw("05_type_mismatch.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ### ❌ Default read
# MAGIC * Conflicting types get widened to **string** (fine), but you don't control it
# MAGIC * The row with `"zip": 00501` is **corrupt** because leading zeros are not allowed by default

# COMMAND ----------

df_types_default = spark.read.json(p("05_type_mismatch.json"))
df_types_default.printSchema()
display(df_types_default)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ Fix, step 1: land everything as STRING
# MAGIC * `primitivesAsString` = every number/boolean is read as a string, so **nothing is lost or mis-inferred**
# MAGIC * `allowNumericLeadingZeros` = accept `00501`
# MAGIC * `allowNonNumericNumbers` = accept `NaN`, `Infinity` (on by default in recent Spark versions)
# MAGIC
# MAGIC This is the classic **bronze layer** approach: land raw as strings, type it in silver.

# COMMAND ----------

df_types_raw = (
    spark.read
    .option("primitivesAsString", True)
    .option("allowNumericLeadingZeros", True)
    .option("allowNonNumericNumbers", True)
    .json(p("05_type_mismatch.json"))
)
df_types_raw.printSchema()
display(df_types_raw)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ Fix, step 2: cast defensively with `try_cast` / `try_to_timestamp`
# MAGIC `try_*` functions return **NULL instead of failing the whole job** (important on serverless, where ANSI
# MAGIC mode is on and a plain `cast('N/A' AS INT)` throws an error).

# COMMAND ----------

df_types_clean = df_types_raw.selectExpr(
    "try_cast(order_id AS INT)                                   AS order_id",

    # "1.0" -> 1, "three" -> NULL
    "try_cast(try_cast(quantity AS DOUBLE) AS INT)               AS quantity",

    # "N/A" / "NaN" -> NULL, money as DECIMAL
    "try_cast(price AS DECIMAL(18,2))                            AS price",

    # normalise every way people write booleans
    """CASE WHEN lower(is_gift) IN ('true','y','yes','1')  THEN true
            WHEN lower(is_gift) IN ('false','n','no','0') THEN false
       END                                                       AS is_gift""",

    # try several date formats, first one that parses wins
    """to_date(coalesce(
            try_to_timestamp(order_date, 'yyyy-MM-dd'),
            try_to_timestamp(order_date, 'MM/dd/yyyy'),
            try_to_timestamp(order_date, 'yyyy/MM/dd'),
            try_to_timestamp(order_date, 'dd-MMM-yyyy')
       ))                                                        AS order_date""",

    # epoch seconds vs ISO / space-separated timestamps
    """CASE WHEN created_at RLIKE '^[0-9]{10}$'
            THEN timestamp_seconds(try_cast(created_at AS BIGINT))
            ELSE try_cast(nullif(created_at, '') AS TIMESTAMP)
       END                                                       AS created_at""",

    # zip codes are identifiers, NOT numbers: keep them as strings
    "zip",

    # keep the raw values next to the clean ones while you validate
    "order_date AS order_date_raw",
    "price      AS price_raw",
)
df_types_clean.printSchema()
display(df_types_clean)

# COMMAND ----------

# MAGIC %md
# MAGIC 💡 **Tip:** count how many values failed to cast. That's your data-quality metric to send back to the
# MAGIC source team.

# COMMAND ----------

display(
    df_types_clean.select(
        F.count_if(F.col("price").isNull() & F.col("price_raw").isNotNull()).alias("bad_prices"),
        F.count_if(F.col("order_date").isNull() & (F.col("order_date_raw") != "")).alias("bad_dates"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 6. Corrupt records & parse modes
# MAGIC
# MAGIC The worst file. Each line has a different problem:
# MAGIC
# MAGIC | order | problem |
# MAGIC |---|---|
# MAGIC | 6001 | ✅ fine |
# MAGIC | 6002 | missing closing `}` (truncated) |
# MAGIC | 6003 | unquoted field names `{order_id: ...}` |
# MAGIC | 6004 | single quotes `'order_id'` |
# MAGIC | 6005 | trailing comma `,}` |
# MAGIC | 6006 | a `/* comment */` inside JSON |
# MAGIC | — | a random log line, not JSON at all |
# MAGIC | 6007 | `Infinity` as a number |
# MAGIC | 6008 | Windows path with invalid escape `C:\data` |
# MAGIC | — | empty line |
# MAGIC | 6009 | ✅ fine |

# COMMAND ----------

show_raw("06_corrupt_records.json")

# COMMAND ----------

corrupt_schema = """
    order_id        INT,
    customer_id     STRING,
    amount          DOUBLE,
    status          STRING,
    note            STRING,
    _corrupt_record STRING
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mode 1: `PERMISSIVE` (default)
# MAGIC Bad rows are **kept**: all fields NULL, the raw line goes into `_corrupt_record`.
# MAGIC ⚠️ The `_corrupt_record` column only appears if it is in your schema (or inference found bad rows).

# COMMAND ----------

df_permissive = (
    spark.read
    .schema(corrupt_schema)
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")   # rename it if you like, e.g. "_bad_json"
    .json(p("06_corrupt_records.json"))
)
display(df_permissive)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mode 2: `DROPMALFORMED`
# MAGIC Bad rows are **silently dropped**. ⚠️ Dangerous: you lose data and nobody knows.

# COMMAND ----------

df_drop = spark.read.schema(corrupt_schema).option("mode", "DROPMALFORMED").json(p("06_corrupt_records.json"))
display(df_drop)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mode 3: `FAILFAST`
# MAGIC The job **fails** at the first bad row. Good for critical feeds where bad data must stop the pipeline.

# COMMAND ----------

try:
    spark.read.schema(corrupt_schema).option("mode", "FAILFAST").json(p("06_corrupt_records.json")).collect()
except Exception as e:
    print("FAILED FAST:", str(e)[:400])

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ Rescue what you can with the leniency options
# MAGIC Many "corrupt" lines are just **non-standard** JSON. Spark can accept them:
# MAGIC
# MAGIC | Option | Accepts | Default |
# MAGIC |---|---|---|
# MAGIC | `allowUnquotedFieldNames` | `{order_id: 1}` | false |
# MAGIC | `allowSingleQuotes` | `{'order_id': 1}` | **true** |
# MAGIC | `allowComments` | `/* ... */` and `// ...` | false |
# MAGIC | `allowBackslashEscapingAnyCharacter` | `"C:\data"` | false |
# MAGIC | `allowNonNumericNumbers` | `NaN`, `Infinity` | true |
# MAGIC | `allowUnquotedControlChars` | raw tabs/newlines inside strings | false |
# MAGIC
# MAGIC Truncated lines, trailing commas and plain text **cannot** be rescued. Those stay corrupt.

# COMMAND ----------

df_lenient = (
    spark.read
    .schema(corrupt_schema)
    .option("mode", "PERMISSIVE")
    .option("allowUnquotedFieldNames", True)
    .option("allowSingleQuotes", True)
    .option("allowComments", True)
    .option("allowBackslashEscapingAnyCharacter", True)
    .option("allowNonNumericNumbers", True)
    .option("allowUnquotedControlChars", True)
    .json(p("06_corrupt_records.json"))
)
display(df_lenient)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ Production pattern: land in bronze, then split into **good** and **quarantine**
# MAGIC
# MAGIC ⚠️ Spark won't let you run a query on a *raw file* that references **only** `_corrupt_record`
# MAGIC (e.g. `count()` of bad rows). Persist to a Delta table first (or `.cache()` on classic compute), then query.

# COMMAND ----------

df_lenient.write.mode("overwrite").saveAsTable("orders_bronze_raw")

good = spark.table("orders_bronze_raw").filter("_corrupt_record IS NULL").drop("_corrupt_record")
bad = spark.table("orders_bronze_raw").filter("_corrupt_record IS NOT NULL").select(
    "_corrupt_record", F.current_timestamp().alias("quarantined_at")
)

good.write.mode("overwrite").saveAsTable("orders_silver")
bad.write.mode("overwrite").saveAsTable("orders_quarantine")

print("good rows:", spark.table("orders_silver").count())
print("bad rows :", spark.table("orders_quarantine").count())
display(spark.table("orders_quarantine"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Databricks-only: `badRecordsPath`
# MAGIC Databricks can write bad records (with the **reason**) to a separate location automatically.
# MAGIC Note: when `badRecordsPath` is set, the `mode` option is ignored.

# COMMAND ----------

bad_records_path = f"{base_path}/bad_records"

try:
    dbutils.fs.rm(bad_records_path, True)
    df_badpath = (
        spark.read
        .schema("order_id INT, customer_id STRING, amount DOUBLE, status STRING, note STRING")
        .option("badRecordsPath", bad_records_path)
        .json(p("06_corrupt_records.json"))
    )
    display(df_badpath)                                              # only good rows
    display(spark.read.json(f"{bad_records_path}/*/bad_records/"))   # path, record, reason
except Exception as e:
    print("badRecordsPath not supported on this compute:", str(e)[:300])

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 7. JSON stored inside a string column
# MAGIC
# MAGIC Super common: API/event logs land in a table or CSV where one column (`payload`) holds a JSON string, and
# MAGIC **different event types have different payload shapes**. One payload (`evt-006`) is even truncated.

# COMMAND ----------

show_raw("07_json_string_column.csv")

# COMMAND ----------

df_events = (
    spark.read
    .option("header", True)
    .option("escape", '"')      # the CSV escapes quotes by doubling them: ""order_id""
    .csv(p("07_json_string_column.csv"))
)
display(df_events)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option 1: `get_json_object`, for quickly pulling a few fields (JSONPath)

# COMMAND ----------

display(
    df_events.select(
        "event_id",
        "source",
        F.get_json_object("payload", "$.order_id").alias("order_id"),
        F.get_json_object("payload", "$.customer.id").alias("customer_id"),
        F.get_json_object("payload", "$.items[0].sku").alias("first_sku"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option 2: Databricks `:` syntax on a STRING column (SQL)

# COMMAND ----------

df_events.createOrReplaceTempView("events")

display(spark.sql("""
    SELECT event_id,
           source,
           payload:order_id::INT          AS order_id,
           payload:customer.tier          AS tier,
           payload:items[0].unit_price    AS first_price,
           payload:carrier                AS carrier
    FROM events
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option 3: `from_json` with a schema, for full typed structs
# MAGIC Don't hand-write the schema. Let `schema_of_json` generate it from a sample, then fix it up.
# MAGIC Because payloads differ per `source`, **parse each event type with its own schema**.

# COMMAND ----------

sample_order = df_events.filter("source = 'order_api'").first()["payload"]
print(spark.range(1).select(F.schema_of_json(F.lit(sample_order))).first()[0])

# COMMAND ----------

order_payload_schema = """
    order_id INT,
    customer STRUCT<id: STRING, tier: STRING>,
    items    ARRAY<STRUCT<sku: STRING, qty: INT, unit_price: DECIMAL(10,2)>>,
    total    DECIMAL(10,2),
    coupon   STRING
"""

df_orders = (
    df_events
    .filter("source = 'order_api'")
    .withColumn("p", F.from_json("payload", order_payload_schema, {"mode": "PERMISSIVE"}))
    .select("event_id", "event_ts", "p.*", "payload")
)
display(df_orders)   # evt-006 (truncated JSON) -> NULL fields instead of a failed job

# COMMAND ----------

# to_json goes the other way: struct -> JSON string (e.g. to send to an API or Kafka)
display(df_orders.select("order_id", F.to_json(F.struct("customer", "total")).alias("json_out")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option 4 (modern): `VARIANT`
# MAGIC One column handles **every** payload shape, no schema needed, and it is faster than repeatedly parsing strings.
# MAGIC `try_parse_json` returns NULL for broken JSON instead of failing.

# COMMAND ----------

df_events_v = df_events.selectExpr("event_id", "source", "try_parse_json(payload) AS v")

display(df_events_v.selectExpr(
    "event_id",
    "source",
    "v:order_id::INT                                  AS order_id",
    "variant_get(v, '$.customer.id', 'string')        AS customer_id",
    "v:amount::DECIMAL(10,2)                          AS payment_amount",
    "v:eta::DATE                                      AS shipping_eta",
    "v IS NULL                                        AS is_broken_json",
))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 8. Bonus: the production way, with Auto Loader
# MAGIC
# MAGIC In real pipelines, new JSON files keep arriving and the schema keeps changing. **Auto Loader** handles this:
# MAGIC * `cloudFiles.schemaLocation`: stores the inferred schema and tracks its evolution
# MAGIC * `cloudFiles.inferColumnTypes`: infer real types (by default everything is a string)
# MAGIC * `cloudFiles.schemaHints`: force the types you care about
# MAGIC * `cloudFiles.schemaEvolutionMode = rescue`: **never fail**, and put unexpected columns / bad types into `_rescued_data`
# MAGIC
# MAGIC Demo: land the **clean** file first, then the **drifted** file, and watch what gets rescued.

# COMMAND ----------

landing_path = f"{base_path}/landing/orders"
schema_loc = f"{base_path}/_autoloader/schema"
checkpoint = f"{base_path}/_autoloader/checkpoint"
bronze_table = "orders_bronze_autoloader"

# reset the demo
for path in [landing_path, schema_loc, checkpoint]:
    dbutils.fs.rm(path, True)
spark.sql(f"DROP TABLE IF EXISTS {bronze_table}")

def run_autoloader():
    (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_loc)
        .option("cloudFiles.inferColumnTypes", True)
        .option("cloudFiles.schemaHints", "order_id BIGINT, price DECIMAL(10,2)")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .load(landing_path)
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .writeStream
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .toTable(bronze_table)
        .awaitTermination()
    )

# COMMAND ----------

# Batch 1: the clean file. This defines the schema.
dbutils.fs.cp(p("01_clean.json"), f"{landing_path}/01_clean.json")
run_autoloader()
display(spark.table(bronze_table))

# COMMAND ----------

# Batch 2: the drifted file arrives. New/renamed columns don't break the stream; they land in _rescued_data.
dbutils.fs.cp(p("04_schema_drift.json"), f"{landing_path}/04_schema_drift.json")
run_autoloader()
display(spark.table(bronze_table).orderBy("order_id"))

# COMMAND ----------

# _rescued_data is JSON, so you can still recover those fields downstream
display(
    spark.table(bronze_table)
    .filter("_rescued_data IS NOT NULL")
    .selectExpr(
        "order_id",
        "_rescued_data",
        "coalesce(customer_id, _rescued_data:customerId, _rescued_data:CUSTOMER_ID) AS customer_id_fixed",
        "coalesce(_rescued_data:amount, _rescued_data:total_amount)::DECIMAL(10,2)  AS amount",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 9. Cheat sheet 📝
# MAGIC
# MAGIC | Problem | Option / function |
# MAGIC |---|---|
# MAGIC | Slow / wrong inference | `.schema(ddl)`, `samplingRatio`, `prefersDecimal` |
# MAGIC | Nested structs | `col("a.b.c")`, `"struct.*"`, `flatten_df()` |
# MAGIC | Arrays | `explode_outer`, `posexplode_outer`, `inline_outer` (avoid plain `explode`!) |
# MAGIC | Pretty-printed / JSON array file | `multiLine = true` |
# MAGIC | Fields always null | `dropFieldIfAllNull = true` |
# MAGIC | Same name, different case | `spark.sql.caseSensitive`, or `VARIANT` + `coalesce` |
# MAGIC | Mixed types | `primitivesAsString = true` → `try_cast` |
# MAGIC | Many date formats | `coalesce(try_to_timestamp(col, fmt1), ...)`, `dateFormat`, `timestampFormat` |
# MAGIC | Leading zeros / NaN | `allowNumericLeadingZeros`, `allowNonNumericNumbers` |
# MAGIC | Non-standard JSON | `allowUnquotedFieldNames`, `allowSingleQuotes`, `allowComments`, `allowBackslashEscapingAnyCharacter`, `allowUnquotedControlChars` |
# MAGIC | Broken rows | `mode` = `PERMISSIVE` / `DROPMALFORMED` / `FAILFAST`, `columnNameOfCorruptRecord`, `badRecordsPath` |
# MAGIC | JSON in a string column | `get_json_object`, `col:path`, `from_json` + `schema_of_json`, `to_json` |
# MAGIC | Unknown / changing shape | `VARIANT`: `parse_json`, `try_parse_json`, `variant_get`, `schema_of_variant_agg` |
# MAGIC | Continuous ingestion + drift | Auto Loader: `schemaLocation`, `schemaHints`, `schemaEvolutionMode`, `_rescued_data` |
# MAGIC
# MAGIC **Golden rules**
# MAGIC 1. Land raw (strings / VARIANT) in **bronze**, so you never lose data at ingestion.
# MAGIC 2. Type and clean in **silver** with `try_*` functions.
# MAGIC 3. **Quarantine** bad records, never silently drop them.
# MAGIC 4. Declare schemas in production. Inference is for exploration.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleanup (optional)

# COMMAND ----------

# for t in ["orders_bronze_raw", "orders_silver", "orders_quarantine", "orders_bronze_autoloader"]:
#     spark.sql(f"DROP TABLE IF EXISTS {t}")
# dbutils.fs.rm(base_path, True)
