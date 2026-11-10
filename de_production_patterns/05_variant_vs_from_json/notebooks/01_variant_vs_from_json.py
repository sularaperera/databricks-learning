# Databricks notebook source
# MAGIC %md
# MAGIC # Semi-Structured Data at Scale: `VARIANT` vs `from_json`
# MAGIC
# MAGIC **Scenario (E-commerce / FinTech):** a payment gateway sends **webhooks** to our API, and we land every event
# MAGIC as JSON. One feed, many shapes:
# MAGIC
# MAGIC | What makes it hard | In the sample data |
# MAGIC |---|---|
# MAGIC | **Many event types**, each with a different `data.object` | `payment.succeeded`, `payment.failed`, `refund.created`, `dispute.opened`, `customer.updated`, `payout.paid` |
# MAGIC | **Polymorphic nested objects** | `payment_method` is a `card`, a `mobile_wallet` or a `bank_transfer` |
# MAGIC | **Free-form keys** | `metadata` holds whatever each merchant sends |
# MAGIC | **Breaking API change** | `api_version 2026-06-01`: `amount` changed from `5198` (minor units) to `{"value": 89.99, "currency": "EUR", "minor_units": 8999}` |
# MAGIC | **Type drift** | an old SDK sends `"amount": "1500"` (string) |
# MAGIC | **Explicit JSON nulls** | `dispute.opened` → `"evidence": {"receipt": null}` |
# MAGIC | **Broken payload** | `evt_013` is truncated |
# MAGIC
# MAGIC We'll solve it two ways and compare: **`from_json` with a schema** vs **`VARIANT`**, then build the
# MAGIC **hybrid** pattern most production teams end up with.
# MAGIC
# MAGIC **Requirements:** serverless or DBR 15.3+ (VARIANT). `variant_explode` and `try_variant_get` need DBR 15.3+ / 16.x.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Setup

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "variant_demo", "Schema")
dbutils.widgets.text("volume", "raw", "Volume")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")
spark.sql(f"USE {catalog}.{schema}")

base_path = f"/Volumes/{catalog}/{schema}/{volume}/variant_demo"
data_path = f"{base_path}/sample_data"
print(f"Sample data path: {data_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC Manual upload alternative:
# MAGIC `databricks fs cp -r ./de_production_patterns/05_variant_vs_from_json/sample_data dbfs:/Volumes/workspace/variant_demo/raw/variant_demo/sample_data`

# COMMAND ----------

import os
import shutil
import time

repo_sample_dir = os.path.abspath(os.path.join(os.getcwd(), "..", "sample_data"))
os.makedirs(data_path, exist_ok=True)

if os.path.isdir(repo_sample_dir):
    for f in sorted(os.listdir(repo_sample_dir)):
        shutil.copy(os.path.join(repo_sample_dir, f), os.path.join(data_path, f))
        print(f"copied {f}")
else:
    print(f"{repo_sample_dir} not found, so upload the files manually.")

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Bronze: land the raw text **and** a VARIANT
# MAGIC `try_parse_json` returns NULL for the broken line instead of failing, and keeping the `raw` string means the
# MAGIC broken payload is still there for debugging.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE webhooks_bronze AS
    SELECT
        value                          AS raw,
        try_parse_json(value)          AS v,
        _metadata.file_name            AS _source_file,
        current_timestamp()            AS _ingested_at
    FROM read_files('{data_path}/payment_webhooks.jsonl', format => 'text')
""")

display(spark.sql("SELECT v IS NOT NULL AS valid_json, count(*) AS events FROM webhooks_bronze GROUP BY 1"))
display(spark.table("webhooks_bronze"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. Approach A: `from_json` with an explicit schema
# MAGIC
# MAGIC The "classic" way: write (or generate) a `STRUCT` schema, parse once, get typed columns.
# MAGIC
# MAGIC ### Step 1: generate the schema from a sample with `schema_of_json`
# MAGIC ⚠️ The schema only describes **the sample you gave it**. Here we pick the first payment event.

# COMMAND ----------

sample = spark.table("webhooks_bronze").filter("raw LIKE '%evt_001%'").first()["raw"]
print(spark.range(1).select(F.schema_of_json(F.lit(sample))).first()[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: parse everything with that (cleaned up) schema

# COMMAND ----------

payment_schema = """
    id STRING,
    type STRING,
    created BIGINT,
    api_version STRING,
    data STRUCT<object: STRUCT<
        id: STRING,
        amount: BIGINT,
        currency: STRING,
        customer: STRING,
        payment_method: STRUCT<type: STRING, card: STRUCT<brand: STRING, last4: STRING>>,
        items: ARRAY<STRUCT<sku: STRING, qty: INT, unit_amount: BIGINT>>,
        metadata: MAP<STRING, STRING>
    >>
"""

parsed = spark.table("webhooks_bronze").select("raw", F.from_json("raw", payment_schema).alias("j"))

display(parsed.selectExpr(
    "j.id", "j.type", "j.api_version",
    "j.data.object.amount       AS amount",
    "j.data.object.currency     AS currency",
    "j.data.object.payment_method.type AS pm_type",
    "j.data.object.payment_method.card.brand AS card_brand",
    "j.data.object.metadata     AS metadata",
))

# COMMAND ----------

# MAGIC %md
# MAGIC ### What went wrong, silently
# MAGIC Look at the table above and count the problems:
# MAGIC * **`evt_009`–`evt_014` (new API version):** `amount` is now an object, and it doesn't fit `BIGINT` → **NULL**.
# MAGIC   `currency` is NULL too (it moved inside `amount`). No error, no warning. Revenue for those payments just
# MAGIC   disappears from reports.
# MAGIC * **`evt_008`:** `"1500"` is a JSON *string*, not a number → `amount` is **NULL** as well.
# MAGIC * **Wallet / bank transfer** payments: their details (`mobile_wallet`, `bank_transfer`) are not in the schema → **dropped**.
# MAGIC * **`refund`, `dispute`, `customer`, `payout`** events: everything specific to them (`reason`, `payment`,
# MAGIC   `email`, `previous_attributes`...) is **dropped**.
# MAGIC * **`evt_013`** (broken) → an all-NULL row.
# MAGIC
# MAGIC `from_json` is not wrong. It does exactly what the schema says. The problem is that **with semi-structured
# MAGIC data the schema is always out of date**. You'd need one schema per event type per API version, and to update
# MAGIC them before every source change.

# COMMAND ----------

display(parsed.selectExpr(
    "count(*) AS events",
    "count_if(j.id IS NULL) AS unparsed_rows",
    "count_if(j.data.object.amount IS NULL AND raw LIKE '%\"amount\"%') AS amounts_lost",
))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. Approach B: `VARIANT`
# MAGIC
# MAGIC `VARIANT` stores **any** JSON value in an efficient binary format. No schema up front, nothing dropped, and
# MAGIC fields are extracted at **query time** with paths.
# MAGIC
# MAGIC | Syntax | Meaning |
# MAGIC |---|---|
# MAGIC | `v:data.object.id` | navigate (result is still a VARIANT) |
# MAGIC | `v:data.object.items[0].sku` | array index |
# MAGIC | `v:type::STRING` | cast; ⚠️ **fails** if the value can't be cast |
# MAGIC | `variant_get(v, '$.path', 'type')` | same as `::`, path as a string |
# MAGIC | `try_variant_get(v, '$.path', 'type')` | **NULL instead of an error** on a bad cast |
# MAGIC | `schema_of_variant(v)` / `schema_of_variant_agg(v)` | the shape of one row / of all rows |
# MAGIC | `variant_explode(v)` | one row per array element or object key |
# MAGIC | `is_variant_null(v:path)` | is it a JSON `null` (vs. field missing)? |
# MAGIC
# MAGIC ⚠️ Paths are **case-sensitive**: `v:Type` ≠ `v:type`.

# COMMAND ----------

display(spark.sql("""
    SELECT
        v:id::STRING                                     AS event_id,
        v:type::STRING                                   AS event_type,
        timestamp_seconds(v:created::BIGINT)             AS created_at,
        v:data.object.payment_method.type::STRING        AS pm_type,
        v:data.object.payment_method.card.brand::STRING  AS card_brand,
        v:data.object.payment_method.mobile_wallet.provider::STRING AS wallet_provider,
        v:data.object.items[0].sku::STRING               AS first_sku,
        v:data.object.reason::STRING                     AS reason,            -- refunds & disputes
        v:data.previous_attributes                       AS changed_fields     -- customer.updated
    FROM webhooks_bronze
    WHERE v IS NOT NULL
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Discover the shapes: what does this feed actually contain?
# MAGIC This is how you find out about the API change **before** a business user does.

# COMMAND ----------

display(spark.sql("""
    SELECT v:api_version::STRING AS api_version,
           schema_of_variant_agg(v:data.object.amount) AS amount_shape,
           count(*) AS events
    FROM webhooks_bronze WHERE v IS NOT NULL
    GROUP BY 1
"""))
# 2026-06-01 -> OBJECT<currency, minor_units, value>: the breaking change, found in one query.
# 2026-01-15 -> VARIANT: "mixed types". Numbers AND a string ("1500") appear in the same field.

# COMMAND ----------

display(spark.sql("""
    SELECT v:type::STRING AS event_type, schema_of_variant_agg(v:data.object) AS object_schema
    FROM webhooks_bronze WHERE v IS NOT NULL
    GROUP BY 1 ORDER BY 1
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Type drift: `::` fails loudly, `try_variant_get` doesn't
# MAGIC Casting an **object** to `BIGINT` is an error. That's good when you want to know, and bad in a pipeline that
# MAGIC must not stop. Pick per column.

# COMMAND ----------

try:
    spark.sql("SELECT v:data.object.amount::BIGINT FROM webhooks_bronze").collect()
except Exception as e:
    print("ERROR:", str(e)[:250])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Handle both API versions explicitly
# MAGIC New version → `amount.minor_units`; old version → `amount` itself (number **or** numeric string).
# MAGIC Normalise to **minor units** (cents), which is what money should be stored as anyway.

# COMMAND ----------

display(spark.sql("""
    SELECT
        v:id::STRING           AS event_id,
        v:api_version::STRING  AS api_version,
        v:data.object.amount   AS amount_raw,
        coalesce(
            try_variant_get(v, '$.data.object.amount.minor_units', 'bigint'),   -- 2026-06-01 format
            try_variant_get(v, '$.data.object.amount', 'bigint')                -- 2026-01-15 format (and "1500")
        )                      AS amount_minor,
        upper(coalesce(
            v:data.object.amount.currency::STRING,
            v:data.object.currency::STRING
        ))                     AS currency
    FROM webhooks_bronze WHERE v IS NOT NULL
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Arrays and free-form maps → `variant_explode`
# MAGIC `metadata` keys differ per merchant. `variant_explode` turns an object into `(pos, key, value)` rows and an
# MAGIC array into `(pos, NULL, value)` rows, so you never need to know the keys in advance.

# COMMAND ----------

display(spark.sql("""
    SELECT b.v:id::STRING AS event_id, m.key AS metadata_key, m.value::STRING AS metadata_value
    FROM webhooks_bronze b,
         LATERAL variant_explode(b.v:data.object.metadata) AS m
    ORDER BY event_id, metadata_key
"""))

# COMMAND ----------

display(spark.sql("""
    SELECT b.v:id::STRING AS event_id,
           i.pos + 1 AS line_no,
           i.value:sku::STRING AS sku,
           i.value:qty::INT AS qty,
           coalesce(try_variant_get(i.value, '$.unit_amount', 'bigint'),
                    CAST(try_variant_get(i.value, '$.unit_amount.value', 'decimal(18,2)') * 100 AS BIGINT)) AS unit_amount_minor
    FROM webhooks_bronze b,
         LATERAL variant_explode(b.v:data.object.items) AS i
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### JSON `null` vs. missing field
# MAGIC `"receipt": null` is **present but null**; `"tracking_url"` is **missing**. In VARIANT these are different, which
# MAGIC is useful ("did the customer *clear* the field, or did the event not mention it?").

# COMMAND ----------

display(spark.sql("""
    SELECT
        v:data.object.evidence.receipt                       AS receipt,
        v:data.object.evidence.receipt IS NULL               AS receipt_is_sql_null,     -- false: it's a VARIANT null
        is_variant_null(v:data.object.evidence.receipt)      AS receipt_is_json_null,    -- true
        v:data.object.evidence.tracking_url IS NULL          AS tracking_is_missing      -- true: path doesn't exist
    FROM webhooks_bronze WHERE v:type::STRING = 'dispute.opened'
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Need a typed struct? Cast a VARIANT sub-tree to a `STRUCT`
# MAGIC Missing fields become NULL; fields not in the struct are ignored. You get `from_json`-style typing for **just
# MAGIC the part you need**, while the full payload stays available.

# COMMAND ----------

display(spark.sql("""
    SELECT v:id::STRING AS event_id,
           try_cast(v:data.object.payment_method AS STRUCT<type: STRING, card: STRUCT<brand: STRING, last4: STRING, exp_year: INT>>) AS pm
    FROM webhooks_bronze
    WHERE v:type::STRING LIKE 'payment.%'
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading JSON files straight into a VARIANT column
# MAGIC `singleVariantColumn` makes the JSON reader (and Auto Loader) put each whole record in one VARIANT column, with no
# MAGIC schema inference at all. That's a good bronze default for feeds like this.

# COMMAND ----------

try:
    df_sv = (
        spark.read.format("json")
        .option("singleVariantColumn", "payload")
        .option("mode", "PERMISSIVE")
        .load(f"{data_path}/payment_webhooks.jsonl")
    )
    df_sv.printSchema()
    display(df_sv.selectExpr("payload:id::STRING AS id", "payload:type::STRING AS type", "*"))
except Exception as e:
    print("singleVariantColumn not supported here (or the broken line was rejected):", str(e)[:300])

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. Performance: how much does "schema on read" cost?
# MAGIC
# MAGIC We generate **1 million** payment events and run the same query (sum of amount per event type) over:
# MAGIC 1. a **JSON string** column parsed with `from_json` *on every query*
# MAGIC 2. a **JSON string** column with `get_json_object` *on every query*
# MAGIC 3. a **VARIANT** column (already parsed, binary)
# MAGIC 4. **typed columns** (fully "shredded": the silver table)
# MAGIC
# MAGIC ⚠️ Timings on shared / serverless compute are noisy and caching can kick in. Look at the **order of magnitude**,
# MAGIC and re-run the cell to compare.

# COMMAND ----------

N = 1_000_000

spark.sql(f"""
    CREATE OR REPLACE TABLE perf_json_string AS
    SELECT to_json(named_struct(
        'id', concat('evt_', id),
        'type', CASE WHEN id % 10 < 7 THEN 'payment.succeeded' WHEN id % 10 < 9 THEN 'payment.failed' ELSE 'refund.created' END,
        'created', 1780308000 + id,
        'data', named_struct('object', named_struct(
            'id', concat('pay_', id),
            'amount', CAST(id % 20000 AS BIGINT),
            'currency', CASE WHEN id % 3 = 0 THEN 'usd' ELSE 'gbp' END,
            'customer', concat('cus_', id % 50000),
            'payment_method', named_struct('type', 'card', 'card', named_struct('brand', 'visa', 'last4', '4242')),
            'metadata', named_struct('order_id', CAST(id AS STRING), 'campaign', 'perf_test')
        ))
    )) AS raw
    FROM range({N})
""")
spark.sql("CREATE OR REPLACE TABLE perf_variant AS SELECT parse_json(raw) AS v FROM perf_json_string")
spark.sql("""
    CREATE OR REPLACE TABLE perf_typed AS
    SELECT v:type::STRING AS event_type, v:data.object.amount::BIGINT AS amount, v:data.object.currency::STRING AS currency
    FROM perf_variant
""")

for t in ["perf_json_string", "perf_variant", "perf_typed"]:
    d = spark.sql(f"DESCRIBE DETAIL {t}").select("sizeInBytes", "numFiles").first()
    print(f"{t:18s} {d.sizeInBytes / 1024 / 1024:8.1f} MB  in {d.numFiles} files")

# COMMAND ----------

queries = {
    "1. string + from_json": """
        SELECT j.type, sum(j.data.object.amount) FROM (
            SELECT from_json(raw, 'type STRING, data STRUCT<object: STRUCT<amount: BIGINT>>') AS j FROM perf_json_string
        ) GROUP BY 1""",
    "2. string + get_json_object": """
        SELECT get_json_object(raw, '$.type'), sum(CAST(get_json_object(raw, '$.data.object.amount') AS BIGINT))
        FROM perf_json_string GROUP BY 1""",
    "3. VARIANT": """
        SELECT v:type::STRING, sum(v:data.object.amount::BIGINT) FROM perf_variant GROUP BY 1""",
    "4. typed columns": """
        SELECT event_type, sum(amount) FROM perf_typed GROUP BY 1""",
}

timings = []
for label, q in queries.items():
    start = time.time()
    spark.sql(q).collect()
    timings.append((label, round(time.time() - start, 2)))

display(spark.createDataFrame(timings, "approach STRING, seconds DOUBLE"))

# COMMAND ----------

# MAGIC %md
# MAGIC **How to read the result:**
# MAGIC * **Storage is the clear signal.** In our test run: JSON strings **7.1 MB**, VARIANT **1.9 MB**, typed columns
# MAGIC   **0.2 MB**. VARIANT's binary format compresses far better than text, and typed columns compress best because
# MAGIC   Parquet stores each column separately with its own encoding and statistics.
# MAGIC * **Timings at 1M small events are close** (all under a second in our run), because the fixed per-query
# MAGIC   overhead of serverless dominates. Parsing cost grows with the data and the overhead doesn't: change `N` to
# MAGIC   `20_000_000` and re-run to see the gap open up on your workspace.
# MAGIC * The string approaches pay the **parse cost on every query**. VARIANT pays it **once** at write time; typed
# MAGIC   columns also let Delta skip files using min/max statistics. That's the reason for the hybrid pattern below.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5. The production pattern: **hybrid**
# MAGIC
# MAGIC * **Bronze:** `raw STRING` + `v VARIANT`, nothing lost, never fails
# MAGIC * **Silver:** promote the **hot fields** (used in filters, joins, group-bys, dashboards) to **typed columns**, and
# MAGIC   keep the full **`payload VARIANT`** next to them for everything else
# MAGIC * **Per-domain views** (payments, refunds...) on top, built from silver

# COMMAND ----------

spark.sql("""
    CREATE OR REPLACE TABLE webhooks_silver
    CLUSTER BY (event_type, created_at)
    AS SELECT
        v:id::STRING                                           AS event_id,
        v:type::STRING                                         AS event_type,
        timestamp_seconds(v:created::BIGINT)                   AS created_at,
        v:api_version::STRING                                  AS api_version,
        v:data.object.id::STRING                               AS object_id,
        coalesce(try_variant_get(v, '$.data.object.amount.minor_units', 'bigint'),
                 try_variant_get(v, '$.data.object.amount', 'bigint'))              AS amount_minor,
        upper(coalesce(v:data.object.amount.currency::STRING, v:data.object.currency::STRING)) AS currency,
        v:data.object.customer::STRING                         AS customer_id,
        v                                                      AS payload,
        _ingested_at
    FROM webhooks_bronze
    WHERE v IS NOT NULL
""")

spark.sql("""
    CREATE OR REPLACE VIEW payments_v AS
    SELECT event_id, event_type, created_at, object_id AS payment_id, amount_minor / 100 AS amount, currency, customer_id,
           payload:data.object.payment_method.type::STRING       AS payment_method,
           payload:data.object.payment_method.card.brand::STRING AS card_brand,
           payload:data.object.failure.code::STRING              AS failure_code,
           payload:data.object.metadata.order_id::STRING         AS order_id
    FROM webhooks_silver
    WHERE event_type LIKE 'payment.%'
""")

display(spark.table("webhooks_silver"))
display(spark.table("payments_v"))

# COMMAND ----------

# Broken events never reach silver, but bronze still has them for the support team
display(spark.sql("SELECT raw, _source_file FROM webhooks_bronze WHERE v IS NULL"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Things you can't do with a VARIANT column (extract and cast first)

# COMMAND ----------

for label, q in {
    "GROUP BY a variant": "SELECT v:type, count(*) FROM webhooks_bronze GROUP BY v:type",
    "compare a variant with a string": "SELECT count(*) FROM webhooks_bronze WHERE v:type = 'payment.succeeded'",
}.items():
    try:
        spark.sql(q).collect()
        print(f"✅ {label}: worked on this runtime")
    except Exception as e:
        print(f"❌ {label}: {str(e)[:160]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 6. Decision guide 📝
# MAGIC
# MAGIC | Situation | Use |
# MAGIC |---|---|
# MAGIC | Stable, documented schema (internal service, contract) | `from_json` / typed schema |
# MAGIC | Many event types, frequent changes, 3rd-party API | **`VARIANT`** in bronze |
# MAGIC | Free-form keys (`metadata`, `attributes`, tags) | `VARIANT` + `variant_explode`, or `MAP<STRING,STRING>` |
# MAGIC | Fields used in filters / joins / dashboards | **typed columns** in silver |
# MAGIC | Need a quick look at one field in a JSON string | `col:path` or `get_json_object` |
# MAGIC | Need full typed nested structs for one event type | `from_json` per type, or `CAST(v:sub AS STRUCT<...>)` |
# MAGIC | Payload may be broken | `try_parse_json` + keep `raw` |
# MAGIC | Value may have the wrong type | `try_variant_get` / `try_cast` |
# MAGIC
# MAGIC **Golden rules**
# MAGIC 1. `from_json` fails **silently** (NULLs), so monitor NULL rates on parsed columns.
# MAGIC 2. Bronze = **never lose data**: `VARIANT` (+ the raw string if payloads can be broken).
# MAGIC 3. Promote hot fields to typed columns; keep the VARIANT for the long tail.
# MAGIC 4. Watch the **shape** of your data over time with `schema_of_variant_agg`, grouped by version or source.
# MAGIC 5. Money in **minor units** (`BIGINT`) or `DECIMAL`, never `DOUBLE`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleanup (optional)

# COMMAND ----------

# for t in ["webhooks_bronze", "webhooks_silver", "perf_json_string", "perf_variant", "perf_typed"]:
#     spark.sql(f"DROP TABLE IF EXISTS {t}")
# spark.sql("DROP VIEW IF EXISTS payments_v")
