# Databricks notebook source
# MAGIC %md
# MAGIC # Schema Evolution with Auto Loader
# MAGIC
# MAGIC **Scenario (Retail POS):** every store's point-of-sale system drops a JSON file of sales into cloud storage.
# MAGIC The POS vendor ships software updates *without telling the data team*. Over five days the files change:
# MAGIC
# MAGIC | Day | File | What changed | Real-world cause |
# MAGIC |---|---|---|---|
# MAGIC | 1 | `batch_01_initial.json` | Baseline | First go-live |
# MAGIC | 2 | `batch_02_new_columns.json` | `loyalty_id`, `payment_method` **added** | Loyalty programme launched |
# MAGIC | 3 | `batch_03_type_changes.json` | `qty` sent as `"3"`, `unit_price` as `"N/A"` / `"6.25"` | A store on an old POS version |
# MAGIC | 4 | `batch_04_case_and_nested.json` | `Store_ID` (wrong case) + new nested `promo` struct | New store on a different POS build |
# MAGIC | 5 | `batch_05_renamed_column.json` | `unit_price` **renamed** to `price` | Vendor "cleaned up" their API |
# MAGIC
# MAGIC We will feed these files to Auto Loader under **each `cloudFiles.schemaEvolutionMode`** and see what happens.
# MAGIC
# MAGIC | Mode | New column arrives | Type mismatch | Stream fails? |
# MAGIC |---|---|---|---|
# MAGIC | `addNewColumns` *(default when no schema given)* | Schema evolves, stream **stops once**, then continues on restart | → `_rescued_data` | Yes, once per change |
# MAGIC | `rescue` | → `_rescued_data`, schema never changes | → `_rescued_data` | Never |
# MAGIC | `failOnNewColumns` | Stream fails, schema **not** updated | → `_rescued_data` | Yes, until you fix it |
# MAGIC | `none` *(default when you give a schema)* | **Silently ignored** | → `_rescued_data` only if `rescuedDataColumn` is set | Never |
# MAGIC
# MAGIC **Requirements:** Unity Catalog workspace (Free Edition works), serverless or DBR 15.4+.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Setup

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "autoloader_demo", "Schema")
dbutils.widgets.text("volume", "raw", "Volume")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")
spark.sql(f"USE {catalog}.{schema}")

base_path = f"/Volumes/{catalog}/{schema}/{volume}/autoloader_demo"
data_path = f"{base_path}/sample_data"
print(f"Sample data path: {data_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Copy the sample files into the volume
# MAGIC If this repo is cloned as a **Databricks Git folder**, the cell copies `../sample_data/*` automatically.
# MAGIC Otherwise upload the files to the path printed above, or use the CLI:
# MAGIC
# MAGIC `databricks fs cp -r ./de_production_patterns/01_autoloader_schema_evolution/sample_data dbfs:/Volumes/workspace/autoloader_demo/raw/autoloader_demo/sample_data`

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
    print(f"{repo_sample_dir} not found, so upload the files manually.")

display(dbutils.fs.ls(data_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Helpers
# MAGIC Every experiment gets its **own** landing folder, schema location, checkpoint and bronze table, so the modes
# MAGIC don't interfere with each other. `land()` simulates the POS system dropping a new file.
# MAGIC
# MAGIC ⚠️ **Why start and wait are in separate cells:** when a stream dies *inside* the cell that started it,
# MAGIC Databricks marks that cell as failed ("*Some streams terminated before this command could finish!*"),
# MAGIC **even if your code catches the exception**. Then **Run all** would stop at the first intended failure. So
# MAGIC streams that may stop on purpose are started with `start_autoloader()` in one cell and checked with
# MAGIC `wait_for()` in the next cell.

# COMMAND ----------

import time
from pyspark.sql import functions as F

def paths_for(name):
    root = f"{base_path}/runs/{name}"
    return {
        "landing": f"{root}/landing",
        "schema": f"{root}/_schema",          # Auto Loader stores the inferred schema + its history here
        "checkpoint": f"{root}/_checkpoint",  # which files were already processed
        "table": f"sales_bronze_{name}",
    }

def reset(name):
    p = paths_for(name)
    for k in ["landing", "schema", "checkpoint"]:
        dbutils.fs.rm(p[k], True)
    spark.sql(f"DROP TABLE IF EXISTS {p['table']}")
    return p

def land(name, *files):
    """Simulate new files arriving in the landing zone."""
    for f in files:
        dbutils.fs.cp(f"{data_path}/{f}", f"{paths_for(name)['landing']}/{f}")
        print(f"landed {f}")

def start_autoloader(name, mode, schema_hints=None):
    """Start an incremental run (availableNow = process every new file, then stop). Does NOT wait."""
    p = paths_for(name)
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", p["schema"])
        .option("cloudFiles.inferColumnTypes", True)        # default is: every column is a STRING
        .option("cloudFiles.schemaEvolutionMode", mode)
    )
    if schema_hints:
        reader = reader.option("cloudFiles.schemaHints", schema_hints)

    return (
        reader.load(p["landing"])
        .withColumn("_source_file", F.col("_metadata.file_name"))
        .withColumn("_ingested_at", F.current_timestamp())
        .writeStream
        .option("checkpointLocation", p["checkpoint"])
        .option("mergeSchema", True)      # let the DELTA TABLE accept the new columns too
        .trigger(availableNow=True)
        .toTable(p["table"])
    )

def wait_for(query):
    """Wait for a run started in an EARLIER cell. Returns True if it succeeded."""
    while query.isActive:
        time.sleep(1)
    err = query.exception()
    if err is None:
        print("✅ stream finished")
        return True
    msg = str(err)
    start = max(msg.find("[UNKNOWN_FIELD"), 0)
    print("❌ stream stopped:\n" + msg[start:start + 450])
    return False

def run_and_wait(name, mode, schema_hints=None):
    """For runs that are NOT expected to stop: start and wait in one cell."""
    start_autoloader(name, mode, schema_hints).awaitTermination()
    print("✅ stream finished")

def show_table(name):
    df = spark.table(paths_for(name)["table"])
    df.printSchema()
    display(df.orderBy("sale_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. `addNewColumns` (the default): evolve, stop, restart
# MAGIC
# MAGIC This is the mode most production pipelines use. When a **new column** appears, Auto Loader:
# MAGIC 1. stops the stream with `UnknownFieldException`
# MAGIC 2. **already writes the new, merged schema** to the schema location
# MAGIC 3. on the next start, reads with the new schema and carries on
# MAGIC
# MAGIC The stop is deliberate: it makes the schema change *visible*. In production you don't restart by hand:
# MAGIC * **Lakeflow Jobs**: give the task retries, and the retry restarts the stream with the new schema
# MAGIC   ```yaml
# MAGIC   tasks:
# MAGIC     - task_key: ingest_pos_sales
# MAGIC       max_retries: 3
# MAGIC       min_retry_interval_millis: 10000
# MAGIC   ```
# MAGIC * **Lakeflow Declarative Pipelines** restart the flow automatically
# MAGIC
# MAGIC Here, "restart" = run the next `start_autoloader` cell.

# COMMAND ----------

name = "add_new_columns"
reset(name)

# Day 1: baseline. Auto Loader infers the schema from this file.
land(name, "batch_01_initial.json")
run_and_wait(name, "addNewColumns")
show_table(name)

# COMMAND ----------

# MAGIC %md
# MAGIC 💡 Notice `_rescued_data` in the schema. Auto Loader **adds it automatically** when it infers the schema.
# MAGIC It is a JSON string holding anything that did not fit the schema.
# MAGIC
# MAGIC ### Day 2: `loyalty_id` + `payment_method` appear

# COMMAND ----------

land(name, "batch_02_new_columns.json")
q = start_autoloader(name, "addNewColumns")

# COMMAND ----------

wait_for(q)   # ❌ expected: UNKNOWN_FIELD_EXCEPTION ... "which can be fixed by an automatic retry: true"

# COMMAND ----------

q = start_autoloader(name, "addNewColumns")   # the "retry": picks up the evolved schema

# COMMAND ----------

wait_for(q)        # ✅
show_table(name)   # day-1 rows have NULL loyalty_id / payment_method, which is correct

# COMMAND ----------

# MAGIC %md
# MAGIC ### Day 3: type changes don't evolve the schema, they get **rescued**
# MAGIC `qty` was inferred as `BIGINT` and `unit_price` as `DOUBLE`. Values like `"3"`, `"N/A"`, `"6.25"` (strings) don't
# MAGIC fit, so the column becomes **NULL** and the original value is kept in `_rescued_data`. Nothing is lost, and the
# MAGIC stream doesn't stop.

# COMMAND ----------

land(name, "batch_03_type_changes.json")
run_and_wait(name, "addNewColumns")
display(spark.table(paths_for(name)["table"]).filter("_source_file = 'batch_03_type_changes.json'"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Day 4: wrong-cased column + new nested struct
# MAGIC * `promo` is new, so the schema evolves (one stop + restart)
# MAGIC * `Store_ID` ≠ `store_id`. Auto Loader is **case-sensitive by default** (`readerCaseSensitive = true`),
# MAGIC   so `Store_ID` is **rescued**, not merged into `store_id`. (Without this, Spark's case-insensitive matching
# MAGIC   could silently merge different columns.)

# COMMAND ----------

land(name, "batch_04_case_and_nested.json")
q = start_autoloader(name, "addNewColumns")

# COMMAND ----------

wait_for(q)   # ❌ expected: new field `promo`

# COMMAND ----------

q = start_autoloader(name, "addNewColumns")

# COMMAND ----------

wait_for(q)
display(spark.table(paths_for(name)["table"]).filter("_source_file = 'batch_04_case_and_nested.json'"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Day 5: a rename looks like "one column disappeared + a new one appeared"
# MAGIC Auto Loader cannot know `price` *is* `unit_price`. It adds `price` as a new column. **Renames always need a
# MAGIC human decision**, which you implement in silver (section 6).

# COMMAND ----------

land(name, "batch_05_renamed_column.json")
q = start_autoloader(name, "addNewColumns")

# COMMAND ----------

wait_for(q)   # ❌ expected: new field `price`

# COMMAND ----------

q = start_autoloader(name, "addNewColumns")

# COMMAND ----------

wait_for(q)
show_table(name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Look inside the schema location: Auto Loader keeps a **history** of every schema version
# MAGIC Each change writes a new numbered file in `_schemas/`. Great for auditing *when* the source changed.

# COMMAND ----------

schema_files = sorted(dbutils.fs.ls(f"{paths_for(name)['schema']}/_schemas"), key=lambda f: f.name)
for f in schema_files:
    print(f"--- schema version {f.name} ---")
    print(dbutils.fs.head(f.path, 3000)[:600], "\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. `rescue`: never fail, never change the schema
# MAGIC
# MAGIC The schema is frozen after the first inference. **Everything new goes to `_rescued_data`.**
# MAGIC Use it when the bronze table must be stable (downstream consumers break on new columns) or when the
# MAGIC stream must *never* stop (e.g. a 24/7 SLA).

# COMMAND ----------

name = "rescue"
reset(name)
land(name, "batch_01_initial.json")
run_and_wait(name, "rescue")

land(name, "batch_02_new_columns.json", "batch_03_type_changes.json",
           "batch_04_case_and_nested.json", "batch_05_renamed_column.json")
run_and_wait(name, "rescue")   # no failure, no restart needed
show_table(name)               # same 6 data columns as day 1; the rest is in _rescued_data

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. `failOnNewColumns`: stop and wait for a human
# MAGIC
# MAGIC The stream fails and keeps failing; the schema is **not** updated. Someone must review the change and either
# MAGIC update the schema / hints or switch mode. Use it for **regulated / contract-driven** feeds (finance,
# MAGIC healthcare) where an unreviewed column might contain PII or break a data contract.

# COMMAND ----------

name = "fail_on_new"
reset(name)
land(name, "batch_01_initial.json")
run_and_wait(name, "failOnNewColumns")

land(name, "batch_02_new_columns.json")
q = start_autoloader(name, "failOnNewColumns")

# COMMAND ----------

wait_for(q)   # ❌

# COMMAND ----------

q = start_autoloader(name, "failOnNewColumns")   # "retry"...

# COMMAND ----------

wait_for(q)   # ❌ still failing: a restart does NOT fix it. A human has to.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. `none`: the silent data-loss mode ⚠️
# MAGIC
# MAGIC New columns are **ignored**. The stream is happy, the dashboards look fine, and `loyalty_id` is quietly thrown
# MAGIC away for weeks until someone asks "where is the loyalty data?". If you must use it, at least set
# MAGIC `rescuedDataColumn` so the data is kept somewhere.

# COMMAND ----------

name = "none_mode"
reset(name)
land(name, "batch_01_initial.json", "batch_02_new_columns.json")

# An explicit schema (what most people do when they use 'none'), WITHOUT a rescued data column
(
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaEvolutionMode", "none")
    .schema("sale_id STRING, store_id STRING, sku STRING, qty INT, unit_price DECIMAL(10,2), sold_at TIMESTAMP")
    .load(paths_for(name)["landing"])
    .writeStream
    .option("checkpointLocation", paths_for(name)["checkpoint"])
    .trigger(availableNow=True)
    .toTable(paths_for(name)["table"])
    .awaitTermination()
)
show_table(name)   # loyalty_id and payment_method are simply gone

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5. `schemaHints`: infer the rest, but pin the columns you care about
# MAGIC
# MAGIC Inference guesses `unit_price` as `DOUBLE` (bad for money) and `sold_at` as `STRING`. Hints fix just those
# MAGIC columns, while everything else is still inferred and can still evolve. Hints can also **pre-declare** columns
# MAGIC (and nested structs) that have not arrived yet.

# COMMAND ----------

name = "with_hints"
reset(name)
land(name, "batch_01_initial.json", "batch_02_new_columns.json", "batch_03_type_changes.json")

hints = """
    sale_id STRING,
    qty INT,
    unit_price DECIMAL(10,2),
    sold_at TIMESTAMP,
    promo STRUCT<code: STRING, pct: INT>
"""
run_and_wait(name, "addNewColumns", schema_hints=hints)   # all files are there at the first inference: no stop
show_table(name)

# COMMAND ----------

# MAGIC %md
# MAGIC 💡 Alternative to hints: `cloudFiles.inferColumnTypes = false` (the default) lands **every column as STRING**.
# MAGIC Type mismatches can then never happen in bronze. You cast in silver with `try_cast`, the same
# MAGIC "land raw, type later" rule from the JSON demo. Many teams prefer this for very messy sources.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 6. Silver: recover the rescued data and make schema drift **observable**
# MAGIC
# MAGIC Bronze kept everything. Now we decide what it means:
# MAGIC * `Store_ID` → `store_id` (case variant)
# MAGIC * `price` → `unit_price` (rename)
# MAGIC * `"6.25"` / `"3"` strings → numbers, `"N/A"` → NULL
# MAGIC
# MAGIC We use the `rescue` table from section 2 because it holds everything in one stable schema.

# COMMAND ----------

bronze = spark.table(paths_for("rescue")["table"])

silver = bronze.selectExpr(
    "sale_id",
    "coalesce(store_id, _rescued_data:Store_ID::STRING)                              AS store_id",
    "sku",
    "coalesce(qty, try_cast(_rescued_data:qty::STRING AS INT))                       AS qty",
    """coalesce(
          unit_price,
          try_cast(_rescued_data:unit_price::STRING AS DECIMAL(10,2)),
          try_cast(_rescued_data:price::STRING AS DECIMAL(10,2))
       )                                                                             AS unit_price""",
    "try_cast(sold_at AS TIMESTAMP)                                                  AS sold_at",
    "_rescued_data:loyalty_id::STRING                                                AS loyalty_id",
    "_rescued_data:payment_method::STRING                                            AS payment_method",
    "_rescued_data:promo.code::STRING                                                AS promo_code",
    "_rescued_data:promo.pct::INT                                                    AS promo_pct",
    "_rescued_data",
    "_source_file",
)
display(silver.orderBy("sale_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drift monitor: which unexpected fields arrived, from which file?
# MAGIC Schedule a query like this and alert on it (Databricks SQL Alert). This turns "the vendor changed something"
# MAGIC from a surprise into a notification.

# COMMAND ----------

drift_report = (
    bronze.filter("_rescued_data IS NOT NULL")
    .select("_source_file", F.explode(F.json_object_keys("_rescued_data")).alias("rescued_field"))
    .filter("rescued_field != '_file_path'")     # Auto Loader adds the source file path inside _rescued_data
    .groupBy("rescued_field")
    .agg(F.count("*").alias("rows"), F.collect_set("_source_file").alias("files"))
    .orderBy(F.desc("rows"))
)
display(drift_report)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 7. Not streaming? `rescuedDataColumn` works in batch reads too

# COMMAND ----------

display(
    spark.read
    .schema("sale_id STRING, store_id STRING, sku STRING, qty INT, unit_price DECIMAL(10,2), sold_at TIMESTAMP")
    .option("rescuedDataColumn", "_rescued_data")
    .json(f"{data_path}/*.json")
    .filter("_rescued_data IS NOT NULL")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 8. Cheat sheet 📝
# MAGIC
# MAGIC | Situation | Choose |
# MAGIC |---|---|
# MAGIC | Normal bronze ingestion, schema changes are welcome | `addNewColumns` + **job retries** + `mergeSchema` on the sink |
# MAGIC | Stream must never stop / bronze schema must be stable | `rescue` |
# MAGIC | Data contract, PII risk, regulated data | `failOnNewColumns` + alert |
# MAGIC | Fixed schema, extra fields truly irrelevant | `none` **plus** `rescuedDataColumn` |
# MAGIC | Money, timestamps, IDs with leading zeros | `cloudFiles.schemaHints` |
# MAGIC | Very messy source | `inferColumnTypes = false` (all strings) → `try_cast` in silver |
# MAGIC | Renamed / re-cased columns | `coalesce(col, _rescued_data:OtherName)` in silver |
# MAGIC | Knowing *when* the source changed | `_schemas/` history + drift monitor on `_rescued_data` |
# MAGIC
# MAGIC **Golden rules**
# MAGIC 1. Bronze must **never lose data**: either evolve or rescue, never `none` without `rescuedDataColumn`.
# MAGIC 2. A schema change should **notify** someone, not silently change dashboards.
# MAGIC 3. Renames and meaning changes need a **human decision** in silver. No tool can guess them.
# MAGIC 4. Keep `_source_file` / `_ingested_at` on every bronze row: you will need them when debugging.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleanup (optional)

# COMMAND ----------

# for n in ["add_new_columns", "rescue", "fail_on_new", "none_mode", "with_hints"]:
#     reset(n)
# dbutils.fs.rm(base_path, True)
