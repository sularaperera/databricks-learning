# Databricks notebook source
# MAGIC %md
# MAGIC # Slowly Changing Dimensions: SCD Type 1 & Type 2 with `MERGE`
# MAGIC
# MAGIC **Scenario (CRM):** the CRM database streams customer changes (CDC) as JSON. Each record says *what* happened
# MAGIC (`op` = INSERT / UPDATE / DELETE) and *in which order* (`change_seq`, the database's commit sequence number).
# MAGIC
# MAGIC Two teams want two different tables:
# MAGIC
# MAGIC | | **SCD Type 1** | **SCD Type 2** |
# MAGIC |---|---|---|
# MAGIC | Keeps | only the **latest** value | **every version**, with validity dates |
# MAGIC | Question it answers | "What is the customer's email *now*?" | "What tier was the customer *on 5 March*?" |
# MAGIC | Typical use | CRM lookups, emails, operational reports | revenue by tier/region *at the time of sale*, audit |
# MAGIC | Cost | 1 row per customer | 1 row per customer **per change** |
# MAGIC
# MAGIC **The data gets harder batch by batch:**
# MAGIC
# MAGIC | Batch | Contents |
# MAGIC |---|---|
# MAGIC | `batch_01_initial` | 5 new customers |
# MAGIC | `batch_02_changes` | city / tier / email changes, a new customer, and a **no-op update** (C001: nothing changed) |
# MAGIC | `batch_03_messy` | **two changes for the same customer** in one batch (C002), an **exact duplicate** (redelivery), an **out-of-order** change from January arriving in March (C003), a **DELETE** (C005) |
# MAGIC
# MAGIC The declarative version (`AUTO CDC`, formerly `APPLY CHANGES`) is in `../pipeline/customers_auto_cdc_pipeline.py`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Setup

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "scd_demo", "Schema")
dbutils.widgets.text("volume", "raw", "Volume")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")
spark.sql(f"USE {catalog}.{schema}")

base_path = f"/Volumes/{catalog}/{schema}/{volume}/scd_demo"
data_path = f"{base_path}/sample_data"
print(f"Sample data path: {data_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC Manual upload alternative:
# MAGIC `databricks fs cp -r ./de_production_patterns/03_scd_type1_type2/sample_data dbfs:/Volumes/workspace/scd_demo/raw/scd_demo/sample_data`

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

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

cdc_schema = """
    customer_id STRING, full_name STRING, email STRING, city STRING, tier STRING,
    op STRING, change_seq BIGINT, event_ts TIMESTAMP
"""

BATCHES = [
    "customers_cdc_batch_01_initial.json",
    "customers_cdc_batch_02_changes.json",
    "customers_cdc_batch_03_messy.json",
]

def read_batch(file_name):
    return spark.read.schema(cdc_schema).json(f"{data_path}/{file_name}")

for b in BATCHES:
    print(f"---- {b}")
    display(read_batch(b))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. SCD Type 1: overwrite in place
# MAGIC
# MAGIC Three rules make an SCD1 MERGE production-safe:
# MAGIC 1. **One source row per key**: keep only the latest change per customer in the batch
# MAGIC 2. **Never go backwards**: only apply a change if `change_seq` is newer than what the table already has
# MAGIC 3. **Handle deletes**: `op = 'DELETE'` removes the row (or sets an `is_deleted` flag: a *soft* delete)

# COMMAND ----------

spark.sql("""
    CREATE OR REPLACE TABLE dim_customer_scd1 (
        customer_id STRING NOT NULL,
        full_name   STRING,
        email       STRING,
        city        STRING,
        tier        STRING,
        change_seq  BIGINT,
        updated_at  TIMESTAMP
    )
""")

def merge_scd1_naive(batch_df):
    """❌ What most people write first."""
    batch_df.createOrReplaceTempView("scd1_source")
    spark.sql("""
        MERGE INTO dim_customer_scd1 t
        USING scd1_source s
        ON t.customer_id = s.customer_id
        WHEN MATCHED THEN UPDATE SET
            full_name = s.full_name, email = s.email, city = s.city, tier = s.tier,
            change_seq = s.change_seq, updated_at = s.event_ts
        WHEN NOT MATCHED THEN INSERT (customer_id, full_name, email, city, tier, change_seq, updated_at)
            VALUES (s.customer_id, s.full_name, s.email, s.city, s.tier, s.change_seq, s.event_ts)
    """)

def merge_scd1(batch_df):
    """✅ Dedup to the latest change per key, ignore stale changes, apply deletes."""
    latest = (
        batch_df
        .withColumn("_rn", F.row_number().over(Window.partitionBy("customer_id").orderBy(F.desc("change_seq"))))
        .filter("_rn = 1")
        .drop("_rn")
    )
    latest.createOrReplaceTempView("scd1_source")
    spark.sql("""
        MERGE INTO dim_customer_scd1 t
        USING scd1_source s
        ON t.customer_id = s.customer_id
        WHEN MATCHED AND s.change_seq > t.change_seq AND s.op = 'DELETE' THEN DELETE
        WHEN MATCHED AND s.change_seq > t.change_seq THEN UPDATE SET
            full_name = s.full_name, email = s.email, city = s.city, tier = s.tier,
            change_seq = s.change_seq, updated_at = s.event_ts
        WHEN NOT MATCHED AND s.op != 'DELETE' THEN INSERT (customer_id, full_name, email, city, tier, change_seq, updated_at)
            VALUES (s.customer_id, s.full_name, s.email, s.city, s.tier, s.change_seq, s.event_ts)
    """)

# COMMAND ----------

# Batches 1 and 2 are "nice", so even the naive merge works
merge_scd1_naive(read_batch(BATCHES[0]))
merge_scd1_naive(read_batch(BATCHES[1]))
display(spark.table("dim_customer_scd1").orderBy("customer_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### ❌ Batch 3 breaks the naive merge
# MAGIC C002 appears **3 times** in the batch. MERGE refuses to update one target row from several source rows,
# MAGIC because it can't know which one should win.

# COMMAND ----------

try:
    merge_scd1_naive(read_batch(BATCHES[2]))
except Exception as e:
    print("ERROR:", str(e)[:400])

# COMMAND ----------

# MAGIC %md
# MAGIC Even if it had worked, the naive version would have **overwritten C003's March data with the January
# MAGIC change** (out-of-order) and **ignored the delete**.
# MAGIC
# MAGIC ### ✅ The safe merge

# COMMAND ----------

merge_scd1(read_batch(BATCHES[2]))
display(spark.table("dim_customer_scd1").orderBy("customer_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC Check the result:
# MAGIC * **C002** → Dehiwala / PLATINUM (the latest of its 3 rows, `change_seq` 302)
# MAGIC * **C003** → still Galle / SILVER: the January change (`change_seq` 150 < 202) was ignored
# MAGIC * **C005** → gone (deleted)
# MAGIC
# MAGIC 💡 **Idempotency:** run the cell again. Nothing changes, because every change has `change_seq <=` the target's.
# MAGIC That matters: jobs get retried, and a retry must never corrupt data.

# COMMAND ----------

merge_scd1(read_batch(BATCHES[2]))   # re-run: no effect
display(spark.table("dim_customer_scd1").orderBy("customer_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ⚠️ **Hard-delete trap:** after C005 is deleted, a *late* UPDATE for C005 (with an older `change_seq`)
# MAGIC would find no row and be **inserted again**, bringing a deleted customer back. Production fixes:
# MAGIC soft deletes (`is_deleted = true` and keep `change_seq`), or keep "tombstones" for a while.
# MAGIC `AUTO CDC` keeps tombstones for you (see the pipeline).

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. SCD Type 2: keep history
# MAGIC
# MAGIC Each version is a row with `effective_from`, `effective_to` and `is_current`. A change means:
# MAGIC **close** the current row (set `effective_to`, `is_current = false`) **and insert** the new version.
# MAGIC
# MAGIC One MERGE can't both update a row *and* insert a new one for the same key. The classic trick is to
# MAGIC **stage** the source twice:
# MAGIC * once with `merge_key = customer_id` → **matches** the current row → close it
# MAGIC * once with `merge_key = NULL` → **never matches** → insert the new version
# MAGIC
# MAGIC On top of that, real CDC needs:
# MAGIC
# MAGIC | Problem | Handling below |
# MAGIC |---|---|
# MAGIC | Exact duplicates (redelivery) | `dropDuplicates(customer_id, change_seq)` |
# MAGIC | Several changes per key in one batch | build **all** versions with `lead()`, insert all, close only the old current row |
# MAGIC | No-op updates (nothing tracked changed) | compare a **hash** of tracked columns with the previous version |
# MAGIC | Out-of-order (older than what's applied) | skip and **log** it in `scd2_skipped_changes` |
# MAGIC | Deletes | close the current row, insert nothing |
# MAGIC | Re-running a batch | same as out-of-order: already applied → skipped |

# COMMAND ----------

TRACKED_COLUMNS = ["full_name", "email", "city", "tier"]   # a change in any of these creates a new version
HIGH_DATE = "9999-12-31 00:00:00"                          # "valid until further notice"

def create_scd2_table(table):
    spark.sql(f"""
        CREATE OR REPLACE TABLE {table} (
            customer_sk    BIGINT GENERATED ALWAYS AS IDENTITY,   -- surrogate key: facts point to a VERSION
            customer_id    STRING NOT NULL,                       -- business key
            full_name      STRING,
            email          STRING,
            city           STRING,
            tier           STRING,
            row_hash       STRING,
            change_seq     BIGINT,
            effective_from TIMESTAMP,
            effective_to   TIMESTAMP,
            is_current     BOOLEAN
        )
    """)
    spark.sql(f"CREATE OR REPLACE TABLE {table}_skipped (customer_id STRING, change_seq BIGINT, op STRING, event_ts TIMESTAMP, reason STRING, logged_at TIMESTAMP)")

# COMMAND ----------

def merge_scd2(batch_df, table):
    session = batch_df.sparkSession       # works in a normal cell AND inside foreachBatch
    by_key = Window.partitionBy("customer_id").orderBy("change_seq")

    # 1. exact duplicates out
    src = batch_df.dropDuplicates(["customer_id", "change_seq"])

    # 2. what the target already knows per customer
    target_state = session.sql(f"""
        SELECT customer_id,
               max(change_seq)                                  AS last_seq,
               max(CASE WHEN is_current THEN row_hash END)      AS current_hash
        FROM {table} GROUP BY customer_id
    """)
    applied = session.table(table).select("customer_id", "change_seq", F.lit(True).alias("_applied"))

    src = (
        src
        .withColumn("row_hash", F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c), F.lit("<null>")) for c in TRACKED_COLUMNS]), 256))
        .join(target_state, "customer_id", "left")
    )

    # 3. stale changes: log them, don't apply them
    (
        src.filter("change_seq <= last_seq")
        .join(applied, ["customer_id", "change_seq"], "left")
        .select(
            "customer_id", "change_seq", "op", "event_ts",
            F.when(F.col("_applied"), "ALREADY_APPLIED").otherwise("OUT_OF_ORDER").alias("reason"),
            F.current_timestamp().alias("logged_at"),
        )
        .write.mode("append").saveAsTable(f"{table}_skipped")
    )
    src = src.filter("last_seq IS NULL OR change_seq > last_seq")

    # 4. no-op updates out (tracked values identical to the previous version)
    src = (
        src
        .withColumn("_prev_hash", F.coalesce(F.lag("row_hash").over(by_key), F.col("current_hash")))
        .filter("(op = 'DELETE' AND _prev_hash IS NOT NULL) OR (op != 'DELETE' AND (_prev_hash IS NULL OR row_hash != _prev_hash))")
    )

    # 5. validity window of every new version inside this batch
    src = (
        src
        .withColumn("_next_ts", F.lead("event_ts").over(by_key))
        .withColumn("effective_to", F.coalesce("_next_ts", F.lit(HIGH_DATE).cast("timestamp")))
        .withColumn("is_current", F.col("_next_ts").isNull() & (F.col("op") != "DELETE"))
        .withColumn("_rn", F.row_number().over(by_key))
    )

    # 6. stage: (a) first change per EXISTING key closes the current row, (b) every non-delete version is inserted
    close_rows = src.filter("_rn = 1 AND current_hash IS NOT NULL").withColumn("merge_key", F.col("customer_id"))
    insert_rows = src.filter("op != 'DELETE'").withColumn("merge_key", F.lit(None).cast("string"))
    close_rows.unionByName(insert_rows).createOrReplaceTempView("scd2_staged")

    session.sql(f"""
        MERGE INTO {table} t
        USING scd2_staged s
        ON t.customer_id = s.merge_key AND t.is_current
        WHEN MATCHED THEN UPDATE SET
            is_current = false,
            effective_to = s.event_ts
        WHEN NOT MATCHED AND s.merge_key IS NULL THEN INSERT
            (customer_id, full_name, email, city, tier, row_hash, change_seq, effective_from, effective_to, is_current)
            VALUES (s.customer_id, s.full_name, s.email, s.city, s.tier, s.row_hash, s.change_seq,
                    s.event_ts, s.effective_to, s.is_current)
    """)

def show_scd2(table):
    display(spark.sql(f"""
        SELECT customer_sk, customer_id, full_name, email, city, tier, change_seq,
               effective_from, effective_to, is_current
        FROM {table} ORDER BY customer_id, change_seq
    """))

# COMMAND ----------

create_scd2_table("dim_customer_scd2")

merge_scd2(read_batch(BATCHES[0]), "dim_customer_scd2")
show_scd2("dim_customer_scd2")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Batch 2
# MAGIC * C002, C003, C004 get a **second version**; their first version is closed
# MAGIC * C006 is new
# MAGIC * C001's update changed nothing → **no new version** (without the hash check you'd get useless duplicate history)
# MAGIC
# MAGIC 💡 `customer_sk` values have gaps (1, 2, 7, 9...). Identity columns guarantee **unique**, not consecutive, values.
# MAGIC Never build logic on them being gap-free.

# COMMAND ----------

merge_scd2(read_batch(BATCHES[1]), "dim_customer_scd2")
show_scd2("dim_customer_scd2")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Batch 3: the messy one
# MAGIC * C002 gets **two** new versions (PLATINUM in Colombo, then Dehiwala); the duplicate is ignored
# MAGIC * C005's current row is **closed** and there is no new version (deleted)
# MAGIC * C003's January change is **skipped** and logged as `OUT_OF_ORDER`

# COMMAND ----------

merge_scd2(read_batch(BATCHES[2]), "dim_customer_scd2")
show_scd2("dim_customer_scd2")

# COMMAND ----------

display(spark.table("dim_customer_scd2_skipped"))

# COMMAND ----------

# Idempotency check: re-run batch 3. Nothing changes; the rows are logged as ALREADY_APPLIED.
before = spark.table("dim_customer_scd2").count()
merge_scd2(read_batch(BATCHES[2]), "dim_customer_scd2")
print("rows before:", before, " rows after:", spark.table("dim_customer_scd2").count())
display(spark.table("dim_customer_scd2_skipped").orderBy("logged_at", "customer_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sanity checks you should automate on every SCD2 table
# MAGIC Both queries must return **zero rows**.

# COMMAND ----------

# 1. at most one current row per customer
display(spark.sql("SELECT customer_id, count(*) AS current_rows FROM dim_customer_scd2 WHERE is_current GROUP BY customer_id HAVING count(*) > 1"))

# 2. no gaps or overlaps: each version must end exactly when the next one starts
display(spark.sql("""
    SELECT * FROM (
        SELECT customer_id, change_seq, effective_to,
               lead(effective_from) OVER (PARTITION BY customer_id ORDER BY effective_from) AS next_from
        FROM dim_customer_scd2
    ) WHERE next_from IS NOT NULL AND next_from != effective_to
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using SCD2: point-in-time ("as of") queries
# MAGIC "Which tier and city was each customer in on **5 March 2026**?" Join facts on
# MAGIC `event_time >= effective_from AND event_time < effective_to`.

# COMMAND ----------

display(spark.sql("""
    SELECT customer_id, full_name, city, tier
    FROM dim_customer_scd2
    WHERE TIMESTAMP'2026-03-05 12:00:00' >= effective_from
      AND TIMESTAMP'2026-03-05 12:00:00' <  effective_to
    ORDER BY customer_id
"""))
# C002 -> PLATINUM in Colombo (moved to Dehiwala on 10 March).
# C005 is missing: it was deleted at 10:00 on 5 March, two hours before the "as of" time. Try 09:00 instead.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. The same logic in a stream: Auto Loader + `foreachBatch`
# MAGIC
# MAGIC In production, CDC files arrive continuously. `foreachBatch` lets you run **any batch logic** (like our MERGE)
# MAGIC on each micro-batch. `maxFilesPerTrigger = 1` makes each file its own micro-batch, the same as above.
# MAGIC
# MAGIC Note `batch_df.sparkSession` inside `merge_scd2`: on serverless / Spark Connect, the function runs in a
# MAGIC separate process and **must not use the notebook's global `spark`**.

# COMMAND ----------

stream_table = "dim_customer_scd2_stream"
create_scd2_table(stream_table)
checkpoint = f"{base_path}/_checkpoints/scd2_stream"
dbutils.fs.rm(checkpoint, True)

(
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.maxFilesPerTrigger", 1)
    .schema(cdc_schema)
    .load(data_path)
    .writeStream
    .foreachBatch(lambda batch_df, batch_id: merge_scd2(batch_df, stream_table))
    .option("checkpointLocation", checkpoint)
    .trigger(availableNow=True)
    .start()
    .awaitTermination()
)

# Same history as the batch-by-batch run? Both directions must be empty.
cols = "customer_id, full_name, email, city, tier, change_seq, effective_from, effective_to, is_current"
diff = spark.sql(f"""
    (SELECT {cols} FROM dim_customer_scd2 EXCEPT SELECT {cols} FROM {stream_table})
    UNION ALL
    (SELECT {cols} FROM {stream_table} EXCEPT SELECT {cols} FROM dim_customer_scd2)
""")
print("differences:", diff.count())

# COMMAND ----------

# MAGIC %md
# MAGIC 💡 Remove `maxFilesPerTrigger` and all three files land in **one** micro-batch. Then C003's January change is
# MAGIC no longer "late" (it is in the same batch as the other C003 changes), so it gets inserted **in the right
# MAGIC place in history**. Handling truly late data correctly needs history *re-writing*, which is exactly what
# MAGIC `AUTO CDC` does for you. That is the main reason to use it instead of hand-written MERGE.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. Compare with `AUTO CDC` (after running the pipeline)
# MAGIC
# MAGIC Deploy and run `customers_auto_cdc_pipeline` (see README), then run these cells. What to look for:
# MAGIC * `AUTO CDC` produces `__START_AT` / `__END_AT` instead of `effective_from` / `effective_to`. They hold the
# MAGIC   **`sequence_by` value** (here `change_seq`: 103 → 150 → 202), not timestamps. Sequence by `event_ts`
# MAGIC   instead if you want dates, but only if the source's timestamps are reliable enough to order changes.
# MAGIC * **C003:** the January change (`change_seq` 150, Matara) *is* in its history, **in the correct position**,
# MAGIC   between versions 103 and 202. Our MERGE could only skip it.
# MAGIC * **C004:** the email change (203) did **not** create a new version, because `track_history_column_list` only
# MAGIC   tracks `city` and `tier`. The current row was updated in place.
# MAGIC * **C005:** its history ends at 303 (the delete), and it's gone from the SCD1 table.

# COMMAND ----------

try:
    display(spark.table("customers_scd1_auto").orderBy("customer_id"))
    display(spark.table("customers_scd2_auto").orderBy("customer_id", "__START_AT"))
except Exception as e:
    print("Pipeline tables not found yet. Run the pipeline first.", str(e)[:200])

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5. Cheat sheet 📝
# MAGIC
# MAGIC | Problem | SCD1 MERGE | SCD2 MERGE | AUTO CDC |
# MAGIC |---|---|---|---|
# MAGIC | Many changes per key in a batch | keep latest (`row_number`) | build all versions (`lead`) | automatic |
# MAGIC | Exact duplicates | latest-wins removes them | `dropDuplicates(key, seq)` | automatic |
# MAGIC | Out-of-order change | `WHEN MATCHED AND s.seq > t.seq` | skip + log | **placed correctly in history** |
# MAGIC | No-op update | harmless overwrite | hash compare | automatic (`track_history_column_list`) |
# MAGIC | Delete | `THEN DELETE` (or soft delete) | close current row | `apply_as_deletes` |
# MAGIC | Retry / rerun | sequence guard | sequence guard | checkpoints |
# MAGIC
# MAGIC **Golden rules**
# MAGIC 1. Always order by a **sequence from the source** (`change_seq`, LSN, `updated_at`), never by arrival time.
# MAGIC 2. A MERGE must be **idempotent**: running the same batch twice changes nothing.
# MAGIC 3. SCD2 needs a **surrogate key** (`customer_sk`) so facts point to the version valid at the time.
# MAGIC 4. Automate the two SCD2 checks: one current row per key, and no gaps or overlaps.
# MAGIC 5. Prefer `AUTO CDC` in Lakeflow pipelines; write MERGE yourself when you need custom logic or are outside a pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleanup (optional)

# COMMAND ----------

# for t in ["dim_customer_scd1", "dim_customer_scd2", "dim_customer_scd2_skipped",
#           "dim_customer_scd2_stream", "dim_customer_scd2_stream_skipped"]:
#     spark.sql(f"DROP TABLE IF EXISTS {t}")
