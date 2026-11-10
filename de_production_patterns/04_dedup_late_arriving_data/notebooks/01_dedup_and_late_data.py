# Databricks notebook source
# MAGIC %md
# MAGIC # Deduplication, Late-Arriving Data & Idempotent Reprocessing
# MAGIC
# MAGIC **Scenario (Logistics):** delivery vans scan parcels (`PICKED_UP` → `IN_TRANSIT` → `OUT_FOR_DELIVERY` →
# MAGIC `DELIVERED`). The handheld scanners send events over a flaky mobile network, so:
# MAGIC
# MAGIC | Problem | In the sample data | Why it happens |
# MAGIC |---|---|---|
# MAGIC | **Exact duplicate** | `E003` twice in file 01 | network layer sent the same message twice |
# MAGIC | **Retry duplicate** | `E005` in files 01 + 02, `E012` in files 03 + 04 (different `received_at` / `attempt`) | device didn't get an ACK, so it re-sent |
# MAGIC | **Late, but within tolerance** | `E013` (10:06, received 10:38), `E018` (10:20, received 11:36) | van in a tunnel / poor signal |
# MAGIC | **Very late** | `E017` (10:08, received 11:35) | van 3 was offline for over an hour |
# MAGIC
# MAGIC Two timestamps matter:
# MAGIC * `event_time`: when the scan **happened** (on the device). Business logic uses this.
# MAGIC * `received_at`: when **we** got it. Lateness = `received_at - event_time`.
# MAGIC
# MAGIC The business wants a **gold table: scans per van per 15 minutes** that is *correct*, even when data is late
# MAGIC or duplicated, and even when a job is re-run.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Setup

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "dedup_demo", "Schema")
dbutils.widgets.text("volume", "raw", "Volume")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")
spark.sql(f"USE {catalog}.{schema}")

base_path = f"/Volumes/{catalog}/{schema}/{volume}/dedup_demo"
data_path = f"{base_path}/sample_data"
print(f"Sample data path: {data_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC Manual upload alternative:
# MAGIC `databricks fs cp -r ./de_production_patterns/04_dedup_late_arriving_data/sample_data dbfs:/Volumes/workspace/dedup_demo/raw/dedup_demo/sample_data`

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

event_schema = """
    event_id STRING, parcel_id STRING, van_id STRING, status STRING,
    event_time TIMESTAMP, received_at TIMESTAMP, attempt INT
"""
FILES = ["scan_events_01.json", "scan_events_02.json", "scan_events_03.json", "scan_events_04.json"]

def read_files(*files):
    return (
        spark.read.schema(event_schema).json([f"{data_path}/{f}" for f in files])
        .withColumn("_source_file", F.col("_metadata.file_name"))
    )

all_events = read_files(*FILES)
display(all_events.orderBy("received_at"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. Batch deduplication: three levels
# MAGIC
# MAGIC ### Level 1: exact duplicates → `distinct()` / `dropDuplicates()`
# MAGIC Removes `E003`'s copy, but **not** the retries: `E005` attempt 1 and attempt 2 differ in `received_at`.

# COMMAND ----------

print("raw rows        :", all_events.count())
print("distinct rows   :", all_events.drop("_source_file").distinct().count())
print("distinct ids    :", all_events.select("event_id").distinct().count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Level 2: by key → `dropDuplicates(["event_id"])`
# MAGIC Gets the count right, but **which** copy survives is **not defined**. It can change between runs, so
# MAGIC `received_at` (and your lateness metrics) can change between reruns of the same data. Fine for a quick
# MAGIC look, not for production.

# COMMAND ----------

display(all_events.dropDuplicates(["event_id"]).filter("event_id IN ('E005', 'E012')"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Level 3: by key, **deterministically** → `row_number()` with an explicit tie-breaker
# MAGIC Decide the rule with the business. Here: *the first time we received it* (lowest `received_at`,
# MAGIC then lowest `attempt`). In SQL, `QUALIFY` is a neat shortcut.

# COMMAND ----------

def dedup_events(df):
    w = Window.partitionBy("event_id").orderBy("received_at", "attempt")
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

display(dedup_events(all_events).orderBy("event_id"))

# COMMAND ----------

all_events.createOrReplaceTempView("all_events")
display(spark.sql("""
    SELECT *
    FROM all_events
    QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY received_at, attempt) = 1
    ORDER BY event_id
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. Dedup within a batch is not enough: make the **load idempotent**
# MAGIC
# MAGIC Files arrive one by one. `E005` attempt 2 is in a **different file** than attempt 1, so per-file dedup can't see
# MAGIC it. And jobs get **re-run** (failures, backfills, someone clicking "Run" twice).
# MAGIC
# MAGIC * `append` → every rerun and every cross-file retry creates duplicates
# MAGIC * **insert-only `MERGE`** on the key → "insert if I've never seen this event_id". Running it 10 times = running it once.

# COMMAND ----------

silver_ddl = """
    event_id STRING NOT NULL, parcel_id STRING, van_id STRING, status STRING,
    event_time TIMESTAMP, received_at TIMESTAMP, attempt INT, _source_file STRING,
    lateness_minutes DOUBLE, is_late BOOLEAN
"""
for t in ["scan_events_append", "scan_events_merge"]:
    spark.sql(f"CREATE OR REPLACE TABLE {t} ({silver_ddl})")

LATE_AFTER_MINUTES = 30

def prepare(df):
    return (
        dedup_events(df)
        .withColumn("lateness_minutes", (F.unix_timestamp("received_at") - F.unix_timestamp("event_time")) / 60)
        .withColumn("is_late", F.col("lateness_minutes") > LATE_AFTER_MINUTES)
    )

def load_append(df):
    prepare(df).write.mode("append").saveAsTable("scan_events_append")

def load_merge(df, table="scan_events_merge"):
    session = df.sparkSession
    prepare(df).createOrReplaceTempView("incoming_events")
    session.sql(f"""
        MERGE INTO {table} t
        USING incoming_events s
        ON t.event_id = s.event_id
        WHEN NOT MATCHED THEN INSERT *
    """)

# load every file, then "accidentally" re-run file 02
for f in FILES + ["scan_events_02.json"]:
    load_append(read_files(f))
    load_merge(read_files(f))

display(spark.sql("""
    SELECT 'append' AS strategy, count(*) AS rows, count(DISTINCT event_id) AS unique_events FROM scan_events_append
    UNION ALL
    SELECT 'merge',             count(*),         count(DISTINCT event_id)                  FROM scan_events_merge
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC 💡 Insert-only MERGE only has to look at the key, and Delta skips files whose `event_id` range can't match.
# MAGIC On big tables, add a **time bound** so MERGE doesn't scan years of history, e.g.
# MAGIC `ON t.event_id = s.event_id AND t.event_time >= current_date() - INTERVAL 7 DAYS`
# MAGIC (only safe if retries never arrive more than 7 days late), and cluster the table by `event_time`.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. Streaming: watermarks, and what they do to late data
# MAGIC
# MAGIC A streaming dedup or aggregation must **remember** what it has seen (state). Without a limit, that state grows
# MAGIC forever. A **watermark** is the limit:
# MAGIC
# MAGIC > `withWatermark("event_time", "30 minutes")`: "I accept events up to 30 minutes older than the newest
# MAGIC > `event_time` I've seen. Anything older is **too late** and may be **dropped**."
# MAGIC
# MAGIC Our watermark after each file (`max event_time seen − 30 min`). Spark applies the watermark from the
# MAGIC **previous** micro-batch to the current one:
# MAGIC
# MAGIC | After file | max `event_time` | watermark | Late event in the *next* file | Result |
# MAGIC |---|---|---|---|---|
# MAGIC | 01 | 10:14 | 09:44 | | |
# MAGIC | 02 | 10:28 | 09:58 | E013 @ 10:06 (file 03) | ✅ 10:06 ≥ 09:58 → kept |
# MAGIC | 03 | 10:44 | 10:14 | E017 @ 10:08 (file 04) | ❌ 10:08 < 10:14 → **dropped** |
# MAGIC | | | | E018 @ 10:20 (file 04) | ✅ 10:20 ≥ 10:14 → kept |
# MAGIC | 04 | 11:48 | 11:18 | | |
# MAGIC
# MAGIC We simulate files arriving one at a time and run two streams after each one:
# MAGIC * **silver**: `dropDuplicatesWithinWatermark(["event_id"])`
# MAGIC * **gold**: 15-minute tumbling-window counts per van, `outputMode("append")`

# COMMAND ----------

landing = f"{base_path}/landing"
ckpt = f"{base_path}/_checkpoints"
for p in [landing, ckpt]:
    dbutils.fs.rm(p, True)
for t in ["scan_events_stream", "van_activity_watermark"]:
    spark.sql(f"DROP TABLE IF EXISTS {t}")

WATERMARK = "30 minutes"

def run_watermark_streams():
    # silver: streaming dedup (state is kept only within the watermark)
    (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(event_schema)
        .load(landing)
        .withColumn("_source_file", F.col("_metadata.file_name"))
        .withWatermark("event_time", WATERMARK)
        .dropDuplicatesWithinWatermark(["event_id"])
        .writeStream
        .option("checkpointLocation", f"{ckpt}/silver")
        .trigger(availableNow=True)
        .toTable("scan_events_stream")
        .awaitTermination()
    )
    # gold: windowed aggregation on top of silver
    (
        spark.readStream.table("scan_events_stream")
        .withWatermark("event_time", WATERMARK)
        .groupBy(F.window("event_time", "15 minutes").alias("w"), "van_id")
        .agg(F.count("*").alias("scans"))
        .select(F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"), "van_id", "scans")
        .writeStream
        .outputMode("append")                  # a window is written ONCE, when the watermark passes its end
        .option("checkpointLocation", f"{ckpt}/gold")
        .trigger(availableNow=True)
        .toTable("van_activity_watermark")
        .awaitTermination()
    )

def show_stream_state(after_file):
    print(f"===== after {after_file} =====")
    print("silver rows:", spark.table("scan_events_stream").count())
    display(spark.table("van_activity_watermark").orderBy("window_start", "van_id"))

# COMMAND ----------

for f in FILES:
    dbutils.fs.cp(f"{data_path}/{f}", f"{landing}/{f}")
    run_watermark_streams()
    show_stream_state(f)

# COMMAND ----------

# MAGIC %md
# MAGIC ### What happened?
# MAGIC * Duplicates `E003`, `E005`, `E012` were removed by the streaming dedup.
# MAGIC * **`E017` is missing from silver**: it arrived after the watermark had passed it, so it was dropped, with **no
# MAGIC   error and no log**.
# MAGIC * Gold windows appear **only after the watermark passes the window end**. Gold stayed **empty until file 04**,
# MAGIC   and the 11:30 / 11:45 windows are still "open" in state. They will be written when newer data pushes the
# MAGIC   watermark forward. That's the price of `append` mode: rows are final but delayed.
# MAGIC
# MAGIC 👉 Watermarks are a **trade-off between latency/state size and completeness**. A longer watermark keeps more
# MAGIC late data but holds more state and delays results. Pick it from the **real lateness distribution** (section 5),
# MAGIC not a guess.

# COMMAND ----------

display(spark.table("scan_events_stream").filter("van_id = 'VAN-3'"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. Correct by design: idempotent silver + **recompute only the affected windows**
# MAGIC
# MAGIC When late data must **never** be lost (billing, SLAs, driver pay), don't let a watermark decide what to drop:
# MAGIC
# MAGIC 1. **Silver** = insert-only MERGE on `event_id`. No watermark, nothing dropped, reruns are safe.
# MAGIC 2. For each micro-batch, find the **15-minute windows the new rows belong to** (late rows → old windows).
# MAGIC 3. **Recompute** just those windows from silver and **MERGE** them into gold (overwrite the old numbers).
# MAGIC
# MAGIC Late data simply *corrects* gold. The cost of each batch is proportional to the windows it touches, not
# MAGIC to the size of the table.

# COMMAND ----------

spark.sql(f"CREATE OR REPLACE TABLE scan_events_silver ({silver_ddl})")
spark.sql("""
    CREATE OR REPLACE TABLE van_activity_15m (
        window_start TIMESTAMP, van_id STRING, scans BIGINT, parcels BIGINT, refreshed_at TIMESTAMP
    )
""")

# floor event_time to its 15-minute window start (900 seconds)
WINDOW_START = "timestamp_seconds(floor(unix_timestamp(event_time) / 900) * 900)"

def process_batch(batch_df, batch_id):
    session = batch_df.sparkSession
    load_merge(batch_df, table="scan_events_silver")                     # step 1

    batch_df.selectExpr(f"{WINDOW_START} AS window_start").distinct() \
        .createOrReplaceTempView("affected_windows")                     # step 2

    session.sql(f"""
        MERGE INTO van_activity_15m t
        USING (
            SELECT {WINDOW_START} AS window_start, van_id,
                   count(*) AS scans, count(DISTINCT parcel_id) AS parcels,
                   current_timestamp() AS refreshed_at
            FROM scan_events_silver
            WHERE {WINDOW_START} IN (SELECT window_start FROM affected_windows)
            GROUP BY 1, 2
        ) s
        ON t.window_start = s.window_start AND t.van_id = s.van_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)                                                                 # step 3

recompute_ckpt = f"{ckpt}/recompute"

def run_recompute_stream():
    (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.maxFilesPerTrigger", 1)
        .schema(event_schema)
        .load(landing)
        .withColumn("_source_file", F.col("_metadata.file_name"))
        .writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", recompute_ckpt)
        .trigger(availableNow=True)
        .start()
        .awaitTermination()
    )

run_recompute_stream()   # all 4 files are already in the landing folder
display(spark.table("van_activity_15m").orderBy("window_start", "van_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Compare the two gold tables
# MAGIC The 10:00 window of **VAN-3** is the interesting row: E013 (10:06) is in both; **E017 (10:08) is only in the
# MAGIC recompute version**. Windows with `scans_watermark = NULL` are still open in the watermark stream.

# COMMAND ----------

display(spark.sql("""
    SELECT r.window_start, r.van_id,
           r.scans  AS scans_recompute,
           w.scans  AS scans_watermark,
           r.scans != coalesce(w.scans, -1) AS differs
    FROM van_activity_15m r
    LEFT JOIN van_activity_watermark w
      ON r.window_start = w.window_start AND r.van_id = w.van_id
    ORDER BY r.window_start, r.van_id
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reprocessing / backfill is safe
# MAGIC Delete the checkpoint and replay **everything** (what you'd do after fixing a bug). Because every step is a
# MAGIC MERGE on a key, the result is identical: no duplicates and no double counting.

# COMMAND ----------

before = spark.sql("SELECT sum(scans) AS s, count(*) AS c FROM van_activity_15m").first()
dbutils.fs.rm(recompute_ckpt, True)
run_recompute_stream()
after = spark.sql("SELECT sum(scans) AS s, count(*) AS c FROM van_activity_15m").first()
print("before replay:", before, "\nafter replay :", after)
print("silver rows  :", spark.table("scan_events_silver").count(), "(20 unique events)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### When you *must* append inside `foreachBatch`: `txnAppId` + `txnVersion`
# MAGIC If a micro-batch fails *after* writing, Spark re-runs it with the **same `batch_id`**. A plain `append`
# MAGIC would write it twice. Delta's idempotent writes skip a write whose `(txnAppId, txnVersion)` it has already
# MAGIC committed. Use `batch_id` as the version.

# COMMAND ----------

spark.sql("CREATE OR REPLACE TABLE scan_audit_log (event_id STRING, batch_id BIGINT)")

def append_idempotently(df, batch_id):
    (
        df.select("event_id", F.lit(batch_id).alias("batch_id"))
        .write.format("delta")
        .option("txnAppId", "scan_audit_writer")    # a stable, unique name per writer / stream
        .option("txnVersion", batch_id)             # must increase with each batch
        .mode("append")
        .saveAsTable("scan_audit_log")
    )

sample = read_files("scan_events_01.json")
append_idempotently(sample, batch_id=1)
append_idempotently(sample, batch_id=1)   # simulated retry of the same batch, skipped by Delta
print("rows:", spark.table("scan_audit_log").count(), "(7 rows from file 01, not 14)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5. Measure lateness: pick the watermark from data, not a guess

# COMMAND ----------

display(spark.sql(f"""
    SELECT van_id,
           count(*)                                     AS events,
           round(percentile(lateness_minutes, 0.5), 1)  AS p50_minutes,
           round(percentile(lateness_minutes, 0.99), 1) AS p99_minutes,
           round(max(lateness_minutes), 1)              AS max_minutes,
           count_if(is_late)                            AS late_events
    FROM scan_events_silver
    GROUP BY van_id ORDER BY van_id
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC Late events stay visible in silver (`is_late = true`), so you can report them and alert when a van goes quiet.
# MAGIC
# MAGIC 💡 `E013` is flagged `is_late` (received 32 minutes after the scan), yet the watermark **kept** it in section 3.
# MAGIC No contradiction: `is_late` compares `received_at` with `event_time` (processing time), while the watermark only
# MAGIC compares **event times with each other** (newest `event_time` seen − 30 min).

# COMMAND ----------

display(spark.table("scan_events_silver").filter("is_late").orderBy(F.desc("lateness_minutes")))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 6. Cheat sheet 📝
# MAGIC
# MAGIC | Problem | Tool |
# MAGIC |---|---|
# MAGIC | Exact duplicates | `distinct()` / `dropDuplicates()` |
# MAGIC | Duplicates by key, pick one **deterministically** | `row_number() OVER (PARTITION BY key ORDER BY ...)`, `QUALIFY` |
# MAGIC | Duplicates across files / reruns | insert-only `MERGE ... WHEN NOT MATCHED THEN INSERT` |
# MAGIC | Streaming dedup with bounded state | `withWatermark` + `dropDuplicatesWithinWatermark` |
# MAGIC | Streaming windows | `withWatermark` + `window()` + `outputMode("append")` |
# MAGIC | Late data must never be lost | idempotent silver + recompute affected windows with `MERGE` |
# MAGIC | Retried `foreachBatch` appends | `txnAppId` + `txnVersion = batch_id` |
# MAGIC | Backfill / bug fix | delete checkpoint & replay: safe only if every write is idempotent |
# MAGIC
# MAGIC **Golden rules**
# MAGIC 1. Every event needs a **unique ID from the source**. Without one, dedup is guesswork.
# MAGIC 2. Business logic runs on **event time**; lateness is measured with **processing time**.
# MAGIC 3. A watermark **drops data silently**. Use it only where approximate is acceptable, or keep a correct path next to it.
# MAGIC 4. Design every write to be **idempotent**, so reruns and backfills are boring.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleanup (optional)

# COMMAND ----------

# for t in ["scan_events_append", "scan_events_merge", "scan_events_stream", "van_activity_watermark",
#           "scan_events_silver", "van_activity_15m", "scan_audit_log"]:
#     spark.sql(f"DROP TABLE IF EXISTS {t}")
# dbutils.fs.rm(base_path, True)
