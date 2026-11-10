# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality: Quarantine Bad Records Instead of Failing the Pipeline
# MAGIC
# MAGIC **Scenario (Banking):** the card processor sends a daily file of card transactions. Finance needs the good
# MAGIC transactions **on time**, Risk needs suspicious ones **flagged**, and nobody wants a 3 AM page because *one*
# MAGIC row had a negative amount.
# MAGIC
# MAGIC The three possible reactions to a bad record:
# MAGIC
# MAGIC | Severity | Action | Example | Lakeflow equivalent |
# MAGIC |---|---|---|---|
# MAGIC | **warn** | keep the row, flag it, count it | amount above the credit limit | `@dp.expect` |
# MAGIC | **drop** | move the row to a **quarantine** table, keep loading the rest | negative amount, unknown account | `@dp.expect_or_drop` + quarantine table |
# MAGIC | **fail** | stop the whole pipeline | a broken file / missing metadata | `@dp.expect_or_fail` |
# MAGIC
# MAGIC This notebook builds that framework in **plain Spark**, so you can run it cell by cell and see every step.
# MAGIC The same rules as a **Lakeflow Declarative Pipeline** are in `../pipeline/transactions_dq_pipeline.py`, and
# MAGIC `02_inspect_pipeline_results` shows how to read the expectation metrics afterwards.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Setup

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "dq_demo", "Schema")
dbutils.widgets.text("volume", "raw", "Volume")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")
spark.sql(f"USE {catalog}.{schema}")

base_path = f"/Volumes/{catalog}/{schema}/{volume}/dq_demo"
data_path = f"{base_path}/sample_data"
print(f"Sample data path: {data_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Copy the sample files into the volume
# MAGIC `sample_data/` has three sub-folders: `transactions/` (day 1), `accounts/` (reference data) and `later/` (day 2,
# MAGIC used to show incremental runs). Manual alternative:
# MAGIC
# MAGIC `databricks fs cp -r ./de_production_patterns/02_data_quality_expectations/sample_data dbfs:/Volumes/workspace/dq_demo/raw/dq_demo/sample_data`

# COMMAND ----------

import os
import shutil

repo_sample_dir = os.path.abspath(os.path.join(os.getcwd(), "..", "sample_data"))

if os.path.isdir(repo_sample_dir):
    shutil.copytree(repo_sample_dir, data_path, dirs_exist_ok=True)
    print("copied sample_data/")
else:
    print(f"{repo_sample_dir} not found, so upload the files manually.")

for sub in ["transactions", "accounts", "later"]:
    display(dbutils.fs.ls(f"{data_path}/{sub}"))

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# start clean so the notebook can be re-run
for t in ["fw_transactions_silver", "fw_transactions_quarantine", "fw_dq_metrics", "fw_dq_staging"]:
    spark.sql(f"DROP TABLE IF EXISTS {t}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Look at the raw data first
# MAGIC Can you spot the problems before the rules do?

# COMMAND ----------

txn_schema = """
    txn_id STRING, account_id STRING, amount DECIMAL(18,2), currency STRING, txn_type STRING,
    merchant_category STRING, txn_ts TIMESTAMP, country STRING, channel STRING
"""

def read_transactions(path):
    return (
        spark.read.schema(txn_schema).json(path)
        .withColumn("_source_file", F.col("_metadata.file_name"))
        .withColumn("_ingested_at", F.current_timestamp())
    )

accounts = (
    spark.read.option("header", True)
    .schema("account_id STRING, customer_name STRING, account_status STRING, credit_limit DECIMAL(18,2), home_country STRING")
    .csv(f"{data_path}/accounts/")
)

day1 = read_transactions(f"{data_path}/transactions/")
display(day1)
display(accounts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Step 1: **standardise** before you validate
# MAGIC Don't reject what you can safely fix. `"usd"` and `" USD "` are clearly `USD`. But only fix things that are
# MAGIC **unambiguous**: `"US$"` *might* be USD, but guessing with money is how audits fail, so that one is rejected.

# COMMAND ----------

def standardise(df):
    return (
        df
        .withColumn("currency", F.upper(F.trim("currency")))
        .withColumn("txn_type", F.upper(F.trim("txn_type")))
        .withColumn("country", F.upper(F.trim("country")))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Step 2: **enrich** with what the rules need
# MAGIC Some rules need context that is not in the row itself:
# MAGIC * **referential integrity**: does the account exist? → join to `accounts`
# MAGIC * **duplicates within the file**: `row_number()` per `txn_id`
# MAGIC * **duplicates across files**: was this `txn_id` already loaded on a previous day?

# COMMAND ----------

def enrich(df):
    dup_window = Window.partitionBy("txn_id").orderBy("_ingested_at", "_source_file")
    df = (
        df.join(accounts.select("account_id", "account_status", "credit_limit"), "account_id", "left")
        .withColumn("_dup_rank", F.row_number().over(dup_window))
    )
    if spark.catalog.tableExists("fw_transactions_silver"):
        loaded = spark.table("fw_transactions_silver").select("txn_id", F.lit(True).alias("_already_loaded"))
        df = df.join(loaded, "txn_id", "left")
    else:
        df = df.withColumn("_already_loaded", F.lit(False))
    return df.withColumn("_already_loaded", F.coalesce("_already_loaded", F.lit(False)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Step 3: rules as **data**, not as scattered `if` statements
# MAGIC
# MAGIC Each rule is `(name, SQL condition that is TRUE for a GOOD row, severity)`.
# MAGIC Keeping rules in one list means you can review them with the business, version them in Git, or even load
# MAGIC them from a table.
# MAGIC
# MAGIC ⚠️ **NULL trap:** `amount > 0` is **NULL** (not false) when `amount` is NULL. Our engine treats NULL as a
# MAGIC failure, but always add explicit `IS NOT NULL` rules too, so the failure reason is clear.

# COMMAND ----------

RULES = [
    # --- drop: the row is wrong, quarantine it ---
    ("txn_id_not_null",       "txn_id IS NOT NULL",                                       "drop"),
    ("amount_not_null",       "amount IS NOT NULL",                                       "drop"),
    ("amount_positive",       "amount > 0",                                               "drop"),
    ("valid_currency",        "currency IN ('USD','EUR','GBP','LKR','INR')",              "drop"),
    ("valid_txn_type",        "txn_type IN ('DEBIT','CREDIT','REFUND')",                  "drop"),
    ("txn_ts_not_in_future",  "txn_ts <= current_timestamp()",                            "drop"),
    ("known_account",         "account_status IS NOT NULL",                               "drop"),
    ("not_duplicate_in_file", "_dup_rank = 1 OR txn_id IS NULL",                          "drop"),
    ("not_already_loaded",    "NOT _already_loaded",                                      "drop"),
    # --- warn: the row is valid but interesting, keep it and flag it ---
    # (`X IS NULL OR ...` so a warning doesn't repeat what a drop rule already reports)
    ("merchant_category_set", "merchant_category IS NOT NULL",                            "warn"),
    ("within_credit_limit",   "amount IS NULL OR credit_limit IS NULL OR amount <= credit_limit", "warn"),
    ("account_active",        "account_status IS NULL OR account_status = 'ACTIVE'",      "warn"),
    # --- fail: something is wrong with the whole load, stop ---
    ("has_source_file",       "_source_file IS NOT NULL",                                 "fail"),
]

display(spark.createDataFrame(RULES, "rule STRING, condition STRING, severity STRING"))

# COMMAND ----------

def apply_rules(df, rules):
    """Add `_failed_rules` (array of rule names this row failed) plus the per-severity views of it."""
    checks = [
        # condition TRUE -> NULL (passed); condition FALSE or NULL -> the rule name (failed)
        F.when(F.coalesce(F.expr(cond), F.lit(False)), F.lit(None)).otherwise(F.lit(name))
        for name, cond, _ in rules
    ]
    names = lambda sev: F.array(*[F.lit(n) for n, _, s in rules if s == sev])
    return (
        df.withColumn("_failed_rules", F.array_compact(F.array(*checks)))
        .withColumn("_errors",   F.array_intersect("_failed_rules", names("drop")))
        .withColumn("_warnings", F.array_intersect("_failed_rules", names("warn")))
        .withColumn("_fatal",    F.array_intersect("_failed_rules", names("fail")))
    )

checked = apply_rules(enrich(standardise(day1)), RULES)
display(checked.select("txn_id", "account_id", "amount", "currency", "txn_type", "_errors", "_warnings", "_fatal"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Step 4: **dataset-level** checks, the circuit breaker
# MAGIC
# MAGIC Row rules can't see the big picture. If **40% of today's file is bad**, the problem is probably the *source*
# MAGIC (a bug in their export), not 40% bad transactions. Loading the "good" 60% would publish wrong totals.
# MAGIC
# MAGIC So before writing anything:
# MAGIC * any `fail` rule broken → stop
# MAGIC * quarantine rate above a threshold → stop
# MAGIC * file empty → stop
# MAGIC
# MAGIC Checking **before** writing means a stopped run leaves silver untouched, so there's no half-loaded day to clean up.

# COMMAND ----------

class DataQualityError(Exception):
    pass

def circuit_breaker(checked, max_bad_ratio):
    stats = checked.agg(
        F.count("*").alias("total"),
        F.count_if(F.size("_errors") > 0).alias("bad"),
        F.count_if(F.size("_fatal") > 0).alias("fatal"),
    ).first()
    bad_ratio = stats.bad / stats.total if stats.total else 1.0
    print(f"rows={stats.total}  quarantined={stats.bad}  ({bad_ratio:.0%})  fatal={stats.fatal}")

    if stats.total == 0:
        raise DataQualityError("empty load")
    if stats.fatal > 0:
        raise DataQualityError(f"{stats.fatal} rows broke a FAIL rule")
    if bad_ratio > max_bad_ratio:
        raise DataQualityError(f"{bad_ratio:.0%} of rows are bad (limit {max_bad_ratio:.0%}). Is the source broken?")
    return stats

# A strict limit trips the breaker on day 1...
try:
    circuit_breaker(checked, max_bad_ratio=0.25)
except DataQualityError as e:
    print("🛑 STOPPED:", e)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Step 5: split and write: **silver**, **quarantine**, **metrics**

# COMMAND ----------

import uuid

def write_outputs(checked, run_id):
    meta_cols = ["_dup_rank", "_already_loaded", "account_status", "credit_limit", "_failed_rules", "_fatal"]

    good = checked.filter(F.size("_errors") == 0).drop("_errors", *meta_cols)
    bad = (
        checked.filter(F.size("_errors") > 0)
        .drop(*meta_cols)
        .withColumn("_dq_run_id", F.lit(run_id))
        .withColumn("_quarantined_at", F.current_timestamp())
    )
    good.write.mode("append").saveAsTable("fw_transactions_silver")
    bad.write.mode("append").saveAsTable("fw_transactions_quarantine")

    # one row per rule per run: this table is what you put on a DQ dashboard
    total = checked.count()
    counts = checked.agg(*[
        F.count_if(F.array_contains("_failed_rules", name)).alias(name) for name, _, _ in RULES
    ]).first().asDict()
    metrics = spark.createDataFrame(
        [(run_id, name, sev, total, counts[name]) for name, _, sev in RULES],
        "run_id STRING, rule STRING, severity STRING, total_rows BIGINT, failed_rows BIGINT",
    ).withColumn("pass_rate", F.round(1 - F.col("failed_rows") / F.col("total_rows"), 4)) \
     .withColumn("run_at", F.current_timestamp())
    metrics.write.mode("append").saveAsTable("fw_dq_metrics")

def run_dq(path, max_bad_ratio=0.5):
    run_id = str(uuid.uuid4())[:8]
    checked = apply_rules(enrich(standardise(read_transactions(path))), RULES)

    # ⚠️ Spark is LAZY: `checked` is a recipe, not a result. It is re-computed for every action below.
    # `enrich()` reads fw_transactions_silver, so once silver is written, re-computing `checked` would mark
    # the rows we JUST loaded as `not_already_loaded` failures (and `current_timestamp()` would change too).
    # Fix: snapshot the validated batch once, then use the snapshot for the breaker, writes and metrics.
    checked.write.mode("overwrite").option("overwriteSchema", True).saveAsTable("fw_dq_staging")
    checked = spark.table("fw_dq_staging")

    circuit_breaker(checked, max_bad_ratio)
    write_outputs(checked, run_id)
    print(f"✅ run {run_id} written")
    return run_id

# COMMAND ----------

# Day 1 with a realistic limit
run_dq(f"{data_path}/transactions/", max_bad_ratio=0.5)

# COMMAND ----------

display(spark.table("fw_transactions_silver").orderBy("txn_id"))

# COMMAND ----------

# Quarantine keeps the FULL row plus WHY it was rejected. Ops can fix and replay these.
display(spark.table("fw_transactions_quarantine").select("txn_id", "account_id", "amount", "currency", "txn_type", "txn_ts", "_errors", "_source_file"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Day 2 arrives
# MAGIC * `"usd"` is fixed by standardisation, so it is **not** rejected
# MAGIC * `amount = 0.00` → quarantined
# MAGIC * account `A-1006` is CLOSED → loaded with a **warning**

# COMMAND ----------

run_dq(f"{data_path}/later/", max_bad_ratio=0.5)

# Run day 1 again by mistake: every row is caught by `not_already_loaded`, so silver stays correct
try:
    run_dq(f"{data_path}/transactions/", max_bad_ratio=0.5)
except DataQualityError as e:
    print("🛑 STOPPED:", e)

# COMMAND ----------

# MAGIC %md
# MAGIC ### The metrics table: trend of each rule over runs

# COMMAND ----------

display(spark.sql("""
    SELECT run_id, run_at, rule, severity, failed_rows, total_rows, pass_rate
    FROM fw_dq_metrics
    WHERE failed_rows > 0
    ORDER BY run_at, severity, rule
"""))

# COMMAND ----------

# Warnings travel with the row, so downstream teams can use them (e.g. Risk reviews over-limit transactions)
display(spark.sql("""
    SELECT txn_id, account_id, amount, credit_limit_warning
    FROM (SELECT *, array_contains(_warnings, 'within_credit_limit') AS credit_limit_warning FROM fw_transactions_silver)
    WHERE size(_warnings) > 0
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Last line of defence: Delta **constraints**
# MAGIC
# MAGIC Rules in code can be bypassed (someone writes to the table from another notebook). **Table constraints** are
# MAGIC enforced by Delta itself on *every* write. A write that violates them fails **as a whole**.
# MAGIC Use constraints for the few things that must *never* be wrong; use rules + quarantine for everything else.

# COMMAND ----------

spark.sql("ALTER TABLE fw_transactions_silver ALTER COLUMN txn_id SET NOT NULL")
spark.sql("ALTER TABLE fw_transactions_silver ADD CONSTRAINT amount_positive CHECK (amount > 0)")
spark.sql("ALTER TABLE fw_transactions_silver ADD CONSTRAINT valid_currency CHECK (currency IN ('USD','EUR','GBP','LKR','INR'))")

display(spark.sql("SHOW TBLPROPERTIES fw_transactions_silver").filter("key LIKE 'delta.constraints%'"))

# COMMAND ----------

try:
    spark.sql("""
        INSERT INTO fw_transactions_silver (txn_id, account_id, amount, currency)
        VALUES ('T-BAD', 'A-1001', -1.00, 'LKR')
    """)
except Exception as e:
    print("🛑 Delta rejected the write:", str(e)[:300])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Replaying fixed quarantine rows
# MAGIC Quarantine is not a graveyard. A typical flow: ops (or the source team) fixes the data, then you replay it
# MAGIC through **the same rules**. Here we pretend the source confirmed `US$` means `USD`.

# COMMAND ----------

fixed = (
    spark.table("fw_transactions_quarantine")
    .filter(F.array_contains("_errors", "valid_currency") & (F.size("_errors") == 1))
    .withColumn("currency", F.when(F.col("currency") == "US$", "USD").otherwise(F.col("currency")))
    .select(*day1.columns)            # back to the original input columns
)
replayed = apply_rules(enrich(standardise(fixed)), RULES)
display(replayed.select("txn_id", "currency", "_errors", "_warnings"))
# In production: write the passing rows to silver and mark them as replayed in the quarantine table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Cheat sheet 📝
# MAGIC
# MAGIC | Concern | Pattern |
# MAGIC |---|---|
# MAGIC | Fixable mess (case, spaces) | **standardise** before validating |
# MAGIC | Row is wrong | **drop** → quarantine table with the reason(s) |
# MAGIC | Row is valid but suspicious | **warn** → keep + `_warnings` column |
# MAGIC | Whole load is wrong | **fail** rule or **circuit breaker** (bad-row ratio, empty file) |
# MAGIC | Referential integrity | left join to the reference table + `IS NOT NULL` rule |
# MAGIC | Duplicates | `row_number()` within the batch + check against the target |
# MAGIC | Must never be violated | Delta `NOT NULL` / `CHECK` constraints |
# MAGIC | Observability | one metrics row per rule per run → dashboard + alerts |
# MAGIC | Rules that read the target table | **snapshot** the checked batch before writing (lazy evaluation trap) |
# MAGIC
# MAGIC **Golden rules**
# MAGIC 1. Never silently drop. Every rejected row lives in quarantine **with its reason**.
# MAGIC 2. Decide *before* writing, so a stopped run leaves no half-loaded data.
# MAGIC 3. Rules are data: one reviewed list, not `if` statements spread across notebooks.
# MAGIC 4. Measure: a pass rate that drops from 99% to 80% is an incident even if nothing failed.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleanup (optional)

# COMMAND ----------

# for t in ["fw_transactions_silver", "fw_transactions_quarantine", "fw_dq_metrics", "fw_dq_staging"]:
#     spark.sql(f"DROP TABLE IF EXISTS {t}")
