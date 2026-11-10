# Databricks notebook source
# MAGIC %md
# MAGIC # Inspect the Lakeflow pipeline: tables, quarantine and expectation metrics
# MAGIC
# MAGIC Run this **after** the `dq_transactions_pipeline` has completed at least one update
# MAGIC (see the README for how to deploy and run it).
# MAGIC
# MAGIC Lakeflow records how many rows passed and failed **every expectation** in the pipeline **event log**. That's
# MAGIC the built-in version of the `fw_dq_metrics` table we built by hand in notebook 01.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "dq_demo", "Schema")
dbutils.widgets.text("volume", "raw", "Volume")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")
spark.sql(f"USE {catalog}.{schema}")
data_path = f"/Volumes/{catalog}/{schema}/{volume}/dq_demo/sample_data"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. What landed where?

# COMMAND ----------

for t in ["transactions_bronze", "transactions_silver", "transactions_quarantine", "daily_account_spend"]:
    print(f"{t:28s} {spark.table(t).count():>4} rows")

# COMMAND ----------

display(spark.table("transactions_quarantine").select("txn_id", "account_id", "amount", "currency", "txn_type", "_failed_rules"))

# COMMAND ----------

display(spark.table("daily_account_spend").orderBy("txn_date", "account_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Expectation metrics from the event log
# MAGIC
# MAGIC `event_log(TABLE(...))` returns the event log of the pipeline that owns that table.
# MAGIC `flow_progress` events contain `details:flow_progress.data_quality.expectations`, an array of
# MAGIC `{name, dataset, passed_records, failed_records}`.

# COMMAND ----------

expectations = spark.sql(f"""
    WITH progress AS (
        SELECT timestamp, origin.update_id, details
        FROM event_log(TABLE({catalog}.{schema}.transactions_silver))
        WHERE event_type = 'flow_progress'
          AND details:flow_progress.data_quality.expectations IS NOT NULL
    )
    SELECT
        update_id,
        e.dataset,
        e.name                AS expectation,
        SUM(e.passed_records) AS passed,
        SUM(e.failed_records) AS failed,
        ROUND(SUM(e.passed_records) / NULLIF(SUM(e.passed_records) + SUM(e.failed_records), 0), 3) AS pass_rate
    FROM progress
    LATERAL VIEW explode(from_json(
        details:flow_progress.data_quality.expectations,
        'array<struct<name: string, dataset: string, passed_records: bigint, failed_records: bigint>>'
    )) AS e
    GROUP BY update_id, e.dataset, e.name
    ORDER BY failed DESC, expectation
""")
display(expectations)

# COMMAND ----------

# MAGIC %md
# MAGIC 💡 Compare with notebook 01's `fw_dq_metrics`: same rules, same counts, but here Lakeflow collected them for
# MAGIC you. Note that silver has 13 rows: the duplicate `T-0003` is still there (expectations are row-level and
# MAGIC can't see duplicates). The gold view removes it, and its `one_row_per_txn` expectation proves it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Incremental run: land day 2, then run the pipeline again
# MAGIC Run the cell below, then trigger the pipeline again (UI → **Start**, or `databricks bundle run dq_transactions_pipeline`).
# MAGIC Only the **new** file is processed (Auto Loader + streaming tables). Then re-run sections 1 and 2.

# COMMAND ----------

dbutils.fs.cp(f"{data_path}/later/transactions_2026-10-02.json", f"{data_path}/transactions/transactions_2026-10-02.json")
display(dbutils.fs.ls(f"{data_path}/transactions/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Try it yourself: make the pipeline **fail**
# MAGIC In `transactions_dq_pipeline.py`, change the fail expectation to something today's data breaks, e.g.
# MAGIC
# MAGIC ```python
# MAGIC @dp.expect_or_fail("no_huge_transactions", "amount < 5000")
# MAGIC ```
# MAGIC
# MAGIC Run a **full refresh**. The update stops with an `EXPECTATION_VIOLATION` error that names the rule and shows
# MAGIC the offending row, and the silver table is **not** updated. That's the behaviour you want for contract-breaking data.
# MAGIC
# MAGIC Same expectations in **SQL** pipelines:
# MAGIC ```sql
# MAGIC CREATE OR REFRESH STREAMING TABLE transactions_silver (
# MAGIC   CONSTRAINT amount_positive   EXPECT (amount > 0)                ON VIOLATION DROP ROW,
# MAGIC   CONSTRAINT has_source_file   EXPECT (_source_file IS NOT NULL)  ON VIOLATION FAIL UPDATE,
# MAGIC   CONSTRAINT account_active    EXPECT (account_status = 'ACTIVE')            -- warn
# MAGIC ) AS SELECT * FROM STREAM(transactions_enriched);
# MAGIC ```
