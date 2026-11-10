"""
Lakeflow Declarative Pipeline (formerly DLT): card transactions with data-quality expectations.

    bronze  transactions_bronze      Auto Loader, raw rows exactly as delivered
    ref     accounts                 reference data (materialized view over the CSV)
    view    transactions_enriched    bronze + account lookup + standardisation
    silver  transactions_silver      warn / drop / fail expectations applied
    quar.   transactions_quarantine  every row that broke a DROP rule, with the rule names
    gold    daily_account_spend      aggregates built only from clean data

This is a pipeline source file, not a notebook: you can't "run" it cell by cell.
Deploy it with the bundle (see ../README.md) or attach it to a pipeline in the UI.
Older code uses `import dlt` and `@dlt.table`; `pyspark.pipelines` is the current API with the same ideas.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Set in the pipeline configuration (resources/pipelines.yml)
SOURCE = spark.conf.get("dq_demo.source_path")

# Same rules as the notebook framework. Each value must be TRUE for a good row.
DROP_RULES = {
    "txn_id_not_null": "txn_id IS NOT NULL",
    "amount_not_null": "amount IS NOT NULL",
    "amount_positive": "amount > 0",
    "valid_currency": "currency IN ('USD','EUR','GBP','LKR','INR')",
    "valid_txn_type": "txn_type IN ('DEBIT','CREDIT','REFUND')",
    "txn_ts_not_in_future": "txn_ts <= current_timestamp()",
    "known_account": "account_status IS NOT NULL",
}
WARN_RULES = {
    "merchant_category_set": "merchant_category IS NOT NULL",
    "within_credit_limit": "amount IS NULL OR credit_limit IS NULL OR amount <= credit_limit",
    "account_active": "account_status IS NULL OR account_status = 'ACTIVE'",
}


# ---------------------------------------------------------------- bronze
@dp.table(comment="Raw card transactions exactly as delivered by the processor.")
def transactions_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaHints", "amount DECIMAL(18,2), txn_ts TIMESTAMP")
        .load(f"{SOURCE}/transactions/")
        .withColumn("_source_file", F.col("_metadata.file_name"))
        .withColumn("_ingested_at", F.current_timestamp())
    )


# ---------------------------------------------------------------- reference data
@dp.materialized_view(comment="Account master data used for referential-integrity checks.")
def accounts():
    return (
        spark.read.option("header", True)
        .schema("account_id STRING, customer_name STRING, account_status STRING, "
                "credit_limit DECIMAL(18,2), home_country STRING")
        .csv(f"{SOURCE}/accounts/")
    )


# ---------------------------------------------------------------- enrich + standardise
@dp.temporary_view(comment="Bronze + account lookup + standardised codes. Not persisted.")
def transactions_enriched():
    accts = spark.read.table("accounts").select("account_id", "account_status", "credit_limit")
    return (
        spark.readStream.table("transactions_bronze")
        .withColumn("currency", F.upper(F.trim("currency")))
        .withColumn("txn_type", F.upper(F.trim("txn_type")))
        .join(accts, "account_id", "left")          # stream-static join
    )


# ---------------------------------------------------------------- silver
@dp.table(comment="Validated transactions. Rows that broke a DROP rule are in transactions_quarantine.")
@dp.expect_all(WARN_RULES)                                      # keep, count in metrics
@dp.expect_all_or_drop(DROP_RULES)                              # remove from this table
@dp.expect_or_fail("has_source_file", "_source_file IS NOT NULL")  # stop the update
def transactions_silver():
    return spark.readStream.table("transactions_enriched")


# ---------------------------------------------------------------- quarantine
# Expectations drop rows but don't store them anywhere. The quarantine table applies the
# INVERSE of the drop rules to the same input, so every dropped row is kept with its reasons.
def _failed_rules(rules):
    return F.array_compact(F.array(*[
        F.when(F.coalesce(F.expr(cond), F.lit(False)), F.lit(None)).otherwise(F.lit(name))
        for name, cond in rules.items()
    ]))

@dp.table(comment="Rows rejected by silver DROP rules, with the names of the rules they broke.")
def transactions_quarantine():
    return (
        spark.readStream.table("transactions_enriched")
        .withColumn("_failed_rules", _failed_rules(DROP_RULES))
        .filter(F.size("_failed_rules") > 0)
        .withColumn("_quarantined_at", F.current_timestamp())
    )


# ---------------------------------------------------------------- gold
@dp.materialized_view(comment="Daily spend per account, from clean data only.")
@dp.expect_or_fail("one_row_per_txn", "txn_count = distinct_txn_count")
def daily_account_spend():
    return (
        spark.read.table("transactions_silver")
        .dropDuplicates(["txn_id"])   # the processor sometimes re-sends a row; see demo 04 for more
        .groupBy(F.to_date("txn_ts").alias("txn_date"), "account_id")
        .agg(
            F.count("*").alias("txn_count"),
            F.countDistinct("txn_id").alias("distinct_txn_count"),
            F.sum(F.when(F.col("txn_type") == "DEBIT", F.col("amount")).otherwise(0)).alias("total_debits"),
            F.sum(F.when(F.col("txn_type") != "DEBIT", F.col("amount")).otherwise(0)).alias("total_credits"),
        )
    )
