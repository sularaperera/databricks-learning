"""
Lakeflow Declarative Pipeline: SCD Type 1 and Type 2 with AUTO CDC (formerly APPLY CHANGES).

    customers_cdc_feed     temporary view   Auto Loader over the CDC JSON files
    customers_scd1_auto    streaming table  latest state per customer (SCD1)
    customers_scd2_auto    streaming table  full history per customer (SCD2)

Compare the result with the hand-written MERGE in ../notebooks/01_scd_with_merge.py.
Everything that needed custom code there (dedup, ordering, late data, deletes) is handled by
`sequence_by` and `apply_as_deletes` here.

Older code: `import dlt` + `dlt.apply_changes(...)`. Same arguments.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

SOURCE = spark.conf.get("scd_demo.source_path")


@dp.temporary_view(comment="CDC records from the CRM, as delivered.")
def customers_cdc_feed():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema("customer_id STRING, full_name STRING, email STRING, city STRING, tier STRING, "
                "op STRING, change_seq BIGINT, event_ts TIMESTAMP")
        .load(SOURCE)
    )


# ---------------------------------------------------------------- SCD Type 1
dp.create_streaming_table("customers_scd1_auto", comment="Current state of each customer (SCD1).")

dp.create_auto_cdc_flow(
    target="customers_scd1_auto",
    source="customers_cdc_feed",
    keys=["customer_id"],
    sequence_by=F.col("change_seq"),                 # ordering comes from the SOURCE, not arrival time
    apply_as_deletes=F.expr("op = 'DELETE'"),
    except_column_list=["op"],                       # CDC metadata we don't want in the dimension
    stored_as_scd_type=1,
)


# ---------------------------------------------------------------- SCD Type 2
dp.create_streaming_table("customers_scd2_auto", comment="Full change history of each customer (SCD2).")

dp.create_auto_cdc_flow(
    target="customers_scd2_auto",
    source="customers_cdc_feed",
    keys=["customer_id"],
    sequence_by=F.col("change_seq"),
    apply_as_deletes=F.expr("op = 'DELETE'"),
    except_column_list=["op"],
    stored_as_scd_type=2,                            # adds __START_AT / __END_AT
    # Only these columns create a new version. A change to any OTHER column (e.g. an email typo fix)
    # updates the current version in place, a "type 1 attribute inside a type 2 table".
    # Remove this line to track history on every column, like the MERGE notebook does.
    track_history_column_list=["city", "tier"],
)
