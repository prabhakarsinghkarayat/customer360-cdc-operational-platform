from pyspark import pipelines as dp


@dp.table(
    name="customer_cdc_deduplicated",
    comment=(
        "Deduplicated incremental customer CDC events. "
        "Ensures one logical event per _change_id before AUTO CDC processing."
    )
)
def customer_cdc_deduplicated():

    return (
        spark.readStream
            .table("dev_customer360.bronze.bronze_customer_cdc_simulated")
            .dropDuplicates(["_change_id"])
    )