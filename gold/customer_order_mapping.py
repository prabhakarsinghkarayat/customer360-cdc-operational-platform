
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dp.materialized_view(
    name="dev_customer360.gold.customer_order_mapping",
    comment=(
        "Complete customer_id to customer_unique_id mapping "
        "from validated Bronze customer records."
    )
)
def customer_order_mapping():

    customers = (
        spark.read.table(
            "dev_customer360.bronze.bronze_customers"
        )
        .select(
            "customer_id",
            "customer_unique_id",
            "_ingestion_timestamp"
        )
        .filter(
            F.col("customer_id").isNotNull()
            & F.col("customer_unique_id").isNotNull()
            & (F.trim("customer_id") != "")
            & (F.trim("customer_unique_id") != "")
        )
    )

    # Keep one deterministic record per customer_id.
    # This preserves all order-associated customer IDs.
    window = (
        Window.partitionBy("customer_id")
        .orderBy(
            F.col("_ingestion_timestamp").desc_nulls_last(),
            F.col("customer_unique_id").asc()
        )
    )

    return (
        customers
        .withColumn(
            "_row_number",
            F.row_number().over(window)
        )
        .filter(F.col("_row_number") == 1)
        .select(
            "customer_id",
            "customer_unique_id"
        )
    )
