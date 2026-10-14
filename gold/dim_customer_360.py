from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dp.materialized_view(
    name="dev_customer360.gold.dim_customer_360",
    comment="Latest available profile per unique customer"
)
def dim_customer_360():
    customers = spark.read.table(
        "dev_customer360.silver.customer_current"
    )

    window = (
        Window.partitionBy("customer_unique_id")
        .orderBy(
            F.col("order_purchase_timestamp").desc_nulls_last(),
            F.col("_event_timestamp").desc_nulls_last(),
            F.col("customer_id").asc_nulls_last()
        )
    )

    return (
        customers
        .filter(
            F.col("customer_unique_id").isNotNull()
            & (F.trim("customer_unique_id") != "")
        )
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .select(
            "customer_unique_id",
            F.col("customer_id").alias("latest_customer_id"),
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
            F.col("order_purchase_timestamp").alias(
                "latest_profile_order_timestamp"
            )
        )
    )