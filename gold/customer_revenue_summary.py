from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dp.materialized_view(
    name="dev_customer360.gold.customer_revenue_summary",
    comment="Customer-level order and payment analytics"
)
def customer_revenue_summary():

    customers = spark.read.table(
        "dev_customer360.silver.customer_current"
    )

    customer_window = (
        Window.partitionBy("customer_id")
        .orderBy(
            F.col("_event_timestamp").desc_nulls_last(),
            F.col("customer_unique_id").asc_nulls_last()
        )
    )

    customer_mapping = (
        customers
        .filter(
            F.col("customer_id").isNotNull()
            & F.col("customer_unique_id").isNotNull()
        )
        .withColumn(
            "_rn",
            F.row_number().over(customer_window)
        )
        .filter(F.col("_rn") == 1)
        .select("customer_id", "customer_unique_id")
    )

    orders = spark.read.table(
        "dev_customer360.gold.fact_orders"
    )

    return (
        orders
        .join(customer_mapping, "customer_id", "inner")
        .groupBy("customer_unique_id")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("total_payment_value").alias(
                "lifetime_payment_value"
            ),
            F.avg("total_payment_value").alias(
                "average_order_payment"
            ),
            F.min("order_purchase_timestamp").alias(
                "first_order_timestamp"
            ),
            F.max("order_purchase_timestamp").alias(
                "last_order_timestamp"
            ),
            F.sum(
                F.when(
                    F.col("order_status") == "delivered", 1
                ).otherwise(0)
            ).alias("delivered_orders"),
            F.sum(
                F.when(
                    F.col("order_status") == "canceled", 1
                ).otherwise(0)
            ).alias("canceled_orders")
        )
    )