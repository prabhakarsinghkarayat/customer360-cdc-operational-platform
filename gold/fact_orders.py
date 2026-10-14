from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="dev_customer360.gold.fact_orders",
    comment="Order facts with separately aggregated payments and items"
)
def fact_orders():

    orders = spark.read.table(
        "dev_customer360.silver.order_current"
    ).alias("o")

    payments = (
        spark.read.table("dev_customer360.silver.payments")
        .groupBy("order_id")
        .agg(
            F.sum("payment_value").alias("total_payment_value"),
            F.count("*").alias("payment_count")
        )
        .alias("p")
    )

    items = (
        spark.read.table("dev_customer360.silver.order_items")
        .groupBy("order_id")
        .agg(
            F.sum("price").alias("items_subtotal"),
            F.sum("freight_value").alias("total_freight"),
            F.count("*").alias("item_count")
        )
        .alias("i")
    )

    return (
        orders
        .join(payments, F.col("o.order_id") == F.col("p.order_id"), "left")
        .join(items, F.col("o.order_id") == F.col("i.order_id"), "left")
        .select(
            F.col("o.order_id").alias("order_id"),
            F.col("o.customer_id").alias("customer_id"),
            F.col("o.order_status").alias("order_status"),
            F.col("o.order_purchase_timestamp").alias(
                "order_purchase_timestamp"
            ),
            F.col("o.order_approved_at").alias("order_approved_at"),
            F.col("o.order_delivered_customer_date").alias(
                "order_delivered_customer_date"
            ),
            F.col("o.order_estimated_delivery_date").alias(
                "order_estimated_delivery_date"
            ),
            F.coalesce(
                F.col("p.total_payment_value"),
                F.lit(0).cast("decimal(18,2)")
            ).alias("total_payment_value"),
            F.coalesce(F.col("p.payment_count"), F.lit(0)).alias(
                "payment_count"
            ),
            F.coalesce(
                F.col("i.items_subtotal"),
                F.lit(0).cast("decimal(18,2)")
            ).alias("items_subtotal"),
            F.coalesce(
                F.col("i.total_freight"),
                F.lit(0).cast("decimal(18,2)")
            ).alias("total_freight"),
            F.coalesce(F.col("i.item_count"), F.lit(0)).alias(
                "item_count"
            )
        )
    )