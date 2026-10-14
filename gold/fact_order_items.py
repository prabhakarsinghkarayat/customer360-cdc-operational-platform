from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="dev_customer360.gold.fact_order_items",
    comment="Order item facts with product and seller attributes"
)
def fact_order_items():

    items = spark.read.table(
        "dev_customer360.silver.order_items"
    ).alias("i")

    products = spark.read.table(
        "dev_customer360.silver.products"
    ).alias("p")

    sellers = spark.read.table(
        "dev_customer360.silver.sellers"
    ).alias("s")

    categories = spark.read.table(
        "dev_customer360.silver.category_translation"
    ).alias("c")

    return (
        items
        .join(
            products,
            F.col("i.product_id") == F.col("p.product_id"),
            "left"
        )
        .join(
            sellers,
            F.col("i.seller_id") == F.col("s.seller_id"),
            "left"
        )
        .join(
            categories,
            F.col("p.product_category_name")
            == F.col("c.product_category_name"),
            "left"
        )
        .select(
            F.col("i.order_id").alias("order_id"),
            F.col("i.order_item_id").alias("order_item_id"),
            F.col("i.product_id").alias("product_id"),
            F.col("i.seller_id").alias("seller_id"),
            F.col("p.product_category_name").alias("product_category"),
            F.col("c.product_category_name_english").alias(
                "product_category_english"
            ),
            F.col("s.seller_city").alias("seller_city"),
            F.col("s.seller_state").alias("seller_state"),
            F.col("i.shipping_limit_date").alias("shipping_limit_date"),
            F.col("i.price").alias("item_price"),
            F.col("i.freight_value").alias("freight_value"),
            (
                F.col("i.price") + F.col("i.freight_value")
            ).alias("item_total_value")
        )
    )