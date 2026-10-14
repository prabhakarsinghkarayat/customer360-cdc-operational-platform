from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="dev_customer360.gold.customer_rfm",
    comment="Customer RFM segmentation using historical dataset reference date"
)
def customer_rfm():

    summary = spark.read.table(
        "dev_customer360.gold.customer_revenue_summary"
    )

    reference = (
        spark.read.table("dev_customer360.gold.fact_orders")
        .agg(
            F.date_add(
                F.max(F.to_date("order_purchase_timestamp")),
                1
            ).alias("reference_date")
        )
    )

    scored = (
        summary
        .crossJoin(reference)
        .withColumn(
            "recency_days",
            F.datediff(
                F.col("reference_date"),
                F.to_date("last_order_timestamp")
            )
        )
        .withColumn("frequency", F.col("total_orders"))
        .withColumn(
            "monetary_value",
            F.col("lifetime_payment_value")
        )
    )

    return (
        scored
        .withColumn(
            "recency_score",
            F.when(F.col("recency_days") <= 30, 5)
             .when(F.col("recency_days") <= 60, 4)
             .when(F.col("recency_days") <= 90, 3)
             .when(F.col("recency_days") <= 180, 2)
             .otherwise(1)
        )
        .withColumn(
            "frequency_score",
            F.when(F.col("frequency") >= 10, 5)
             .when(F.col("frequency") >= 5, 4)
             .when(F.col("frequency") >= 3, 3)
             .when(F.col("frequency") >= 2, 2)
             .otherwise(1)
        )
        .withColumn(
            "monetary_score",
            F.when(F.col("monetary_value") >= 2000, 5)
             .when(F.col("monetary_value") >= 1000, 4)
             .when(F.col("monetary_value") >= 500, 3)
             .when(F.col("monetary_value") >= 200, 2)
             .otherwise(1)
        )
        .withColumn(
            "rfm_segment",
            F.when(
                (F.col("recency_score") >= 4)
                & (F.col("frequency_score") >= 4),
                "CHAMPIONS"
            )
            .when(
                (F.col("recency_score") >= 4)
                & (F.col("frequency_score") >= 2),
                "LOYAL"
            )
            .when(
                (F.col("recency_score") <= 2)
                & (F.col("frequency_score") >= 3),
                "AT_RISK"
            )
            .when(
                F.col("recency_score") >= 4,
                "RECENT"
            )
            .otherwise("REGULAR")
        )
        .select(
            "customer_unique_id",
            "reference_date",
            "recency_days",
            "frequency",
            "monetary_value",
            "recency_score",
            "frequency_score",
            "monetary_score",
            "rfm_segment"
        )
    )