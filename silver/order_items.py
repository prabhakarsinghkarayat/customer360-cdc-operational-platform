
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from framework.order_items_validator import OrderItemsValidator
from framework.order_items_deduplicator import OrderItemsDeduplicator


BRONZE_TABLE = "dev_customer360.bronze.bronze_order_items"

BUSINESS_COLUMNS = OrderItemsDeduplicator.BUSINESS_COLUMNS


def validated_order_items():
    """
    Read Bronze and apply shared row-level
    data-quality validation rules.
    """
    bronze_df = spark.read.table(BRONZE_TABLE)

    return OrderItemsValidator.validate(bronze_df)


def routed_order_items():
    """
    Apply validation-first duplicate routing.

    Returns:
        accepted_df
        quarantine_df
    """
    validated_df = validated_order_items()

    return OrderItemsDeduplicator.route(validated_df)


def suppressed_exact_duplicates():
    """
    Identify exact duplicate records that were
    intentionally excluded from Silver.

    Row-level invalid records are excluded first.
    Conflicting business keys are excluded because
    those records belong in quarantine.
    """
    validated_df = validated_order_items()

    valid_rows = validated_df.filter(
        F.size(F.col("_dq_errors")) == 0
    )

    classification = OrderItemsDeduplicator.classify(valid_rows)

    exact_rows = (
        valid_rows.join(
            classification.select(
                *OrderItemsDeduplicator.BUSINESS_KEY,
                "duplicate_status"
            ),
            on=OrderItemsDeduplicator.BUSINESS_KEY,
            how="inner"
        )
        .filter(
            F.col("duplicate_status") == "EXACT_DUPLICATE"
        )
    )

    survivor_window = (
        Window
        .partitionBy(*BUSINESS_COLUMNS)
        .orderBy(
            F.col("_ingestion_timestamp").asc_nulls_last(),
            F.col("_source_file_path").asc_nulls_last(),
            F.col("_source_file_name").asc_nulls_last(),
            F.col("_record_hash").asc_nulls_last()
        )
    )

    return (
        exact_rows
        .withColumn(
            "_duplicate_rank",
            F.row_number().over(survivor_window)
        )
        .filter(F.col("_duplicate_rank") > 1)
        .drop("_duplicate_rank")
    )


@dp.materialized_view(
    name="dev_customer360.silver.order_items",
    comment=(
        "Validated, deduplicated and standardized "
        "Olist order items."
    )
)
def silver_order_items():

    accepted_df, _ = routed_order_items()

    return (
        accepted_df
        .select(
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "shipping_limit_date",
            F.col("price").cast("decimal(18,2)").alias("price"),
            F.col("freight_value").cast("decimal(18,2)").alias(
                "freight_value"
            ),
            "_record_hash",
            "_source_system",
            "_source_file_path",
            "_ingestion_timestamp"
        )
    )


@dp.materialized_view(
    name="dev_customer360.silver.order_items_quarantine",
    comment=(
        "Rejected order items with row-level DQ "
        "errors or conflicting business versions."
    )
)
def silver_order_items_quarantine():

    _, quarantine_df = routed_order_items()

    return quarantine_df


@dp.materialized_view(
    name="dev_customer360.silver.order_items_duplicates_audit",
    comment=(
        "Exact duplicate order-item records suppressed "
        "during deterministic deduplication."
    )
)
def silver_order_items_duplicates_audit():

    return suppressed_exact_duplicates()
