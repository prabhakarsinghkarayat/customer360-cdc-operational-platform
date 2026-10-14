from pyspark import pipelines as dp
from config.quality_rules import QUALITY_RULE_REGISTRY
from framework.quality_validator import QualityValidator


validator = QualityValidator()


def validated_order_item_records():
    """
    Reads Bronze order items and applies configured DQ rules.
    """

    bronze_df = spark.readStream.table(
        "dev_customer360.bronze.bronze_order_items"
    )

    return validator.apply_rules(bronze_df, QUALITY_RULE_REGISTRY["order_items"])


@dp.table(
    name="dev_customer360.silver.validated_order_items",
    comment=(
        "Order item records that passed blocking data-quality "
        "rules and are eligible for downstream processing."
    )
)
def validated_order_items():
    validated_df = validated_order_item_records()
    return validator.get_valid_records(validated_df)


@dp.table(
    name="dev_customer360.ops.quarantine_order_items",
    comment=(
        "Order item records that failed one or more "
        "QUARANTINE-level data-quality rules."
    )
)
def quarantine_order_items():
    validated_df = validated_order_item_records()
    return validator.get_quarantined_records(validated_df)