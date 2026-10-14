from pyspark import pipelines as dp
from config.quality_rules import QUALITY_RULE_REGISTRY
from framework.quality_validator import QualityValidator

validator = QualityValidator()

def validated_order_records():
    """
    Reads Bronze orders and applies configured DQ rules.
    """

    bronze_df = spark.readStream.table("dev_customer360.bronze.bronze_orders")

    return validator.apply_rules(bronze_df, QUALITY_RULE_REGISTRY["orders"])


@dp.table(
    name = "dev_customer360.silver.validated_orders",
    comment = (
        "Order records that passed blocking data-quality rules "
        "and are eligible for downstream CDC processing. "
        "Warning-level records are retained."
    )
)
def validated_orders():
    validated_df = validated_order_records()
    return validator.get_valid_records(validated_df)


@dp.table(
    name = "dev_customer360.ops.quarantine_orders",
    comment = (
        "Order records that failed one or more "
        "QUARANTINE-level data-quality rules."
    )
)
def quarantine_orders():
    validated_df = validated_order_records()
    return validator.get_quarantined_records(validated_df)