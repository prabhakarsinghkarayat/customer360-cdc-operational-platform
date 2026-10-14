from pyspark import pipelines as dp
from config.quality_rules import QUALITY_RULE_REGISTRY
from framework.quality_validator import QualityValidator


validator = QualityValidator()


def validated_product_records():
    """
    Reads Bronze products and applies configured DQ rules.
    """
    bronze_df = spark.readStream.table(
        "dev_customer360.bronze.bronze_products"
    )
    return validator.apply_rules(bronze_df, QUALITY_RULE_REGISTRY["products"])


@dp.table(
    name="dev_customer360.silver.validated_products",
    comment=(
        "Product records that passed blocking data-quality "
        "rules and are eligible for downstream processing."
    )
)
def validated_products():
    validated_df = validated_product_records()
    return validator.get_valid_records(validated_df)


@dp.table(
    name="dev_customer360.ops.quarantine_products",
    comment=(
        "Product records that failed one or more "
        "QUARANTINE-level data-quality rules."
    )
)
def quarantine_products():
    validated_df = validated_product_records()
    return validator.get_quarantined_records(validated_df)