from pyspark import pipelines as dp
from config.quality_rules import QUALITY_RULE_REGISTRY
from framework.quality_validator import QualityValidator


validator = QualityValidator()


def validated_payment_records():
    """
    Reads Bronze payments and applies configured DQ rules.
    """

    bronze_df = spark.readStream.table(
        "dev_customer360.bronze.bronze_payments"
    )

    return validator.apply_rules(bronze_df, QUALITY_RULE_REGISTRY["payments"])


@dp.table(
    name="dev_customer360.silver.validated_payments",
    comment=(
        "Payment records that passed blocking data-quality "
        "rules and are eligible for downstream processing."
    )
)
def validated_payments():
    validated_df = validated_payment_records()
    return validator.get_valid_records(validated_df)


@dp.table(
    name="dev_customer360.ops.quarantine_payments",
    comment=(
        "Payment records that failed one or more "
        "QUARANTINE-level data-quality rules."
    )
)
def quarantine_payments():
    validated_df = validated_payment_records()
    return validator.get_quarantined_records(validated_df)