from pyspark import pipelines as dp
from config.quality_rules import QUALITY_RULE_REGISTRY
from framework.quality_validator import QualityValidator


validator = QualityValidator()


def validated_review_records():
    """
    Reads Bronze reviews and applies configured DQ rules.
    """

    bronze_df = spark.readStream.table("dev_customer360.bronze.bronze_reviews")

    return validator.apply_rules(bronze_df, QUALITY_RULE_REGISTRY["reviews"])


@dp.table(
    name="dev_customer360.silver.validated_reviews",
    comment=(
        "Review records that passed blocking data-quality "
        "rules and are eligible for downstream processing."
    )
)
def validated_reviews():
    validated_df = validated_review_records()
    return validator.get_valid_records(validated_df)


@dp.table(
    name="dev_customer360.ops.quarantine_reviews",
    comment=(
        "Review records that failed one or more "
        "QUARANTINE-level data-quality rules."
    )
)
def quarantine_reviews():
    validated_df = validated_review_records()
    return validator.get_quarantined_records(validated_df)