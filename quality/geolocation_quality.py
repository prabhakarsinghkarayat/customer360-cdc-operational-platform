from pyspark import pipelines as dp
from config.quality_rules import QUALITY_RULE_REGISTRY
from framework.quality_validator import QualityValidator


validator = QualityValidator()


def validated_geolocation_records():
    """
    Reads Bronze geolocation observations and applies configured DQ rules.
    """

    bronze_df = spark.readStream.table(
        "dev_customer360.bronze.bronze_geolocation"
    )

    return validator.apply_rules(bronze_df, QUALITY_RULE_REGISTRY["geolocation"])


@dp.table(
    name="dev_customer360.silver.validated_geolocation",
    comment=(
        "Geolocation observations that passed blocking "
        "data-quality rules and are eligible for conformance."
    )
)
def validated_geolocation():
    validated_df = validated_geolocation_records()
    return validator.get_valid_records(validated_df)


@dp.table(
    name="dev_customer360.ops.quarantine_geolocation",
    comment=(
        "Geolocation observations that failed one or more "
        "QUARANTINE-level data-quality rules."
    )
)
def quarantine_geolocation():
    validated_df = validated_geolocation_records()
    return validator.get_quarantined_records(validated_df)