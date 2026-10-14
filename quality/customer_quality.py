from pyspark import pipelines as dp
from config.quality_rules import QUALITY_RULE_REGISTRY
from framework.quality_validator import QualityValidator


validator = QualityValidator()


def validated_customer_records():
    """
    Reads Bronze customers and applies configured DQ Rules.
    """

    bronze_df = spark.readStream.table("dev_customer360.bronze.bronze_customers")

    return validator.apply_rules(bronze_df, QUALITY_RULE_REGISTRY["customers"])


@dp.table(
    name = "dev_customer360.silver.validated_customers",
    comment=(
        "Customer records that passed configured data-quality "
        "rules and are eligible for downstream CDC processing."
    )
)
def validated_customers():
    validated_df = validated_customer_records()
    return validator.get_valid_records(validated_df)



@dp.table(
    name = "dev_customer360.ops.quarantine_customers",
    comment=(
        "Customer records that failed one or more configured "
        "data-quality rules and require investigation."
    )
)
def quarantine_customers():
    validated_df = validated_customer_records()
    return validator.get_quarantined_records(validated_df)