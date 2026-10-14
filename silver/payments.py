
from pyspark import pipelines as dp
from pyspark.sql import functions as F

from framework.payments_validator import PaymentsValidator


BRONZE_TABLE = "dev_customer360.bronze.bronze_payments"


def validated_payments():
    """
    Read Bronze and apply payment-specific
    data-quality validation rules.
    """
    bronze_df = spark.read.table(BRONZE_TABLE)

    return PaymentsValidator.validate(bronze_df)


@dp.materialized_view(
    name="dev_customer360.silver.payments",
    comment="Validated Olist payments with data-quality warnings."
)
def silver_payments():

    return (
        validated_payments()
        .filter(F.size(F.col("_dq_errors")) == 0)
        .select(
            "order_id",
            "payment_sequential",
            F.lower(F.trim(F.col("payment_type"))).alias(
                "payment_type"
            ),
            "payment_installments",
            F.col("payment_value")
                .cast("decimal(18,2)")
                .alias("payment_value"),
            "_dq_warnings",
            "_record_hash",
            "_source_system",
            "_source_file_path",
            "_ingestion_timestamp"
        )
    )


@dp.materialized_view(
    name="dev_customer360.silver.payments_quarantine",
    comment="Rejected Olist payments with explicit DQ reasons."
)
def silver_payments_quarantine():

    return (
        validated_payments()
        .filter(F.size(F.col("_dq_errors")) > 0)
    )
