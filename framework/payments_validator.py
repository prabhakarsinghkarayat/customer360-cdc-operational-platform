
from pyspark.sql import functions as F


class PaymentsValidator:
    """
    Shared row-level validation framework
    for Olist payment records.

    Hard errors -> Quarantine
    Warnings    -> Retain in Silver with warning

    Business key:
        (order_id, payment_sequential)
    """

    @staticmethod
    def validate(df):
        """
        Add two array columns:
            _dq_errors
            _dq_warnings

        Does not filter or modify source records.
        """

        payment_type = F.lower(
            F.trim(F.col("payment_type"))
        )

        errors = F.array_compact(
            F.array(
                F.when(
                    F.col("order_id").isNull()
                    | (F.trim(F.col("order_id")) == ""),
                    F.lit("INVALID_ORDER_ID")
                ),

                F.when(
                    F.col("payment_sequential").isNull()
                    | (F.col("payment_sequential") <= 0),
                    F.lit("INVALID_PAYMENT_SEQUENCE")
                ),

                F.when(
                    F.col("payment_type").isNull()
                    | (F.trim(F.col("payment_type")) == ""),
                    F.lit("INVALID_PAYMENT_TYPE")
                ),

                F.when(
                    F.col("payment_installments").isNull()
                    | (F.col("payment_installments") <= 0),
                    F.lit("INVALID_INSTALLMENTS")
                ),

                F.when(
                    F.col("payment_value").isNull()
                    | F.isnan("payment_value")
                    | (F.col("payment_value") < 0),
                    F.lit("INVALID_PAYMENT_VALUE")
                ),

                F.when(
                    payment_type == "not_defined",
                    F.lit("UNDEFINED_PAYMENT_TYPE")
                ),

                F.when(
                    F.col("_rescued_data").isNotNull(),
                    F.lit("RESCUED_DATA")
                )
            )
        )

        warnings = F.array_compact(
            F.array(
                F.when(
                    (payment_type == "voucher")
                    & (F.col("payment_value") == 0),
                    F.lit("ZERO_VALUE_VOUCHER")
                )
            )
        )

        return (
            df
            .withColumn("_dq_errors", errors)
            .withColumn("_dq_warnings", warnings)
        )
