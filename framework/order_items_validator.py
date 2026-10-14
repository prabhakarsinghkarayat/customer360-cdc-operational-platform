from pyspark.sql import functions as F


class OrderItemsValidator:
    """Shared validation rules for order-item records."""

    @staticmethod
    def validate(df):

        return df.withColumn(
            "_dq_errors",
            F.array_compact(
                F.array(
                    F.when(
                        F.col("order_id").isNull(),
                        F.lit("NULL_ORDER_ID")
                    ),
                    F.when(
                        F.col("order_item_id").isNull(),
                        F.lit("NULL_ORDER_ITEM_ID")
                    ),
                    F.when(
                        F.col("product_id").isNull(),
                        F.lit("NULL_PRODUCT_ID")
                    ),
                    F.when(
                        F.col("seller_id").isNull(),
                        F.lit("NULL_SELLER_ID")
                    ),
                    F.when(
                        F.col("shipping_limit_date").isNull(),
                        F.lit("NULL_SHIPPING_LIMIT_DATE")
                    ),
                    F.when(
                        F.col("price").isNull()
                        | (F.col("price") <= 0)
                        | F.isnan("price"),
                        F.lit("INVALID_PRICE")
                    ),
                    F.when(
                        F.col("freight_value").isNull()
                        | (F.col("freight_value") < 0)
                        | F.isnan("freight_value"),
                        F.lit("INVALID_FREIGHT")
                    ),
                    F.when(
                        F.col("_rescued_data").isNotNull(),
                        F.lit("RESCUED_DATA")
                    )
                )
            )
        )