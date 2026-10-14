from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    lit,
    row_number,
    sha2,
    concat_ws,
    when
)
from pyspark.sql.window import Window
from config.order_lifecycle_config import ORDER_LIFECYCLE_EVENTS


class OrderLifecycleEventGenerator:

    def generate(self, orders_df: DataFrame) -> DataFrame:

        event_dfs = []

        # Convert each lifecycle timestamp into an event row
        for event in ORDER_LIFECYCLE_EVENTS:
            event_df = (
                orders_df
                # Only create an event when we actually know its timestamp
                .filter(
                    col(event.timestamp_column).isNotNull()
                    & (
                        (lit(event.event_type) != lit("SHIPPED"))
                        | (
                            col("order_delivered_carrier_date")
                            >= col("order_purchase_timestamp")
                        )
                    )
                )
                .select(
                    "order_id",
                    "customer_id",
                    # State of the order AFTER this event
                    lit(event.resulting_status).alias("order_status"),
                    "order_purchase_timestamp",
                    "order_approved_at",
                    "order_delivered_carrier_date",
                    "order_delivered_customer_date",
                    "order_estimated_delivery_date",
                    lit(event.event_type).alias("_event_type"),
                    lit(event.lifecycle_rank).alias("_lifecycle_rank"),
                    col(event.timestamp_column).alias(
                        "_event_timestamp"
                    ),
                )
            )

            event_dfs.append(event_df)

        # Combine PURCHASED / APPROVED / SHIPPED / DELIVERED
        events_df = event_dfs[0]

        for event_df in event_dfs[1:]:
            events_df = events_df.unionByName(event_df)

        # Deterministic lifecycle ordering
        sequence_window = (
            Window
            .partitionBy("order_id")
            .orderBy(
                col("_event_timestamp"),
                col("_lifecycle_rank")
            )
        )

        events_df = (
            events_df
            .withColumn("_sequence_number", row_number().over(sequence_window))
            .withColumn("_operation", lit("UPDATE"))
            # First lifecycle event creates the order
            .withColumn("_operation", when(col("_sequence_number") == 1, lit("INSERT")).otherwise(lit("UPDATE")))
            .withColumn(
                "_change_id",
                sha2(
                    concat_ws(
                        "||",
                        col("order_id"),
                        col("_sequence_number").cast("string"),
                        col("_event_type")
                    ),
                    256
                )
            )
        )

        return events_df