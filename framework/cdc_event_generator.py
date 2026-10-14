from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col,
    concat_ws,
    lag,
    lit,
    row_number,
    sha2,
    when
)
from config.cdc_config import CDCConfig


class CDCEventGenerator:
    """
    Generates deterministic INSERT / UPDATE CDC events
    from ordered source observations.
    """
    def generate(self, df: DataFrame, config: CDCConfig) -> DataFrame:
        ordering_columns = [col(config.event_timestamp_column)] + [col(column) for column in config.tie_breaker_columns]

        window = (Window.partitionBy(*config.business_key).orderBy(*ordering_columns))

        result = df.withColumn("_sequence_number", row_number().over(window))

        # Previous value of every tracked attribute
        for column in config.tracked_columns:
            result = result.withColumn(f"_previous_{column}", lag(col(column)).over(window))

        # Determine whether any tracked attribute changed
        change_conditions = [~col(column).eqNullSafe(col(f"_previous_{column}")) for column in config.tracked_columns]

        any_change = change_conditions[0]

        for condition in change_conditions[1:]:
            any_change = any_change | condition

        result = result\
            .withColumn("_operation", when(col("_sequence_number") == 1, lit("INSERT"))
            .when(any_change, lit("UPDATE")).otherwise(lit("NO_CHANGE")))

        result = result.withColumn("_event_timestamp", col(config.event_timestamp_column))

        result = result.withColumn(
            "_change_id",
            sha2(
                concat_ws(
                    "||",
                    *[
                        col(column).cast("string")
                        for column in (
                            list(config.business_key)
                            + ["_sequence_number"]
                        )
                    ]
                ),
                256
            )
        )

        # NO_CHANGE observations are not CDC events.
        result = result.filter(col("_operation") != "NO_CHANGE")

        # Remove temporary comparison columns.
        result = result.drop(
            *[
                f"_previous_{column}"
                for column in config.tracked_columns
            ]
        )

        return result