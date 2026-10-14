
from pyspark.sql import functions as F
from pyspark.sql.window import Window


class OrderItemsDeduplicator:
    """
    Reusable duplicate detection and routing framework
    for Olist Order Items.

    Business key:
        (order_id, order_item_id)

    Rules:
        1. Reject records failing row-level DQ.
        2. Accept unique valid records.
        3. Retain one copy of exact duplicates.
        4. Quarantine conflicting business-key versions.
        5. Select duplicate survivors deterministically.
    """

    BUSINESS_KEY = [
        "order_id",
        "order_item_id"
    ]

    BUSINESS_COLUMNS = [
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "shipping_limit_date",
        "price",
        "freight_value"
    ]

    @staticmethod
    def classify(df):
        """
        Classify business keys as:
            UNIQUE
            EXACT_DUPLICATE
            CONFLICTING_DUPLICATE

        Comparison uses business columns rather than
        _record_hash because synthetic transformations
        may not recalculate the original hash.
        """

        keys = OrderItemsDeduplicator.BUSINESS_KEY
        columns = OrderItemsDeduplicator.BUSINESS_COLUMNS

        total_counts = (
            df.groupBy(*keys)
              .agg(
                  F.count("*").alias("total_records")
              )
        )

        version_counts = (
            df.select(*columns)
              .distinct()
              .groupBy(*keys)
              .agg(
                  F.count("*").alias("distinct_versions")
              )
        )

        return (
            total_counts
            .join(version_counts, on=keys, how="inner")
            .withColumn(
                "duplicate_status",
                F.when(
                    F.col("distinct_versions") > 1,
                    F.lit("CONFLICTING_DUPLICATE")
                )
                .when(
                    F.col("total_records") > 1,
                    F.lit("EXACT_DUPLICATE")
                )
                .otherwise(
                    F.lit("UNIQUE")
                )
            )
        )

    @staticmethod
    def route(df):
        """
        Route records after row-level validation.

        Input:
            DataFrame containing _dq_errors array.

        Returns:
            accepted_df:
                Valid unique records and one survivor
                per exact-duplicate business version.

            quarantine_df:
                Invalid records and conflicting duplicates.

        Note:
            The input must already have been processed
            by OrderItemsValidator.validate().
        """

        keys = OrderItemsDeduplicator.BUSINESS_KEY
        columns = OrderItemsDeduplicator.BUSINESS_COLUMNS

        # -----------------------------------------
        # STEP 1: Row-level validation precedence
        # -----------------------------------------

        invalid_rows = df.filter(
            F.size(F.col("_dq_errors")) > 0
        )

        valid_rows = df.filter(
            F.size(F.col("_dq_errors")) == 0
        )

        # -----------------------------------------
        # STEP 2: Classify valid business keys
        # -----------------------------------------

        classification = (
            OrderItemsDeduplicator.classify(valid_rows)
        )

        classified = valid_rows.join(
            classification.select(
                *keys,
                "duplicate_status"
            ),
            on=keys,
            how="left"
        )

        # -----------------------------------------
        # STEP 3: Quarantine conflicting versions
        # -----------------------------------------

        conflicting = (
            classified
            .filter(
                F.col("duplicate_status")
                == "CONFLICTING_DUPLICATE"
            )
            .withColumn(
                "_dq_errors",
                F.array_union(
                    F.col("_dq_errors"),
                    F.array(
                        F.lit("CONFLICTING_DUPLICATE")
                    )
                )
            )
        )

        # -----------------------------------------
        # STEP 4: Deterministic survivor selection
        # -----------------------------------------

        # Partition by the complete business record.
        # Earlier ingestion wins; source metadata
        # provides additional tie-breaking fields.

        survivor_window = (
            Window
            .partitionBy(*columns)
            .orderBy(
                F.col("_ingestion_timestamp")
                 .asc_nulls_last(),

                F.col("_source_file_path")
                 .asc_nulls_last(),

                F.col("_source_file_name")
                 .asc_nulls_last(),

                F.col("_record_hash")
                 .asc_nulls_last()
            )
        )

        accepted = (
            classified
            .filter(
                F.col("duplicate_status")
                != "CONFLICTING_DUPLICATE"
            )
            .withColumn(
                "_survivor_rank",
                F.row_number().over(survivor_window)
            )
            .filter(
                F.col("_survivor_rank") == 1
            )
            .drop("_survivor_rank")
        )

        # -----------------------------------------
        # STEP 5: Combine quarantine records
        # -----------------------------------------

        quarantine = invalid_rows.unionByName(
            conflicting,
            allowMissingColumns=True
        )

        return accepted, quarantine
