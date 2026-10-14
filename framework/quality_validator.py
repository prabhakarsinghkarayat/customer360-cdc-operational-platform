from pyspark.sql import DataFrame
from pyspark.sql.functions import array, array_compact, col, expr, lit, when, size, current_timestamp
from config.quality_rules import QualityRule

class QualityValidator:
    """
    Evaulates configured data-quality rules against a DataFrame
    and applies rule actions such as QUARANTINE and WARN.
    """

    def apply_rules(self, df: DataFrame, rules: tuple[QualityRule, ...]) -> DataFrame:

        # -----------------------------------------------------
        # 1. Capture all failed rules
        # -----------------------------------------------------   
        failed_rule_expressions = [when(~expr(rule.condition), lit(rule.rule_name)) for rule in rules]


         # -----------------------------------------------------
        # 2. Capture failed QUARANTINE rules
        # -----------------------------------------------------
        quarantine_rule_expressions = [
            when(
                ~expr(rule.condition), 
                lit(rule.rule_name)
                ) 
            for rule in rules if rule.action.upper() == "QUARANTINE"
        ]


        # -----------------------------------------------------
        # 3. Capture failed WARN rules
        # -----------------------------------------------------
        warning_rule_expressions = [
            when(
                ~expr(rule.condition),
                lit(rule.rule_name)
            )
            for rule in rules if rule.action.upper() == "WARN"
            ]
        
        validated_df = (
            df
            .withColumn("_dq_failed_rules", array_compact(array(*failed_rule_expressions)).cast("array<string>"))
            .withColumn("_dq_quarantine_rules", array_compact(array(*quarantine_rule_expressions)).cast("array<string>"))
            .withColumn("_dq_warning_rules", array_compact(array(*warning_rule_expressions)).cast("array<string>"))
        )


        # -----------------------------------------------------
        # 4. Determine final DQ status
        # -----------------------------------------------------
        validated_df = validated_df.withColumn(
            "_dq_status",
            when(
                size(col("_dq_quarantine_rules")) > 0,
                lit("QUARANTINED")
            )
            .when(
                size(col("_dq_warning_rules")) > 0,
                lit("WARNING")
            )
            .otherwise(
                lit("VALID")
            )
        )

        # -----------------------------------------------------
        # 5. Record DQ evaluation timestamp
        # -----------------------------------------------------
        validated_df = validated_df.withColumn("_dq_checked_at", current_timestamp())

        return validated_df
    

    def get_valid_records(self, df: DataFrame) -> DataFrame:
        """
        Returns records eligible for downstream processing.

        VALID and WARNING records are retained.
        QUARANTINED records are excluded.
        """
        return df.filter(col("_dq_status").isin("VALID", "WARNING"))
    

    def get_quarantined_records(self, df: DataFrame) -> DataFrame:
        """
        Returns records that failed one or more
        QUARANTINE-level DQ rules.
        """
        return df.filter(col("_dq_status") == "QUARANTINED")
