from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr


# SCD Type 2 target:
# preserves historical customer profile versions
dp.create_streaming_table(
    name="dev_customer360.silver.customer_history",
    comment=(
        "Historical customer profile maintained using Lakeflow AUTO CDC "
        "with SCD Type 2 semantics."
    )
)
dp.create_auto_cdc_flow(
    flow_name="customer_history_cdc_flow",
    target="dev_customer360.silver.customer_history",
    source="bronze_customer_cdc",
    keys=["customer_unique_id"],
    sequence_by=col("_sequence_number"),
    apply_as_deletes=expr("_operation = 'DELETE'"),
    except_column_list=[
        "_operation",
        "_sequence_number",
    ],
    stored_as_scd_type=2
)

dp.create_auto_cdc_flow(
    flow_name="customer_history_simulated_cdc_flow",
    target="dev_customer360.silver.customer_history",
    source="bronze_customer_cdc_simulated",
    keys=["customer_unique_id"],

    # Historical flow uses INT sequencing
    sequence_by=col("_sequence_number").cast("int"),

    apply_as_deletes=expr("_operation = 'DELETE'"),
    except_column_list=[
        "_operation",
        "_sequence_number",
    ],
    stored_as_scd_type=2
)