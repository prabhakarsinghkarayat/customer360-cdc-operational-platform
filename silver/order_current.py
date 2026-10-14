from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr


dp.create_streaming_table(
    name="dev_customer360.silver.order_current",
    comment=(
        "Current order lifecycle state maintained using Lakeflow "
        "AUTO CDC with SCD Type 1 semantics."
    )
)


dp.create_auto_cdc_flow(
    flow_name="order_current_cdc_flow",
    target="dev_customer360.silver.order_current",
    source="bronze_order_cdc",
    keys=["order_id"],
    sequence_by=col("_sequence_number"),
    apply_as_deletes=expr("_operation = 'DELETE'"),
    except_column_list=[
        "_operation",
        "_sequence_number",
    ],
    stored_as_scd_type=1
)