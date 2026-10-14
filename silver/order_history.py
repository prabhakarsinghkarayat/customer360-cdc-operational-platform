from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr


dp.create_streaming_table(
    name="dev_customer360.silver.order_history",
    comment=(
        "Historical order lifecycle states maintained using Lakeflow "
        "AUTO CDC with SCD Type 2 semantics."
    )
)


dp.create_auto_cdc_flow(
    flow_name="order_history_cdc_flow",
    target="dev_customer360.silver.order_history",
    source="bronze_order_cdc",
    keys=["order_id"],
    sequence_by=col("_sequence_number"),
    apply_as_deletes=expr("_operation = 'DELETE'"),
    except_column_list=[
        "_operation",
        "_sequence_number",
    ],
    stored_as_scd_type=2
)