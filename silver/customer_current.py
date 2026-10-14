from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr


# SCD Type 1 target:
# one current row per customer_unique_id
dp.create_streaming_table(
    name="dev_customer360.silver.customer_current",
    comment=(
        "Current customer profile maintained using Lakeflow AUTO CDC "
        "with SCD Type 1 semantics."
    )
)
dp.create_auto_cdc_flow(
    flow_name = "customer_current_cdc_flow",
    target = "dev_customer360.silver.customer_current",
    source = "bronze_customer_cdc",
    keys = ["customer_unique_id"],
    sequence_by = col("_sequence_number"),
    apply_as_deletes = expr("_operation = 'DELETE'"),
    except_column_list = [
        "_operation",
        "_sequence_number",
    ],
    stored_as_scd_type = 1
)

dp.create_auto_cdc_flow(
    flow_name="customer_current_simulated_cdc_flow",
    target="dev_customer360.silver.customer_current",
    source="bronze_customer_cdc_simulated",
    keys=["customer_unique_id"],
    sequence_by=col("_sequence_number").cast("int"),
    apply_as_deletes=expr("_operation = 'DELETE'"),
    except_column_list=[
        "_operation",
        "_sequence_number",
    ],
    stored_as_scd_type=1
)