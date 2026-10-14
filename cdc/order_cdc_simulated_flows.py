from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr


# ============================================================
# SIMULATED ORDER CDC -> SCD TYPE 1
# ============================================================

dp.create_auto_cdc_flow(
    flow_name="order_current_simulated_cdc_flow",
    target="dev_customer360.silver.order_current",
    source="bronze_order_cdc_simulated",
    keys=["order_id"],
    sequence_by=col("_sequence_number").cast("int"),
    apply_as_deletes=expr("_operation = 'DELETE'"),
    except_column_list=[
        "_operation",
        "_sequence_number",
        "_rescued_data",
        "_source_system",
        "_source_entity",
        "_source_file_path",
        "_source_file_name",
        "_source_file_size",
        "_source_file_modification_time",
        "_ingestion_timestamp",
        "_ingestion_date",
    ],
    stored_as_scd_type=1
)


# ============================================================
# SIMULATED ORDER CDC -> SCD TYPE 2
# ============================================================

dp.create_auto_cdc_flow(
    flow_name="order_history_simulated_cdc_flow",
    target="dev_customer360.silver.order_history",
    source="bronze_order_cdc_simulated",
    keys=["order_id"],
    sequence_by=col("_sequence_number").cast("int"),
    apply_as_deletes=expr("_operation = 'DELETE'"),
    except_column_list=[
        "_operation",
        "_sequence_number",
        "_rescued_data",
        "_source_system",
        "_source_entity",
        "_source_file_path",
        "_source_file_name",
        "_source_file_size",
        "_source_file_modification_time",
        "_ingestion_timestamp",
        "_ingestion_date",
    ],
    stored_as_scd_type=2
)