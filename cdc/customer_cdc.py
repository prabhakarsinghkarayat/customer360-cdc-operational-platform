from pyspark import pipelines as dp
from config.cdc_config import CUSTOMER_CDC_CONFIG
from framework.cdc_event_generator import CDCEventGenerator


@dp.table(
    name="bronze_customer_cdc",
    comment=(
        "Customer CDC event stream reconstructed from validated "
        "customer observations and order purchase chronology."
    )
)
def bronze_customer_cdc():
    customer_df = spark.read.table(
        "dev_customer360.silver.validated_customers"
    )
    orders_df = spark.read.table(
        "dev_customer360.silver.validated_orders"
    )

    customer_observations_df = (
        customer_df.alias("c")
        .join(
            orders_df.alias("o"),
            "customer_id",
            "inner"
        )
        .select(
            "c.customer_unique_id",
            "c.customer_id",
            "c.customer_zip_code_prefix",
            "c.customer_city",
            "c.customer_state",
            "o.order_id",
            "o.order_purchase_timestamp"
        )
    )

    generator = CDCEventGenerator()

    return generator.generate(customer_observations_df, CUSTOMER_CDC_CONFIG)