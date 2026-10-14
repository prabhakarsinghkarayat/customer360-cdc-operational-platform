from pyspark import pipelines as dp
from framework.order_lifecycle_event_generator import (
    OrderLifecycleEventGenerator
)


@dp.table(
    name="bronze_order_cdc",
    comment=(
        "Order lifecycle CDC events reconstructed from validated "
        "Olist order timestamps. Only timestamp-supported lifecycle "
        "events are generated."
    )
)
def bronze_order_cdc():

    orders_df = spark.read.table(
        "dev_customer360.silver.validated_orders"
    )

    generator = OrderLifecycleEventGenerator()

    return generator.generate(orders_df)