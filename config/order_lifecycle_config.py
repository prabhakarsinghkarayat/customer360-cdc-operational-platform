from dataclasses import dataclass


@dataclass(frozen=True)
class OrderLifecycleEvent:
    event_type: str
    timestamp_column: str
    resulting_status: str
    lifecycle_rank: int


ORDER_LIFECYCLE_EVENTS = (
    OrderLifecycleEvent(
        event_type="PURCHASED",
        timestamp_column="order_purchase_timestamp",
        resulting_status="purchased",
        lifecycle_rank=1,
    ),
    OrderLifecycleEvent(
        event_type="APPROVED",
        timestamp_column="order_approved_at",
        resulting_status="approved",
        lifecycle_rank=2,
    ),
    OrderLifecycleEvent(
        event_type="SHIPPED",
        timestamp_column="order_delivered_carrier_date",
        resulting_status="shipped",
        lifecycle_rank=3,
    ),
    OrderLifecycleEvent(
        event_type="DELIVERED",
        timestamp_column="order_delivered_customer_date",
        resulting_status="delivered",
        lifecycle_rank=4,
    ),
)