 from dataclasses import dataclass


@dataclass(frozen=True)
class CDCConfig:
    """
    Defines the CDC contract for an entity.
    """
    entity_name: str
    business_key: tuple[str, ...]
    tracked_columns: tuple[str, ...]
    event_timestamp_column: str
    tie_breaker_columns: tuple[str, ...]


CUSTOMER_CDC_CONFIG = CDCConfig(
    entity_name="customer",
    business_key=("customer_unique_id",),
    tracked_columns=(
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
    ),
    event_timestamp_column="order_purchase_timestamp",
    tie_breaker_columns=(
        "customer_id",
    ),
)