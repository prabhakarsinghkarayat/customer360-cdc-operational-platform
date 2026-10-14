from dataclasses import dataclass
from pyspark.sql.types import StructType
from config.schemas import CUSTOMER_CDC_SIMULATOR_SCHEMA
from config.schemas import ORDER_CDC_SIMULATOR_SCHEMA

import os

@dataclass(frozen=True)
class IngestionConfig:
    """
    Defines the source contract and ingestion configuration for a single source entity
    """
    entity_name: str
    source_system: str
    source_path: str
    target_table: str
    schema: StructType
    business_columns: tuple[str, ...]
    file_format: str = "csv"
    # Optional source-specific reader settings
    reader_options: tuple[tuple[str, str], ...] = ()


from config.schemas import SCHEMA_REGISTRY

CATALOG = os.getenv("CUSTOMER360_CATALOG", "dev_customer360")
LANDING_PATH = f"/Volumes/{CATALOG}/bronze/landing"
BASE_PATH = f"{LANDING_PATH}/olist"

CUSTOMERS_CONFIG = IngestionConfig(
    entity_name="customers",
    source_system="olist",
    source_path=f"{BASE_PATH}/customers",
    target_table="bronze_customers",
    schema=SCHEMA_REGISTRY["customers"],
    business_columns=(
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state"
    )
)


ORDERS_CONFIG = IngestionConfig(
    entity_name="orders",
    source_system="olist",
    source_path=f"{BASE_PATH}/orders",
    target_table="bronze_orders",
    schema=SCHEMA_REGISTRY["orders"],
    business_columns=(
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    )
)


ORDER_ITEMS_CONFIG = IngestionConfig(
    entity_name="order_items",
    source_system="olist",
    source_path=f"{BASE_PATH}/order_items",
    target_table="bronze_order_items",
    schema=SCHEMA_REGISTRY["order_items"],
    business_columns=(
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "shipping_limit_date",
        "price",
        "freight_value"
    )
)


PAYMENTS_CONFIG = IngestionConfig(
    entity_name="payments",
    source_system="olist",
    source_path=f"{BASE_PATH}/payments",
    target_table="bronze_payments",
    schema=SCHEMA_REGISTRY["payments"],
    business_columns=(
        "order_id",
        "payment_sequential",
        "payment_type",
        "payment_installments",
        "payment_value"
    )
)


REVIEWS_CONFIG = IngestionConfig(
    entity_name="reviews",
    source_system="olist",
    source_path=f"{BASE_PATH}/reviews",
    target_table="bronze_reviews",
    schema=SCHEMA_REGISTRY["reviews"],
    business_columns=(
        "review_id",
        "order_id",
        "review_score",
        "review_comment_title",
        "review_comment_message",
        "review_creation_date",
        "review_answer_timestamp"
    ),
    reader_options=(
        ("quote", '"'),
        ("escape", '"'),
        ("multiLine", "true")
    )
)


PRODUCTS_CONFIG = IngestionConfig(
    entity_name="products",
    source_system="olist",
    source_path=f"{BASE_PATH}/products",
    target_table="bronze_products",
    schema=SCHEMA_REGISTRY["products"],
    business_columns=(
        "product_id",
        "product_category_name",
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm"
    )
)


SELLERS_CONFIG = IngestionConfig(
    entity_name="sellers",
    source_system="olist",
    source_path=f"{BASE_PATH}/sellers",
    target_table="bronze_sellers",
    schema=SCHEMA_REGISTRY["sellers"],
    business_columns=(
        "seller_id",
        "seller_zip_code_prefix",
        "seller_city",
        "seller_state"
    )
)


GEOLOCATION_CONFIG = IngestionConfig(
    entity_name="geolocation",
    source_system="olist",
    source_path=f"{BASE_PATH}/geolocation",
    target_table="bronze_geolocation",
    schema=SCHEMA_REGISTRY["geolocation"],
    business_columns=(
        "geolocation_zip_code_prefix",
        "geolocation_lat",
        "geolocation_lng",
        "geolocation_city",
        "geolocation_state"
    )
)


CATEGORY_TRANSLATION_CONFIG = IngestionConfig(
    entity_name="category_translation",
    source_system="olist",
    source_path=f"{BASE_PATH}/category_translation",
    target_table="bronze_category_translation",
    schema=SCHEMA_REGISTRY["category_translation"],
    business_columns=(
        "product_category_name",
        "product_category_name_english"
    )
)


INGESTION_CONFIGS = {
    "customers": CUSTOMERS_CONFIG,
    "orders": ORDERS_CONFIG,
    "order_items": ORDER_ITEMS_CONFIG,
    "payments": PAYMENTS_CONFIG,
    "reviews": REVIEWS_CONFIG,
    "products": PRODUCTS_CONFIG,
    "sellers": SELLERS_CONFIG,
    "geolocation": GEOLOCATION_CONFIG,
    "category_translation": CATEGORY_TRANSLATION_CONFIG
}



CUSTOMER_CDC_SIMULATOR_CONFIG = IngestionConfig(
    entity_name="customer_cdc_simulator",
    source_system="cdc_simulator",
    source_path=f"{LANDING_PATH}/cdc_simulator/customers/",
    target_table="bronze_customer_cdc_simulated",
    schema=CUSTOMER_CDC_SIMULATOR_SCHEMA,
    business_columns=(
        "customer_unique_id",
        "customer_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
        "_operation",
        "_event_timestamp",
        "_sequence_number",
        "_change_id",
    ),
)

# ============================================================
# ORDER CDC SIMULATOR
# ============================================================

ORDER_CDC_SIMULATOR_CONFIG = IngestionConfig(
    entity_name="order_cdc_simulated",
    source_system="olist_cdc_simulator",
    source_path=f"{LANDING_PATH}/cdc_simulator/orders",
    target_table="bronze_order_cdc_simulated",
    schema=ORDER_CDC_SIMULATOR_SCHEMA,

    business_columns=(
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
        "_event_type",
        "_lifecycle_rank",
        "_event_timestamp",
        "_sequence_number",
        "_operation",
        "_change_id",
    ),

    file_format="csv",

    reader_options=(
        ("header", "true"),
    ),
)