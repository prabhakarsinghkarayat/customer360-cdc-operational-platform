from dataclasses import dataclass

@dataclass(frozen = True)
class QualityRule:
    """
    Defines a single reusable data-quality rule.
    """

    rule_name: str
    condition: str
    action: str
    description: str

CUSTOMER_QUALITY_RULES = (
    QualityRule(
        rule_name = "CUSTOMER_ID_NOT_NULL",
        condition = "customer_id IS NOT NULL",
        action = "QUARANTINE",
        description = "Customer ID must be present."
    ),

    QualityRule(
        rule_name = "CUSTOMER_UNIQUE_ID_NOT_NULL",
        condition = "customer_unique_id IS NOT NULL",
        action = "QUARANTINE",
        description = "Customer unique ID must be present."
    ),

    QualityRule(
        rule_name="CUSTOMER_ZIP_NOT_NULL",
        condition="customer_zip_code_prefix IS NOT NULL",
        action="QUARANTINE",
        description="Customer ZIP code must be present."
    ),

    QualityRule(
        rule_name="CUSTOMER_CITY_NOT_BLANK",
        condition=(
            "customer_city IS NOT NULL "
            "AND TRIM(customer_city) <> ''"
        ),
        action="QUARANTINE",
        description="Customer city must be present and non-blank."
    ),

    QualityRule(
        rule_name="CUSTOMER_STATE_VALID",
        condition=(
            "customer_state IS NOT NULL "
            "AND customer_state RLIKE '^[A-Z]{2}$'"
        ),
        action="QUARANTINE",
        description="Customer state must be a valid two-character state code."
    ),

    QualityRule(
        rule_name="CUSTOMER_NOT_RESCUED",
        condition="_rescued_data IS NULL",
        action="QUARANTINE",
        description="Records containing rescued source data require investigation."
    )
)

ORDER_QUALITY_RULES = (
    QualityRule(
        rule_name = "ORDER_ID_NOT_NULL",
        condition = "order_id IS NOT NULL",
        action = "QUARANTINE",
        description = "Order ID must be present."
    ),

    QualityRule(
        rule_name="ORDER_CUSTOMER_ID_NOT_NULL",
        condition="customer_id IS NOT NULL",
        action="QUARANTINE",
        description="Customer ID must be present for an order."
    ),

    QualityRule(
        rule_name="ORDER_STATUS_VALID",
        condition="""
            order_status IN (
                'created',
                'approved',
                'processing',
                'invoiced',
                'shipped',
                'delivered',
                'canceled',
                'unavailable'
            )
        """,
        action="QUARANTINE",
        description="Order status must be a recognized lifecycle status."
    ),

    QualityRule(
        rule_name="ORDER_PURCHASE_TS_NOT_NULL",
        condition="order_purchase_timestamp IS NOT NULL",
        action="QUARANTINE",
        description="Order purchase timestamp must be present."
    ),

    QualityRule(
        rule_name="ORDER_ESTIMATED_DELIVERY_NOT_NULL",
        condition="order_estimated_delivery_date IS NOT NULL",
        action="QUARANTINE",
        description="Estimated delivery date must be present."
    ),

    QualityRule(
        rule_name="ORDER_NOT_RESCUED",
        condition="_rescued_data IS NULL",
        action="QUARANTINE",
        description="Records containing rescued source data require investigation."
    ),

    QualityRule(
        rule_name="ORDER_CARRIER_NOT_BEFORE_APPROVAL",
        condition="""
            order_delivered_carrier_date IS NULL
            OR order_approved_at IS NULL
            OR order_delivered_carrier_date >= order_approved_at
        """,
        action="WARN",
        description="Carrier handoff timestamp should not occur before order approval."
    ),

    QualityRule(
        rule_name="ORDER_DELIVERY_NOT_BEFORE_CARRIER",
        condition="""
            order_delivered_customer_date IS NULL
            OR order_delivered_carrier_date IS NULL
            OR order_delivered_customer_date >= order_delivered_carrier_date
        """,
        action="WARN",
        description="Customer delivery timestamp should not occur before carrier handoff."
    )
)

ORDER_ITEM_QUALITY_RULES = (

    QualityRule(
        rule_name="ORDER_ITEM_ORDER_ID_NOT_NULL",
        condition="order_id IS NOT NULL",
        action="QUARANTINE",
        description="Order ID must be present for an order item."
    ),

    QualityRule(
        rule_name="ORDER_ITEM_ID_NOT_NULL",
        condition="order_item_id IS NOT NULL",
        action="QUARANTINE",
        description="Order item ID must be present."
    ),

    QualityRule(
        rule_name="ORDER_ITEM_PRODUCT_ID_NOT_NULL",
        condition="product_id IS NOT NULL",
        action="QUARANTINE",
        description="Product ID must be present for an order item."
    ),

    QualityRule(
        rule_name="ORDER_ITEM_SELLER_ID_NOT_NULL",
        condition="seller_id IS NOT NULL",
        action="QUARANTINE",
        description="Seller ID must be present for an order item."
    ),

    QualityRule(
        rule_name="ORDER_ITEM_SHIPPING_LIMIT_NOT_NULL",
        condition="shipping_limit_date IS NOT NULL",
        action="QUARANTINE",
        description="Shipping limit timestamp must be present."
    ),

    QualityRule(
        rule_name="ORDER_ITEM_PRICE_POSITIVE",
        condition="price IS NOT NULL AND price > 0",
        action="QUARANTINE",
        description="Order item price must be greater than zero."
    ),

    QualityRule(
        rule_name="ORDER_ITEM_FREIGHT_NON_NEGATIVE",
        condition="freight_value IS NOT NULL AND freight_value >= 0",
        action="QUARANTINE",
        description="Freight value must be zero or greater."
    ),

    QualityRule(
        rule_name="ORDER_ITEM_NOT_RESCUED",
        condition="_rescued_data IS NULL",
        action="QUARANTINE",
        description="Records containing rescued source data require investigation."
    )
)

PAYMENT_QUALITY_RULES = (

    QualityRule(
        rule_name="PAYMENT_ORDER_ID_NOT_NULL",
        condition="order_id IS NOT NULL",
        action="QUARANTINE",
        description="Order ID must be present for a payment."
    ),

    QualityRule(
        rule_name="PAYMENT_SEQUENCE_NOT_NULL",
        condition="payment_sequential IS NOT NULL",
        action="QUARANTINE",
        description="Payment sequence must be present."
    ),

    QualityRule(
        rule_name="PAYMENT_TYPE_VALID",
        condition="""
            payment_type IN (
                'credit_card',
                'boleto',
                'voucher',
                'debit_card',
                'not_defined'
            )
        """,
        action="QUARANTINE",
        description="Payment type must be a recognized source payment type."
    ),

    QualityRule(
        rule_name="PAYMENT_INSTALLMENTS_NON_NEGATIVE",
        condition=(
            "payment_installments IS NOT NULL "
            "AND payment_installments >= 0"
        ),
        action="QUARANTINE",
        description="Payment installments must be zero or greater."
    ),

    QualityRule(
        rule_name="PAYMENT_VALUE_NON_NEGATIVE",
        condition="payment_value IS NOT NULL AND payment_value >= 0",
        action="QUARANTINE",
        description="Payment value must be zero or greater."
    ),

    QualityRule(
        rule_name="PAYMENT_NOT_RESCUED",
        condition="_rescued_data IS NULL",
        action="QUARANTINE",
        description="Records containing rescued source data require investigation."
    )
)

REVIEW_QUALITY_RULES = (

    QualityRule(
        rule_name="REVIEW_ID_NOT_NULL",
        condition="review_id IS NOT NULL",
        action="QUARANTINE",
        description="Review ID must be present."
    ),

    QualityRule(
        rule_name="REVIEW_ORDER_ID_NOT_NULL",
        condition="order_id IS NOT NULL",
        action="QUARANTINE",
        description="Order ID must be present for a review."
    ),

    QualityRule(
        rule_name="REVIEW_SCORE_VALID",
        condition="review_score IS NOT NULL AND review_score BETWEEN 1 AND 5",
        action="QUARANTINE",
        description="Review score must be between 1 and 5."
    ),

    QualityRule(
        rule_name="REVIEW_CREATION_DATE_NOT_NULL",
        condition="review_creation_date IS NOT NULL",
        action="QUARANTINE",
        description="Review creation date must be present."
    ),

    QualityRule(
        rule_name="REVIEW_ANSWER_TIMESTAMP_NOT_NULL",
        condition="review_answer_timestamp IS NOT NULL",
        action="QUARANTINE",
        description="Review answer timestamp must be present."
    ),

    QualityRule(
        rule_name="REVIEW_ANSWER_NOT_BEFORE_CREATION",
        condition=(
            "review_answer_timestamp IS NULL "
            "OR review_creation_date IS NULL "
            "OR review_answer_timestamp >= review_creation_date"
        ),
        action="QUARANTINE",
        description="Review answer timestamp must not occur before review creation."
    ),

    QualityRule(
        rule_name="REVIEW_NOT_RESCUED",
        condition="_rescued_data IS NULL",
        action="QUARANTINE",
        description="Records containing rescued source data require investigation."
    )
)

PRODUCT_QUALITY_RULES = (

    QualityRule(
        rule_name="PRODUCT_ID_NOT_NULL",
        condition="product_id IS NOT NULL",
        action="QUARANTINE",
        description="Product ID must be present."
    ),

    QualityRule(
        rule_name="PRODUCT_PHYSICAL_ATTRIBUTES_PRESENT",
        condition=(
            "product_weight_g IS NOT NULL "
            "AND product_length_cm IS NOT NULL "
            "AND product_height_cm IS NOT NULL "
            "AND product_width_cm IS NOT NULL"
        ),
        action="WARN",
        description=(
            "Product physical attributes should be present "
            "for logistics and dimensional analysis."
        )
    ),

    QualityRule(
        rule_name="PRODUCT_PHYSICAL_ATTRIBUTES_POSITIVE",
        condition=(
            "product_weight_g IS NULL "
            "OR product_length_cm IS NULL "
            "OR product_height_cm IS NULL "
            "OR product_width_cm IS NULL "
            "OR ("
            "product_weight_g > 0 "
            "AND product_length_cm > 0 "
            "AND product_height_cm > 0 "
            "AND product_width_cm > 0"
            ")"
        ),
        action="WARN",
        description=(
            "Available product physical attributes should "
            "contain positive values."
        )
    ),

    QualityRule(
        rule_name="PRODUCT_NOT_RESCUED",
        condition="_rescued_data IS NULL",
        action="QUARANTINE",
        description="Records containing rescued source data require investigation."
    )
)

SELLER_QUALITY_RULES = (

    QualityRule(
        rule_name="SELLER_ID_NOT_NULL",
        condition="seller_id IS NOT NULL",
        action="QUARANTINE",
        description="Seller ID must be present."
    ),

    QualityRule(
        rule_name="SELLER_ZIP_NOT_NULL",
        condition="seller_zip_code_prefix IS NOT NULL",
        action="QUARANTINE",
        description="Seller ZIP code prefix must be present."
    ),

    QualityRule(
        rule_name="SELLER_CITY_NOT_BLANK",
        condition=(
            "seller_city IS NOT NULL "
            "AND TRIM(seller_city) <> ''"
        ),
        action="QUARANTINE",
        description="Seller city must be present."
    ),

    QualityRule(
        rule_name="SELLER_STATE_VALID",
        condition=(
            "seller_state IS NOT NULL "
            "AND seller_state RLIKE '^[A-Z]{2}$'"
        ),
        action="QUARANTINE",
        description="Seller state must be a two-character uppercase code."
    ),

    QualityRule(
        rule_name="SELLER_NOT_RESCUED",
        condition="_rescued_data IS NULL",
        action="QUARANTINE",
        description="Records containing rescued source data require investigation."
    )
)

GEOLOCATION_QUALITY_RULES = (

    QualityRule(
        rule_name="GEOLOCATION_ZIP_NOT_NULL",
        condition="geolocation_zip_code_prefix IS NOT NULL",
        action="QUARANTINE",
        description="Geolocation ZIP code prefix must be present."
    ),

    QualityRule(
        rule_name="GEOLOCATION_LATITUDE_VALID",
        condition=(
            "geolocation_lat IS NOT NULL "
            "AND geolocation_lat BETWEEN -90 AND 90"
        ),
        action="QUARANTINE",
        description="Latitude must be present and between -90 and 90."
    ),

    QualityRule(
        rule_name="GEOLOCATION_LONGITUDE_VALID",
        condition=(
            "geolocation_lng IS NOT NULL "
            "AND geolocation_lng BETWEEN -180 AND 180"
        ),
        action="QUARANTINE",
        description="Longitude must be present and between -180 and 180."
    ),

    QualityRule(
        rule_name="GEOLOCATION_CITY_NOT_BLANK",
        condition=(
            "geolocation_city IS NOT NULL "
            "AND TRIM(geolocation_city) <> ''"
        ),
        action="QUARANTINE",
        description="Geolocation city must be present."
    ),

    QualityRule(
        rule_name="GEOLOCATION_STATE_VALID",
        condition=(
            "geolocation_state IS NOT NULL "
            "AND geolocation_state RLIKE '^[A-Z]{2}$'"
        ),
        action="QUARANTINE",
        description="Geolocation state must be a two-character uppercase code."
    ),

    QualityRule(
        rule_name="GEOLOCATION_NOT_RESCUED",
        condition="_rescued_data IS NULL",
        action="QUARANTINE",
        description="Records containing rescued source data require investigation."
    )
)

CATEGORY_TRANSLATION_QUALITY_RULES = (

    QualityRule(
        rule_name="CATEGORY_NAME_NOT_BLANK",
        condition=(
            "product_category_name IS NOT NULL "
            "AND TRIM(product_category_name) <> ''"
        ),
        action="QUARANTINE",
        description="Source product category name must be present."
    ),

    QualityRule(
        rule_name="CATEGORY_ENGLISH_NAME_NOT_BLANK",
        condition=(
            "product_category_name_english IS NOT NULL "
            "AND TRIM(product_category_name_english) <> ''"
        ),
        action="QUARANTINE",
        description="English product category translation must be present."
    ),

    QualityRule(
        rule_name="CATEGORY_TRANSLATION_NOT_RESCUED",
        condition="_rescued_data IS NULL",
        action="QUARANTINE",
        description="Records containing rescued source data require investigation."
    )
)

QUALITY_RULE_REGISTRY = {
    "customers": CUSTOMER_QUALITY_RULES,
    "orders": ORDER_QUALITY_RULES,
    "order_items": ORDER_ITEM_QUALITY_RULES,
    "payments": PAYMENT_QUALITY_RULES,
    "reviews": REVIEW_QUALITY_RULES,
    "products": PRODUCT_QUALITY_RULES,
    "sellers": SELLER_QUALITY_RULES,
    "geolocation": GEOLOCATION_QUALITY_RULES,
    "category_translation": CATEGORY_TRANSLATION_QUALITY_RULES
}
