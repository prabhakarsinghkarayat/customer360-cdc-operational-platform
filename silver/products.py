from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="dev_customer360.silver.products",
    comment="Cleaned and deduplicated product master data"
)
def silver_products():

    df = spark.read.table(
        "dev_customer360.bronze.bronze_products"
    )

    return (
        df
        .filter(
            F.col("product_id").isNotNull()
            & (F.trim(F.col("product_id")) != "")
        )
        .dropDuplicates(["product_id"])
        .select(
            "product_id",
            "product_category_name",
            "product_name_lenght",
            "product_description_lenght",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
            "_record_hash",
            "_ingestion_timestamp"
        )
    )