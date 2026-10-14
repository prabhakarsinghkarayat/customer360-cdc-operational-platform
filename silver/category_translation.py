from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="dev_customer360.silver.category_translation",
    comment="Product category Portuguese-to-English lookup"
)
def silver_category_translation():

    df = spark.read.table(
        "dev_customer360.bronze.bronze_category_translation"
    )

    return (
        df
        .filter(
            F.col("product_category_name").isNotNull()
            & (
                F.trim(F.col("product_category_name")) != ""
            )
        )
        .dropDuplicates(["product_category_name"])
        .select(
            "product_category_name",
            "product_category_name_english"
        )
    )