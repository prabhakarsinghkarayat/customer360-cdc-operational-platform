from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="dev_customer360.silver.sellers",
    comment="Cleaned seller master data"
)
def silver_sellers():

    df = spark.read.table(
        "dev_customer360.bronze.bronze_sellers"
    )

    return (
        df
        .filter(
            F.col("seller_id").isNotNull()
            & (F.trim(F.col("seller_id")) != "")
        )
        .dropDuplicates(["seller_id"])
        .select(
            "seller_id",
            "seller_zip_code_prefix",
            "seller_city",
            "seller_state",
            "_record_hash",
            "_ingestion_timestamp"
        )
    )