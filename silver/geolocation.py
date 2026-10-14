from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="dev_customer360.silver.geolocation",
    comment="Distinct geolocation reference observations"
)
def silver_geolocation():

    df = spark.read.table(
        "dev_customer360.bronze.bronze_geolocation"
    )

    return (
        df
        .filter(
            F.col("geolocation_zip_code_prefix").isNotNull()
            & F.col("geolocation_lat").isNotNull()
            & F.col("geolocation_lng").isNotNull()
        )
        .filter(
            F.col("geolocation_lat").between(-90, 90)
            & F.col("geolocation_lng").between(-180, 180)
        )
        .dropDuplicates([
            "geolocation_zip_code_prefix",
            "geolocation_lat",
            "geolocation_lng",
            "geolocation_city",
            "geolocation_state"
        ])
        .select(
            "geolocation_zip_code_prefix",
            "geolocation_lat",
            "geolocation_lng",
            "geolocation_city",
            "geolocation_state"
        )
    )