from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="dev_customer360.silver.reviews",
    comment="Cleaned Olist customer reviews"
)
def silver_reviews():

    df = spark.read.table(
        "dev_customer360.bronze.bronze_reviews"
    )

    return (
        df
        .filter(
            F.col("review_id").isNotNull()
            & (F.trim(F.col("review_id")) != "")
            & F.col("order_id").isNotNull()
            & (F.trim(F.col("order_id")) != "")
        )
        .dropDuplicates(["review_id", "order_id"])
        .select(
            "review_id",
            "order_id",
            "review_score",
            "review_comment_title",
            "review_comment_message",
            "review_creation_date",
            "review_answer_timestamp",
            "_record_hash",
            "_ingestion_timestamp"
        )
    )