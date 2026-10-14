from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    coalesce,
    concat_ws,
    lit,
    sha2
)

from config.ingestion_config import IngestionConfig


class RecordHasher:
    """
    Creates a deterministic SHA-256 hash from business columns.

    Responsibility:
        Record fingerprinting only.
    """

    NULL_TOKEN = "<NULL>"
    DELIMITER = "||"

    def add_hash(self, df: DataFrame, config: IngestionConfig) -> DataFrame:
        hash_columns = [
            coalesce(col(column_name).cast("string"), lit(self.NULL_TOKEN)) for column_name in config.business_columns
        ]

        return df.withColumn("_record_hash", sha2(concat_ws(self.DELIMITER, *hash_columns), 256))