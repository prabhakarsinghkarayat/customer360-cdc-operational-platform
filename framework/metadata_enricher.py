from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
    to_date
)

from config.ingestion_config import IngestionConfig


class MetadataEnricher:
    """
    Adds technical lineage and ingestion metadata to Bronze records.

    Responsibility:
        Metadata enrichment only.
    """

    def enrich(self, df: DataFrame, config: IngestionConfig) -> DataFrame:
        enriched_df = (

            # -------------------------------------------------------
            # Source metadata
            # -------------------------------------------------------
            df.withColumn("_source_system", lit(config.source_system))
            .withColumn("_source_entity", lit(config.entity_name))

            # -------------------------------------------------------
            # File metadata
            # -------------------------------------------------------
            .withColumn("_source_file_path", col("_metadata.file_path"))
            .withColumn("_source_file_name", col("_metadata.file_name"))
            .withColumn("_source_file_size", col("_metadata.file_size"))
            .withColumn("_source_file_modification_time", col("_metadata.file_modification_time"))
            
            # -------------------------------------------------------
            # Ingestion metadata
            # -------------------------------------------------------
            .withColumn("_ingestion_timestamp", current_timestamp())
        )

        enriched_df = enriched_df.withColumn("_ingestion_date", to_date(col("_ingestion_timestamp")))
        return enriched_df