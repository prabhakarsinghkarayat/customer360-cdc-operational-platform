from pyspark import pipelines as dp

from config.ingestion_config import INGESTION_CONFIGS
from framework.auto_loader_reader import AutoLoaderReader
from framework.metadata_enricher import MetadataEnricher
from framework.record_hasher import RecordHasher



# ---------------------------------------------------------
# Reusable framework components
# ---------------------------------------------------------

reader = AutoLoaderReader(spark)
metadata_enricher = MetadataEnricher()
record_hasher = RecordHasher()



# ---------------------------------------------------------
# Generic Bronze Dataset Factory
# ---------------------------------------------------------

def create_bronze_dataset(entity_name: str):

    config = INGESTION_CONFIGS[entity_name]

    @dp.table(
        name=config.target_table,
        comment=(
            f"Raw Olist {entity_name} records incrementally "
            f"ingested using Auto Loader with source lineage "
            f"and ingestion metadata."
        )
    )
    def bronze_dataset():

        df = reader.read(config)
        df = metadata_enricher.enrich(df, config)
        df = record_hasher.add_hash(df, config)

        return df


# ---------------------------------------------------------
# Register Bronze Streaming Tables
# ---------------------------------------------------------

for entity_name in INGESTION_CONFIGS:
    create_bronze_dataset(entity_name)