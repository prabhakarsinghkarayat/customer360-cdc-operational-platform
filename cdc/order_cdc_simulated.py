from pyspark import pipelines as dp

from config.ingestion_config import ORDER_CDC_SIMULATOR_CONFIG
from framework.auto_loader_reader import AutoLoaderReader
from framework.metadata_enricher import MetadataEnricher


@dp.table(
    name="bronze_order_cdc_simulated",
    comment=(
        "Incremental simulated order CDC events ingested "
        "from the CDC simulator landing area using Auto Loader."
    )
)
def bronze_order_cdc_simulated():

    reader = AutoLoaderReader(spark)
    metadata_enricher = MetadataEnricher()

    df = reader.read(
        ORDER_CDC_SIMULATOR_CONFIG
    )

    return metadata_enricher.enrich(
        df,
        ORDER_CDC_SIMULATOR_CONFIG
    )