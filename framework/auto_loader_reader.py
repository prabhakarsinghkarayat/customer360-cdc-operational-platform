from pyspark.sql import DataFrame, SparkSession
from config.ingestion_config import IngestionConfig


class AutoLoaderReader:
    """
    Generic Auto Loader reader.

    Reads source-specific configuration from IngestionConfig
    instead of containing entity-specific ingestion logic.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def read(self, config: IngestionConfig) -> DataFrame:
        
        reader = (
            self.spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", config.file_format)
                .option("header", "true")
                .option("rescuedDataColumn", "_rescued_data")
                .option("columnNameOfCorruptRecord", "_corrupt_record")
                .schema(config.schema)
        )

        # Apply entity-specific reader options when configured
        for option_name, option_value in config.reader_options:
            reader = reader.option(option_name, option_value)

        return reader.load(config.source_path)