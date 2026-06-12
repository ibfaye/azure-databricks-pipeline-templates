"""
Data readers for various source formats.
Supports ADLS Gen2, Event Hubs, Kafka, and API polling.
"""
from typing import Optional, Dict, Any, List
import json
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp, input_file_name
from pyspark.sql.types import StructType

from .config import PipelineConfig


class DataLakeReader:
    """Read data from ADLS Gen2 in various formats with schema inference."""

    def __init__(self, config: PipelineConfig):
        self.config = config
        self.spark = SparkSession.builder.getOrCreate()

    def read_csv(
        self,
        path: str,
        schema: Optional[StructType] = None,
        **options
    ) -> DataFrame:
        """Read CSV files with production defaults."""
        defaults = {
            "header": "true",
            "inferSchema": "true" if schema is None else "false",
            "mode": "FAILFAST",  # Fail on malformed data
            "sep": ",",
            "quote": '"',
            "escape": '"',
            "multiLine": "false",
            "ignoreLeadingWhiteSpace": "true",
            "ignoreTrailingWhiteSpace": "true",
        }
        defaults.update(options)
        return self.spark.read.schema(schema).options(**defaults).csv(path)

    def read_parquet(self, path: str) -> DataFrame:
        """Read Parquet files (schema enforced by Parquet itself)."""
        return self.spark.read.parquet(path)

    def read_json(self, path: str, schema: Optional[StructType] = None) -> DataFrame:
        """Read JSON lines with schema validation."""
        reader = self.spark.read.option("mode", "FAILFAST")
        if schema:
            reader = reader.schema(schema)
        return reader.json(path)

    def read_delta(self, table_path: str) -> DataFrame:
        """Read a Delta table by path."""
        return self.spark.read.format("delta").load(table_path)

    def read_stream(
        self,
        path: str,
        format: str = "cloudFiles",
        schema_location: Optional[str] = None,
        **options
    ) -> DataFrame:
        """Read streaming data using Auto Loader (cloudFiles)."""
        stream = (
            self.spark.readStream.format(format)
            .option("cloudFiles.format", format)
            .option("cloudFiles.schemaLocation", schema_location or self.config.checkpoint_path)
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        )
        for k, v in options.items():
            stream = stream.option(k, v)
        return stream.load(path)

    def landing_to_bronze(self, source_name: str) -> DataFrame:
        """Standard pattern: read from landing zone, add audit columns."""
        base_path = f"{self.config.landing_path}{source_name}/"
        df = self.read_parquet(base_path)
        return (
            df
            .withColumn("_ingested_at", current_timestamp())
            .withColumn("_source_file", input_file_name())
            .withColumn("_source_name", col("_source_name") if "_source_name" in df.columns
                        else col("_source_file"))
        )


class EventHubReader:
    """Read streaming data from Azure Event Hubs (Kafka protocol)."""

    def __init__(self, config: PipelineConfig):
        self.config = config
        self.spark = SparkSession.builder.getOrCreate()

    def read_stream(
        self,
        eventhub_namespace: str,
        eventhub_name: str,
        consumer_group: str = "$Default",
        max_events_per_trigger: int = 10000,
    ) -> DataFrame:
        """Stream from Event Hubs with Kafka connector."""
        connection_string = self._get_eventhub_connection_string(
            eventhub_namespace, eventhub_name
        )

        return (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", f"{eventhub_namespace}.servicebus.windows.net:9093")
            .option("subscribe", eventhub_name)
            .option("kafka.security.protocol", "SASL_SSL")
            .option("kafka.sasl.mechanism", "PLAIN")
            .option("kafka.sasl.jaas.config",
                    f"org.apache.kafka.common.security.plain.PlainLoginModule required "
                    f'username="$ConnectionString" password="{connection_string}";')
            .option("maxOffsetsPerTrigger", max_events_per_trigger)
            .option("startingOffsets", "earliest")  # or "latest"
            .option("failOnDataLoss", "false")
            .load()
            .select(
                col("key").cast("string").alias("event_key"),
                col("value").cast("string").alias("event_body"),
                col("timestamp").alias("event_enqueued_time"),
            )
        )

    def _get_eventhub_connection_string(
        self, namespace: str, eventhub: str
    ) -> str:
        """Fetch Event Hubs connection string from Databricks secrets."""
        try:
            scope = "eventhub-secrets"
            secret_name = f"{namespace}--{eventhub}"
            return PipelineConfig._get_secret(secret_name, scope)
        except Exception:
            raise RuntimeError(
                f"Event Hub connection string not found for {namespace}/{eventhub}. "
                f"Ensure it exists in Databricks secret scope '{scope}'."
            )
