"""
Azure Databricks Pipeline SDK
Production-grade ETL framework for Unity Catalog medallion architecture.

Usage:
    from pipelines.src import PipelineConfig, BronzeIngestion, SilverTransformation

    config = PipelineConfig(environment="prod")
    ingestion = BronzeIngestion(config)
    ingestion.run()
"""

__version__ = "1.0.0"
__author__ = "XamXam Graph"

from .config import PipelineConfig
from .readers import DataLakeReader, EventHubReader
from .transformers import DataValidator, Deduplicator, PIIMasker
from .writers import DeltaWriter, UnityCatalogWriter

__all__ = [
    "PipelineConfig",
    "DataLakeReader",
    "EventHubReader",
    "DataValidator",
    "Deduplicator",
    "PIIMasker",
    "DeltaWriter",
    "UnityCatalogWriter",
]
