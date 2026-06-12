"""
Centralized pipeline configuration.
Reads from Databricks secrets, environment variables, and widget parameters.
"""
from dataclasses import dataclass, field
from typing import Optional, Dict, Any
import os
import json


@dataclass
class PipelineConfig:
    """Production-grade pipeline configuration."""

    environment: str = "dev"

    # Unity Catalog
    bronze_catalog: str = "bronze"
    silver_catalog: str = "silver"
    gold_catalog: str = "gold"

    # Storage
    storage_account: Optional[str] = None
    landing_container: str = "landing"
    bronze_container: str = "bronze"
    silver_container: str = "silver"
    gold_container: str = "gold"
    checkpoint_container: str = "checkpoint"

    # Pipeline
    trigger_interval: str = "daily"  # daily, hourly, streaming
    max_retries: int = 3
    retry_delay_seconds: int = 300

    # Data quality
    null_threshold_pct: float = 5.0
    freshness_warning_hours: int = 24
    freshness_error_hours: int = 48

    # Spark
    spark_conf: Dict[str, str] = field(default_factory=lambda: {
        "spark.databricks.delta.optimizeWrite.enabled": "true",
        "spark.databricks.delta.autoCompact.enabled": "true",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.shuffle.partitions": "auto",
    })

    # Observability
    enable_great_expectations: bool = True
    ge_checkpoint_path: str = "/dbfs/pipelines/great_expectations/checkpoints/"

    def __post_init__(self):
        # Resolve storage account from env or Databricks secrets
        if self.storage_account is None:
            self.storage_account = self._get_secret("storage-account-name")

    @property
    def bronze_path(self) -> str:
        return f"abfss://{self.bronze_container}@{self.storage_account}.dfs.core.windows.net/"

    @property
    def silver_path(self) -> str:
        return f"abfss://{self.silver_container}@{self.storage_account}.dfs.core.windows.net/"

    @property
    def gold_path(self) -> str:
        return f"abfss://{self.gold_container}@{self.storage_account}.dfs.core.windows.net/"

    @property
    def landing_path(self) -> str:
        return f"abfss://{self.landing_container}@{self.storage_account}.dfs.core.windows.net/"

    @property
    def checkpoint_path(self) -> str:
        return f"abfss://{self.checkpoint_container}@{self.storage_account}.dfs.core.windows.net/"

    @staticmethod
    def _get_secret(secret_name: str, scope: str = "pipeline-secrets") -> str:
        """Fetch a secret from Databricks secret scope, with env var fallback."""
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()
            # Databricks secrets utility
            return spark.conf.get(f"spark.databricks.secret.{scope}.{secret_name}", "")
        except Exception:
            return os.getenv(secret_name.upper().replace("-", "_"), "")

    @classmethod
    def from_widgets(cls) -> "PipelineConfig":
        """Create config from Databricks notebook widgets."""
        try:
            env = dbutils.widgets.get("environment")  # type: ignore
        except Exception:
            env = os.getenv("ENVIRONMENT", "dev")

        try:
            account = dbutils.widgets.get("storage_account")  # type: ignore
        except Exception:
            account = os.getenv("STORAGE_ACCOUNT")

        return cls(environment=env, storage_account=account)
