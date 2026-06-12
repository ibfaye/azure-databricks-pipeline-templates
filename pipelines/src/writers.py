"""
Delta Lake and Unity Catalog writers.
Handles merge/upsert, schema evolution, vacuum, and optimize.
"""
from typing import Optional, List, Dict, Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from delta.tables import DeltaTable

from .config import PipelineConfig


class DeltaWriter:
    """Write DataFrames to Delta Lake with production-grade configuration."""

    def __init__(self, config: PipelineConfig):
        self.config = config
        self.spark = SparkSession.builder.getOrCreate()

    def write(
        self,
        df: DataFrame,
        table_path: str,
        mode: str = "overwrite",
        partition_by: Optional[List[str]] = None,
        zorder_by: Optional[List[str]] = None,
        optimize: bool = True,
        vacuum_retention_hours: int = 168,  # 7 days
    ) -> None:
        """Write a DataFrame to Delta with full configuration."""
        writer = df.write.format("delta").mode(mode)

        if partition_by:
            writer = writer.partitionBy(*partition_by)

        writer.save(table_path)

        if optimize:
            self.optimize(table_path, zorder_by)
            self.vacuum(table_path, retention_hours=vacuum_retention_hours)

    def merge(
        self,
        source: DataFrame,
        target_path: str,
        merge_keys: List[str],
        update_columns: Optional[List[str]] = None,
        insert_only: bool = False,
    ) -> None:
        """UPSERT into a Delta table using MERGE."""
        if not DeltaTable.isDeltaTable(self.spark, target_path):
            raise ValueError(f"Target path is not a Delta table: {target_path}")

        target = DeltaTable.forPath(self.spark, target_path)
        merge_condition = " AND ".join(
            f"target.{k} = source.{k}" for k in merge_keys
        )

        if insert_only:
            # Insert-only: skip existing records
            target.alias("target").merge(
                source.alias("source"), merge_condition
            ).whenNotMatchedInsertAll().execute()
        else:
            # Full merge: update matching, insert non-matching
            merge_op = target.alias("target").merge(source.alias("source"), merge_condition)

            if update_columns:
                update_set = {c: f"source.{c}" for c in update_columns}
                merge_op = merge_op.whenMatchedUpdate(set=update_set)

            merge_op.whenNotMatchedInsertAll().execute()

    def stream_write(
        self,
        df: DataFrame,
        table_path: str,
        checkpoint_path: str,
        trigger_interval: str = "availableNow",  # availableNow, once, or e.g. '10 seconds'
        output_mode: str = "append",
        partition_by: Optional[List[str]] = None,
    ) -> None:
        """Stream a DataFrame to Delta (Auto Loader + structured streaming)."""
        writer = (
            df.writeStream
            .format("delta")
            .outputMode(output_mode)
            .option("checkpointLocation", checkpoint_path)
            .trigger(processingTime=trigger_interval)
        )

        if partition_by:
            writer = writer.partitionBy(*partition_by)

        stream = writer.start(table_path)
        stream.awaitTermination()

    def optimize(
        self,
        table_path: str,
        zorder_by: Optional[List[str]] = None,
    ) -> None:
        """Run OPTIMIZE on a Delta table for read performance."""
        self.spark.sql(f"OPTIMIZE delta.`{table_path}`")
        if zorder_by:
            zorder_cols = ", ".join(zorder_by)
            self.spark.sql(f"OPTIMIZE delta.`{table_path}` ZORDER BY ({zorder_cols})")

    def vacuum(self, table_path: str, retention_hours: int = 168) -> None:
        """Clean up stale files older than retention window."""
        self.spark.sql(
            f"VACUUM delta.`{table_path}` RETAIN {retention_hours} HOURS"
        )


class UnityCatalogWriter:
    """Write directly to Unity Catalog tables with three-part naming."""

    def __init__(self, config: PipelineConfig):
        self.config = config
        self.spark = SparkSession.builder.getOrCreate()

    def write_table(
        self,
        df: DataFrame,
        catalog: str,
        schema: str,
        table: str,
        mode: str = "overwrite",
        partition_by: Optional[List[str]] = None,
        comment: Optional[str] = None,
        tbl_properties: Optional[Dict[str, str]] = None,
    ) -> None:
        """Write a DataFrame to a Unity Catalog table (catalog.schema.table)."""
        full_name = f"{catalog}.{schema}.{table}"

        # Ensure schema exists
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

        writer = df.write.mode(mode)

        if partition_by:
            writer = writer.partitionBy(*partition_by)

        writer.saveAsTable(full_name)

        # Add metadata
        if comment:
            self.spark.sql(f"COMMENT ON TABLE {full_name} IS '{comment}'")

        if tbl_properties:
            for k, v in tbl_properties.items():
                self.spark.sql(
                    f"ALTER TABLE {full_name} SET TBLPROPERTIES ('{k}' = '{v}')"
                )

    def merge_table(
        self,
        source: DataFrame,
        catalog: str,
        schema: str,
        table: str,
        merge_keys: List[str],
        update_columns: Optional[List[str]] = None,
    ) -> None:
        """MERGE into an existing Unity Catalog table."""
        full_name = f"{catalog}.{schema}.{table}"
        target = DeltaTable.forName(self.spark, full_name)

        merge_condition = " AND ".join(
            f"target.{k} = source.{k}" for k in merge_keys
        )

        merge_op = target.alias("target").merge(source.alias("source"), merge_condition)

        if update_columns:
            update_set = {c: f"source.{c}" for c in update_columns}
            merge_op = merge_op.whenMatchedUpdate(set=update_set)

        merge_op.whenNotMatchedInsertAll().execute()

    def create_external_table(
        self,
        location: str,
        catalog: str,
        schema: str,
        table: str,
        file_format: str = "delta",
    ) -> None:
        """Register an existing Delta path as a Unity Catalog external table."""
        full_name = f"{catalog}.{schema}.{table}"
        self.spark.sql(
            f"CREATE TABLE IF NOT EXISTS {full_name} "
            f"USING {file_format} "
            f"LOCATION '{location}'"
        )

    def grant_select(self, catalog: str, schema: str, table: str, principal: str) -> None:
        """Grant SELECT to a principal (user/group/service principal)."""
        full_name = f"{catalog}.{schema}.{table}"
        self.spark.sql(f"GRANT SELECT ON TABLE {full_name} TO `{principal}`")
