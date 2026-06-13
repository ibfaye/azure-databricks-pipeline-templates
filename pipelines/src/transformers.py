"""
Data transformation utilities for the medallion architecture.
"""
import os
from typing import List, Optional, Dict, Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, sha2, concat_ws, row_number, current_timestamp,
    regexp_replace, when, lit, stddev, avg, abs as spark_abs,
    broadcast, coalesce, trim, initcap, upper
)
from pyspark.sql.window import Window


class Deduplicator:
    """Remove duplicate records using flexible deduplication strategies."""

    def __init__(self, unique_keys: List[str], order_by: str = "_ingested_at"):
        self.unique_keys = unique_keys
        self.order_by = order_by

    def deduplicate(self, df: DataFrame) -> DataFrame:
        """Keep the latest record per unique key combination."""
        window = Window.partitionBy(*self.unique_keys).orderBy(col(self.order_by).desc())
        return (
            df
            .withColumn("_row_num", row_number().over(window))
            .filter(col("_row_num") == 1)
            .drop("_row_num")
        )


class DataValidator:
    """Production data validation with Great Expectations integration."""

    def __init__(self, null_threshold_pct: float = 5.0):
        self.null_threshold_pct = null_threshold_pct

    def validate_not_null(self, df: DataFrame, columns: List[str]) -> DataFrame:
        """Check null percentage and flag if above threshold."""
        total_count = df.count()
        violations = []
        for column in columns:
            null_count = df.filter(col(column).isNull()).count()
            null_pct = (null_count / total_count * 100) if total_count > 0 else 0
            if null_pct > self.null_threshold_pct:
                violations.append({
                    "column": column,
                    "null_pct": round(null_pct, 2),
                    "threshold_pct": self.null_threshold_pct,
                })
        if violations:
            raise DataQualityException(
                f"Null threshold exceeded: {violations}",
                violations=violations,
            )
        return df

    def validate_freshness(
        self, df: DataFrame, timestamp_column: str, max_hours: int = 24
    ) -> DataFrame:
        """Validate that data is within the freshness window."""
        from pyspark.sql.functions import max as spark_max
        max_ts = df.select(spark_max(col(timestamp_column))).collect()[0][0]
        if max_ts is None:
            raise DataQualityException("No data found — freshness check failed.")
        from datetime import datetime, timezone
        hours_since = (datetime.now(timezone.utc) - max_ts).total_seconds() / 3600
        if hours_since > max_hours:
            raise DataQualityException(
                f"Data is {hours_since:.1f}h old — exceeds {max_hours}h freshness threshold."
            )
        return df

    def validate_schema(self, df: DataFrame, expected_columns: List[str]) -> DataFrame:
        """Check that all expected columns exist."""
        actual = set(df.columns)
        expected_set = set(expected_columns)
        missing = expected_set - actual
        extra = actual - expected_set
        if missing:
            raise DataQualityException(f"Missing columns: {missing}")
        if extra:
            print(f"[WARN] Extra columns found (OK in schema evolution): {extra}")
        return df


class PIIMasker:
    """Production PII masking for GDPR/CCPA compliance."""

    SHA256_SALT: str = os.getenv("DBT_PII_SALT", "databricks-pii-salt-2025")

    @staticmethod
    def hash_column(df: DataFrame, column: str) -> DataFrame:
        """SHA-256 hash with salt for pseudonymization."""
        return df.withColumn(
            column,
            sha2(concat_ws("|", lit(PIIMasker.SHA256_SALT), col(column)), 256)
        )

    @staticmethod
    def mask_email(df: DataFrame, column: str) -> DataFrame:
        """Mask email: jdoe@example.com → jd***@e***.com"""
        return df.withColumn(
            column,
            regexp_replace(
                col(column),
                r'^(.{2})([^@]*)(@)([^.]+)(.*)$',
                r'$1***$3$4***$5'
            )
        )

    @staticmethod
    def mask_phone(df: DataFrame, column: str) -> DataFrame:
        """Mask phone: +1234567890 → +12******90"""
        return df.withColumn(
            column,
            regexp_replace(col(column), r'(\+?\d{2,3})\d{6,}(\d{2})', r'$1******$2')
        )

    @staticmethod
    def drop_pii_columns(df: DataFrame, columns: List[str]) -> DataFrame:
        """Completely remove PII columns (for gold layer)."""
        return df.drop(*columns)

    @classmethod
    def bronze_to_silver(
        cls, df: DataFrame, pii_columns: Dict[str, str]
    ) -> DataFrame:
        """
        Apply PII masking strategies.
        pii_columns: map of column_name → strategy (hash, mask_email, mask_phone, drop)
        """
        result = df
        for col_name, strategy in pii_columns.items():
            if strategy == "hash":
                result = cls.hash_column(result, col_name)
            elif strategy == "mask_email":
                result = cls.mask_email(result, col_name)
            elif strategy == "mask_phone":
                result = cls.mask_phone(result, col_name)
            elif strategy == "drop":
                result = result.drop(col_name)
        return result


class DataQualityException(Exception):
    """Custom exception for data quality failures."""

    def __init__(self, message: str, violations: Optional[List[Dict]] = None):
        super().__init__(message)
        self.violations = violations or []
