# Project: Internal Data Platform
# Domains: Manufacturing / Finance / SBE
# Tech: Spark / Databricks / Airflow / K8s
# Author: Parveen Kumar (parvkuma)

# ==============================
# Directory Structure
# ==============================
# data-platform/
# ├── domains/
# │   ├── manufacturing/
# │   │   ├── bronze_ingest.py
# │   │   ├── silver_transform.py
# │   │   └── gold_metrics.py
# │   ├── finance/
# │   │   ├── bronze_ingest.py
# │   │   ├── silver_transform.py
# │   │   └── gold_metrics.py
# │   └── sbe/
# │       ├── bronze_ingest.py
# │       ├── silver_transform.py
# │       └── gold_metrics.py
# ├── common/
# │   ├── base_job.py
# │   ├── spark_session.py
# │   ├── config.py
# │   └── logger.py
# └── airflow/
#     └── domain_pipelines_dag.py

# ==============================
# common/spark_session.py
# ==============================
from pyspark.sql import SparkSession


def get_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hive")
        .enableHiveSupport()
        .getOrCreate()
    )

# ==============================
# common/base_job.py
# ==============================
class BaseSparkJob:
    def __init__(self, spark):
        self.spark = spark

    def read(self):
        raise NotImplementedError

    def transform(self, df):
        raise NotImplementedError

    def write(self, df):
        raise NotImplementedError

    def run(self):
        df = self.read()
        df = self.transform(df)
        self.write(df)

# ==============================
# common/logger.py
# ==============================
import logging


def get_logger(name):
    logging.basicConfig(level=logging.INFO)
    return logging.getLogger(name)

# ==============================
# Manufacturing Domain
# ==============================
# domains/manufacturing/bronze_ingest.py
from common.spark_session import get_spark
from common.base_job import BaseSparkJob
from common.logger import get_logger

class ManufacturingBronzeIngest(BaseSparkJob):
    def read(self):
        return self.spark.read.csv(
            "s3://raw/manufacturing/production.csv",
            header=True,
            inferSchema=True
        )

    def transform(self, df):
        return df.withColumnRenamed("prod_dt", "production_date")

    def write(self, df):
        df.write.format("iceberg").mode("append").save("warehouse.manufacturing.bronze_production")

if __name__ == "__main__":
    spark = get_spark("manufacturing_bronze")
    ManufacturingBronzeIngest(spark).run()

# ==============================
# domains/manufacturing/silver_transform.py
from pyspark.sql.functions import col
from common.spark_session import get_spark
from common.base_job import BaseSparkJob

class ManufacturingSilverTransform(BaseSparkJob):
    def read(self):
        return self.spark.table("warehouse.manufacturing.bronze_production")

    def transform(self, df):
        return df.filter(col("quantity") > 0)

    def write(self, df):
        df.write.format("iceberg").mode("overwrite").save("warehouse.manufacturing.silver_production")

# ==============================
# domains/manufacturing/gold_metrics.py
from pyspark.sql.functions import sum
from common.spark_session import get_spark
from common.base_job import BaseSparkJob

class ManufacturingGoldMetrics(BaseSparkJob):
    def read(self):
        return self.spark.table("warehouse.manufacturing.silver_production")

    def transform(self, df):
        return df.groupBy("plant_id").agg(sum("quantity").alias("total_output"))

    def write(self, df):
        df.write.format("iceberg").mode("overwrite").save("warehouse.manufacturing.gold_output_metrics")

# ==============================
# Finance Domain
# ==============================
# domains/finance/bronze_ingest.py
from common.spark_session import get_spark
from common.base_job import BaseSparkJob

class FinanceBronzeIngest(BaseSparkJob):
    def read(self):
        return self.spark.read.parquet("s3://raw/finance/transactions/")

    def transform(self, df):
        return df

    def write(self, df):
        df.write.format("iceberg").mode("append").save("warehouse.finance.bronze_transactions")

# domains/finance/silver_transform.py
from pyspark.sql.functions import col
from common.base_job import BaseSparkJob

class FinanceSilverTransform(BaseSparkJob):
    def read(self):
        return self.spark.table("warehouse.finance.bronze_transactions")

    def transform(self, df):
        return df.filter(col("amount") > 0)

    def write(self, df):
        df.write.format("iceberg").mode("overwrite").save("warehouse.finance.silver_transactions")

# domains/finance/gold_metrics.py
from pyspark.sql.functions import sum
from common.base_job import BaseSparkJob

class FinanceGoldMetrics(BaseSparkJob):
    def read(self):
        return self.spark.table("warehouse.finance.silver_transactions")

    def transform(self, df):
        return df.groupBy("account_id").agg(sum("amount").alias("total_spend"))

    def write(self, df):
        df.write.format("iceberg").mode("overwrite").save("warehouse.finance.gold_account_metrics")

# ==============================
# SBE Domain (Sales / Business Events)
# ==============================
# domains/sbe/bronze_ingest.py
from common.spark_session import get_spark
from common.base_job import BaseSparkJob

class SBEBronzeIngest(BaseSparkJob):
    def read(self):
        return self.spark.read.json("s3://raw/sbe/events/")

    def transform(self, df):
        return df

    def write(self, df):
        df.write.format("iceberg").mode("append").save("warehouse.sbe.bronze_events")

# domains/sbe/silver_transform.py
from pyspark.sql.functions import col
from common.base_job import BaseSparkJob

class SBESilverTransform(BaseSparkJob):
    def read(self):
        return self.spark.table("warehouse.sbe.bronze_events")

    def transform(self, df):
        return df.filter(col("event_type").isNotNull())

    def write(self, df):
        df.write.format("iceberg").mode("overwrite").save("warehouse.sbe.silver_events")

# domains/sbe/gold_metrics.py
from pyspark.sql.functions import count
from common.base_job import BaseSparkJob

class SBEGoldMetrics(BaseSparkJob):
    def read(self):
        return self.spark.table("warehouse.sbe.silver_events")

    def transform(self, df):
        return df.groupBy("event_type").agg(count("*").alias("event_count"))

    def write(self, df):
        df.write.format("iceberg").mode("overwrite").save("warehouse.sbe.gold_event_metrics")

# ==============================
# Data Quality Framework (FROM SCRATCH)
# ==============================
# common/data_quality.py

"""
Data Quality Framework
----------------------
This module provides reusable, Spark-native data quality checks
that can be applied at Bronze, Silver, or Gold layers.

Design goals:
- Fail fast
- Domain agnostic
- Spark-scalable (no collect to driver)
- Easy to extend
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col


class DataQualityResult:
    """
    Simple container to hold check results.
    """

    def __init__(self, check_name: str, passed: bool, details: str = ""):
        self.check_name = check_name
        self.passed = passed
        self.details = details

    def __repr__(self):
        status = "PASS" if self.passed else "FAIL"
        return f"{self.check_name}: {status} ({self.details})"


class DataQualityValidator:
    """
    Core Data Quality Validator.
    Each instance operates on a single Spark DataFrame.
    """

    def __init__(self, df: DataFrame):
        self.df = df
        self.results = []

    # ------------------------------
    # Row Count Check
    # ------------------------------
    def check_row_count(self, min_rows: int = 1):
        row_count = self.df.count()
        passed = row_count >= min_rows
        self.results.append(
            DataQualityResult(
                check_name="row_count",
                passed=passed,
                details=f"rows={row_count}, min_required={min_rows}"
            )
        )
        return self

    # ------------------------------
    # Null Check
    # ------------------------------
    def check_not_null(self, column: str):
        null_count = self.df.filter(col(column).isNull()).count()
        passed = null_count == 0
        self.results.append(
            DataQualityResult(
                check_name=f"not_null_{column}",
                passed=passed,
                details=f"nulls={null_count}"
            )
        )
        return self

    # ------------------------------
    # Primary Key Uniqueness Check
    # ------------------------------
    def check_primary_key(self, column: str):
        total_count = self.df.count()
        distinct_count = self.df.select(column).distinct().count()
        passed = total_count == distinct_count
        self.results.append(
            DataQualityResult(
                check_name=f"pk_unique_{column}",
                passed=passed,
                details=f"total={total_count}, distinct={distinct_count}"
            )
        )
        return self

    # ------------------------------
    # Execute All Checks
    # ------------------------------
    def validate(self):
        """
        Returns True if all checks pass, else False.
        """
        return all(result.passed for result in self.results)

    # ------------------------------
    # Raise Exception on Failure
    # ------------------------------
    def raise_if_failed(self):
        failed = [r for r in self.results if not r.passed]
        if failed:
            message = "Data Quality Checks Failed:
"
            message += "
".join(str(r) for r in failed)
            raise ValueError(message)

    # ------------------------------
    # Get Structured Results
    # ------------------------------
    def get_results(self):
        return self.results


# ==============================
# Persist Quality Results → Iceberg Audit Table
# ==============================
# common/data_quality_audit.py

from pyspark.sql import Row
from datetime import datetime


def persist_quality_results(spark, results, table_name: str, domain: str, layer: str):
    """
    Persists data quality results into an Iceberg audit table.
    """
    rows = []
    run_ts = datetime.utcnow().isoformat()

    for r in results:
        rows.append(
            Row(
                domain=domain,
                layer=layer,
                check_name=r.check_name,
                passed=r.passed,
                details=r.details,
                run_timestamp=run_ts
            )
        )

    audit_df = spark.createDataFrame(rows)

    (
        audit_df.write
        .format("iceberg")
        .mode("append")
        .save(table_name)
    )


# ==============================
# Schema Drift Detection
# ==============================
# common/schema_drift.py

"""
Schema Drift Detector
---------------------
Detects column-level schema changes between source DataFrame
and target Iceberg table.
"""

from pyspark.sql.types import StructType


class SchemaDriftDetector:
    def __init__(self, source_schema: StructType, target_schema: StructType):
        self.source_schema = source_schema
        self.target_schema = target_schema

    def detect(self):
        source_fields = {f.name: f.dataType.simpleString() for f in self.source_schema.fields}
        target_fields = {f.name: f.dataType.simpleString() for f in self.target_schema.fields}

        added_columns = source_fields.keys() - target_fields.keys()
        removed_columns = target_fields.keys() - source_fields.keys()
        type_changes = {
            col: (source_fields[col], target_fields[col])
            for col in source_fields.keys() & target_fields.keys()
            if source_fields[col] != target_fields[col]
        }

        return {
            "added_columns": list(added_columns),
            "removed_columns": list(removed_columns),
            "type_changes": type_changes
        }

    def has_drift(self):
        drift = self.detect()
        return bool(drift["added_columns"] or drift["removed_columns"] or drift["type_changes"])


# ==============================
# Example Usage (Silver / Gold Job)
# ==============================
# validator = DataQualityValidator(df)
# validator.check_row_count(1).check_primary_key("id")
# validator.raise_if_failed()
#
# persist_quality_results(
#     spark=spark,
#     results=validator.get_results(),
#     table_name="platform.audit_data_quality",
#     domain="finance",
#     layer="silver"
# )
#
# target_schema = spark.table("warehouse.finance.silver_transactions").schema
# drift_detector = SchemaDriftDetector(df.schema, target_schema)
# if drift_detector.has_drift():
#     raise ValueError(f"Schema drift detected: {drift_detector.detect()}")

# ==============================
# Freshness & SLA Checks
# ==============================
# common/freshness_checks.py

from pyspark.sql.functions import max, col
from datetime import datetime, timedelta


class FreshnessValidator:
    def __init__(self, df, timestamp_column: str):
        self.df = df
        self.timestamp_column = timestamp_column

    def check_freshness_minutes(self, max_delay_minutes: int):
        latest_ts = self.df.select(max(col(self.timestamp_column))).collect()[0][0]
        delay = (datetime.utcnow() - latest_ts).total_seconds() / 60
        if delay > max_delay_minutes:
            raise ValueError(f"Freshness SLA breached: delay={delay} minutes")


# ==============================
# Config-driven Checks (YAML)
# ==============================
# common/quality_config_loader.py

import yaml


def load_quality_config(path: str):
    with open(path, 'r') as f:
        return yaml.safe_load(f)


# Example YAML (per table)
# quality_rules:
#   min_rows: 10
#   not_null:
#     - account_id
#   primary_key: transaction_id
#   freshness:
#     column: event_time
#     max_delay_minutes: 60


# ==============================
# Auto Schema Evolution with Allowlist
# ==============================
# common/schema_evolution.py

class SchemaEvolutionManager:
    def __init__(self, allowed_additions=None, allowed_type_changes=None):
        self.allowed_additions = allowed_additions or []
        self.allowed_type_changes = allowed_type_changes or {}

    def validate(self, drift):
        for col in drift['added_columns']:
            if col not in self.allowed_additions:
                raise ValueError(f"Unauthorized column addition: {col}")

        for col, (src, tgt) in drift['type_changes'].items():
            allowed = self.allowed_type_changes.get(col)
            if allowed != (src, tgt):
                raise ValueError(f"Unauthorized type change: {col} {src}->{tgt}")


# ==============================
# Airflow Sensor on Audit Table
# ==============================
# airflow/quality_sensor.py

from airflow.sensors.base import BaseSensorOperator


class DataQualitySensor(BaseSensorOperator):
    def poke(self, context):
        # Pseudocode: query Iceberg audit table
        # return True only if latest run passed
        return True


# ==============================
# Great Expectations Parity (Spark-native)
# ==============================
# common/expectations.py

class SparkExpectations:
    def __init__(self, df):
        self.df = df

    def expect_column_values_to_not_be_null(self, column):
        if self.df.filter(col(column).isNull()).count() > 0:
            raise ValueError(f"Expectation failed: {column} has nulls")

    def expect_column_values_to_be_unique(self, column):
        if self.df.count() != self.df.select(column).distinct().count():
            raise ValueError(f"Expectation failed: {column} not unique")

    def expect_table_row_count_to_be_between(self, min_rows, max_rows):
        count = self.df.count()
        if not (min_rows <= count <= max_rows):
            raise ValueError(f"Expectation failed: row_count={count}")


# ==============================
# Airflow DAG
# ==============================
# airflow/domain_pipelines_dag.py
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime

with DAG(
    dag_id="domain_data_pipelines",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
):

    manufacturing = KubernetesPodOperator(
        name="manufacturing_pipeline",
        task_id="manufacturing_pipeline",
        image="spark-job:latest",
        cmds=["spark-submit"],
        arguments=["domains/manufacturing/bronze_ingest.py"],
        namespace="data-platform"
    )
