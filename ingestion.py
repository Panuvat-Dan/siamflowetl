import pandas as pd
import pyspark
from pyspark.sql import SparkSession
from siametl.logging import Logger
from siametl.quality import DataQuality
from .logging import LoggingHandler
from .quality import QualityHandler

class ReadHandler:
    def __init__(self, spark):
        self.spark = spark

    def read(self, source, source_format, engine="spark", **kwargs):
        if source_format.lower() == "csv":
            if engine == "pandas":
                return pd.read_csv(source, **kwargs)
            elif engine == "spark":
                return self.spark.read.csv(source, header=True, inferSchema=True, **kwargs)
        elif source_format.lower() == "excel":
            if engine == "pandas":
                return pd.read_excel(source, **kwargs)
            elif engine == "spark":
                return self.spark.read.format("com.crealytics.spark.excel").option("header", "true").load(source, **kwargs)
        elif source_format.lower() == "json":
            if engine == "pandas":
                return pd.read_json(source, **kwargs)
            elif engine == "spark":
                return self.spark.read.json(source, **kwargs)
        elif source_format.lower() == "parquet":
            if engine == "pandas":
                return pd.read_parquet(source, engine="pyarrow", **kwargs)
            elif engine == "spark":
                return self.spark.read.parquet(source, **kwargs)
        elif source_format.lower() == "table":
            if engine == "pandas":
                return pd.read_sql_table(source, **kwargs)
            elif engine == "spark":
                return self.spark.read.format("jdbc").options(**kwargs).load()
        else:
            raise ValueError("Unsupported source format. Supported formats: 'csv', 'excel', 'json', 'parquet', 'table'.")

class WriteHandler:
    def __init__(self, spark):
        self.spark = spark

    def write(self, data, target, target_format, write_mode="overwrite", engine="spark", **kwargs):
        if engine == "pandas":
            if target_format.lower() == "csv":
                data.to_csv(target, mode=write_mode, **kwargs)
            elif target_format.lower() == "json":
                data.to_json(target, mode=write_mode, **kwargs)
            elif target_format.lower() == "parquet":
                data.to_parquet(target, mode=write_mode, **kwargs)
            elif target_format.lower() == "table":
                data.to_sql(target, **kwargs)
            elif target_format.lower() == "xlsx":
                data.to_excel(target, mode=write_mode, **kwargs)
            else:
                raise ValueError("Unsupported target format. Supported formats: 'csv', 'json', 'parquet', 'table', 'xlsx'.")
        elif engine == "spark":
            if target_format.lower() == "csv":
                data.write.mode(write_mode).csv(target)
            elif target_format.lower() == "json":
                data.write.mode(write_mode).json(target)
            elif target_format.lower() == "parquet":
                data.write.mode(write_mode).parquet(target)
            elif target_format.lower() == "table":
                data.write.mode(write_mode).saveAsTable(target)
            else:
                raise ValueError("Unsupported target format for Spark. Supported formats: 'csv', 'json', 'parquet', 'table'.")
        else:
            raise ValueError("Unsupported engine. Supported engines: 'spark', 'pandas'.")

class IngestionHandler:
    def __init__(self, spark_app_name="IngestionApp", spark_master="local[*]"):
        self.spark = SparkSession.builder \
            .appName(spark_app_name) \
            .master(spark_master) \
            .getOrCreate()
        self.reader = ReadHandler(self.spark)
        self.writer = WriteHandler(self.spark)
        self.logger = LoggingHandler()
        self.quality = QualityHandler()

    def ingest(self, source, source_format, target, target_format, write_mode="overwrite", engine="spark", **kwargs):
        self.logger.log_info(f"Ingestion started for source: {source} to target: {target}")
        try:
            data = self.reader.read(source, source_format, engine, **kwargs)
            stage_target = f"stage_{target}"
            self.writer.write(data, stage_target, target_format, write_mode, engine, **kwargs)
            self.quality.validate_no_nulls(data, "column_name")
            self.quality.validate_threshold(data, "column_name", threshold=0.1)
            self.writer.write(data, target, target_format, write_mode, engine, **kwargs)
            self.logger.log_info(f"Ingestion completed for source: {source} to target: {target}")
        except Exception as e:
            self.logger.log_error(f"Ingestion failed for source: {source} to target: {target} with error: {e}")
            raise e

if __name__ == "__main__":
    # Example csv to parquet
    ingestion = IngestionHandler()
    ingestion.ingest(
        source="data/input.csv",
        source_format="csv",
        target="data/output.parquet",
        target_format="parquet",
        write_mode="overwrite"
    )
    # Example json to table
    ingestion.ingest(
        source="data/input.json",
        source_format="json",
        target="my_table",
        target_format="table",
        write_mode="append",
        url="jdbc:mysql://localhost:3306/mydb",
        driver="com.mysql.cj.jdbc.Driver",
        user="root",
        password="password"
    )
    # Example table to csv
    ingestion.ingest(
        source="my_table",
        source_format="table",
        target="data/output.csv",
        target_format="csv",
        write_mode="overwrite",
        url="jdbc:mysql://localhost:3306/mydb",
        driver="com.mysql.cj.jdbc.Driver",
        user="root",
        password="password"
    )




