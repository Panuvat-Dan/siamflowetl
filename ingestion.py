from pyspark.sql import SparkSession
import pandas as pd
from .quality import QualityHandler

class IngestionHandler:
    def __init__(self, spark_app_name="IngestionApp", spark_master="local[*]"):
        self.spark = SparkSession.builder \
            .appName(spark_app_name) \
            .master(spark_master) \
            .getOrCreate()
        self.quality = QualityHandler()

    def read_data(self, source, source_format, engine="pandas", **kwargs):
        if engine == "pandas":
            if source_format.lower() == "csv":
                return pd.read_csv(source, **kwargs)
            elif source_format.lower() == "json":
                return pd.read_json(source, **kwargs)
            elif source_format.lower() == "parquet":
                return pd.read_parquet(source,engine = "pyarrow", **kwargs) # pandas need extension engine to read parquet
            elif source_format.lower() == "table":
                return pd.read_sql_table(source, **kwargs)
            elif source_format.lower() == "xlsx":
                return pd.read_excel(source, **kwargs)
            else:
                raise ValueError("Unsupported source format. Supported formats: 'csv', 'json', 'parquet', 'table', 'xlsx'.")
        elif engine == "spark":
            if source_format.lower() == "csv":
                return self.spark.read.csv(source, **kwargs)
            elif source_format.lower() == "json":
                return self.spark.read.json(source, **kwargs)
            elif source_format.lower() == "parquet":
                return self.spark.read.parquet(source, **kwargs)
            elif source_format.lower() == "table":
                return self.spark.read.table(source, **kwargs)
            else:
                raise ValueError("Unsupported source format for Spark. Supported formats: 'csv', 'json', 'parquet', 'table'.")
        else:
            raise ValueError("Unsupported engine. Supported engines: 'spark', 'pandas'.")

    def write_data(self, data, target, target_format, engine="pandas", write_mode="overwrite", **kwargs):
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

    def process_data(self, source, source_format, target, target_format, engine="pandas", read_kwargs={}, write_kwargs={}):
        try:
            self.logger.log_info(f"Reading data from source {source}, source format {source_format}...................")
            data = self.read_data(source, source_format, engine, **read_kwargs)
            
            self.logger.log_info("Validating data quality...................")
            self.quality.validate_no_nulls(data, "primary_key_column")  # Replace with actual primary key column
            self.quality.validate_threshold(data, "column_name")  # Replace with actual column name
            
            self.logger.log_info(f"Writing data to target {target}...................")
            self.write_data(data, target, target_format, engine, **write_kwargs)
            self.logger.log_info("Data ingestion completed successfully!")
        except Exception as e:
            self.logger.log_error(f"Data ingestion failed: {e} !")
            raise

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




