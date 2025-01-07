import pandas as pd
from pyspark.sql import SparkSession

class QualityHandler:
    def __init__(self, spark_app_name="QualityApp", spark_master="local[*]"):
        self.spark = SparkSession.builder \
            .appName(spark_app_name) \
            .master(spark_master) \
            .getOrCreate()

    def validate_no_nulls(self, df, column_name):
        if isinstance(df, pd.DataFrame):
            if df[column_name].isnull().any():
                raise ValueError(f"Null values found in column {column_name}")
        else:
            if df.filter(df[column_name].isNull()).count() > 0:
                raise ValueError(f"Null values found in column {column_name}")

    def validate_threshold(self, df, column_name, threshold=0.1):
        if isinstance(df, pd.DataFrame):
            if df[column_name].isnull().mean() > threshold:
                raise ValueError(f"Column {column_name} exceeds null threshold of {threshold}")
        else:
            total_count = df.count()
            null_count = df.filter(df[column_name].isNull()).count()
            if (null_count / total_count) > threshold:
                raise ValueError(f"Column {column_name} exceeds null threshold of {threshold}")

    def validate_schema(self, df, expected_schema):
        if isinstance(df, pd.DataFrame):
            actual_columns = df.columns.tolist()
        else:
            actual_columns = df.columns
        for column in expected_schema:
            if column not in actual_columns:
                raise ValueError(f"Schema mismatch: expected column {column} not found")

    def validate_no_duplicates(self, df, column_name):
        if isinstance(df, pd.DataFrame):
            if df[column_name].duplicated().any():
                raise ValueError(f"Duplicate values found in column {column_name}")
        else:
            if df.groupBy(column_name).count().filter("count > 1").count() > 0:
                raise ValueError(f"Duplicate values found in column {column_name}")

if __name__ == "__main__":
    # Pandas DataFrame example
    #data = {
    #    "id": [1, 2, 3, None],
    #    "calories": [420, 380, 390, 500],
    #    "duration": [50, 40, 45, 60],
    #    "target": [1, 0, 1, 2]
    #}
    #df = pd.DataFrame(data)
    #print(df)
    #quality_handler = QualityHandler()
    #
    #errors = []
    #try:
    #    quality_handler.validate_no_nulls(df, "id")
    #except ValueError as e:
    #    errors.append(str(e))
    #
    #try:
    #    quality_handler.validate_threshold(df, "id")
    #except ValueError as e:
    #    errors.append(str(e))
    #
    #expected_columns = ["id", "calories", "duration", "target"]
    #try:
    #    quality_handler.validate_schema(df, expected_columns)
    #except ValueError as e:
    #    errors.append(str(e))
    #
    #try:
    #    quality_handler.validate_no_duplicates(df, "id")
    #except ValueError as e:
    #    errors.append(str(e))
    #
    #if errors:
    #    for error in errors:
    #        print(error)
    #    raise ValueError("Validation errors occurred.")
    
    # Spark DataFrame example
    spark = SparkSession.builder.appName("QualityApp").master("local[*]").getOrCreate()
    data = [
        (1, 420, 50, 1),
        (2, 380, 40, 0),
        (3, 390, 45, 1),
        (None, 500, 60, 2)
    ]
    columns = ["id", "calories", "duration", "target"]
    spark_df = spark.createDataFrame(data, columns)
    spark_df.show()

    quality_handler = QualityHandler()
    errors = []
    try:
        quality_handler.validate_no_nulls(spark_df, "id")
    except ValueError as e:
        errors.append(str(e))
    
    try:
        quality_handler.validate_threshold(spark_df, "id")
    except ValueError as e:
        errors.append(str(e))
    
    expected_schema = ["id", "calories", "duration", "target"]
    try:
        quality_handler.validate_schema(spark_df, expected_schema)
    except ValueError as e:
        errors.append(str(e))
    
    try:
        quality_handler.validate_no_duplicates(spark_df, "id")
    except ValueError as e:
        errors.append(str(e))
    
    if errors:
        for error in errors:
            print(error)
        raise ValueError("Validation errors occurred.")