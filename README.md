# siamflowetl

## Overview
`siamflowetl` is a Python package for data ingestion and quality validation using both Pandas and Spark DataFrames.

## Installation
To install the package, use the following command:
```sh
pip install siamflowetl
```

## Usage

### IngestionHandler
The `IngestionHandler` class is used to read and write data in various formats using Pandas or Spark.

#### Example: Reading and Writing Data
```python
from siamflowetl.ingestion import IngestionHandler

# Initialize the ingestion handler
ingestion = IngestionHandler()

# Read data from a CSV file using Pandas
data = ingestion.read_data(source="data/input.csv", source_format="csv", engine="pandas")

# Write data to a Parquet file using Pandas
ingestion.write_data(data, target="data/output.parquet", target_format="parquet", engine="pandas")
```

### QualityHandler
The `QualityHandler` class is used to validate data quality for both Pandas and Spark DataFrames.

#### Example: Validating Data Quality
```python
from siamflowetl.quality import QualityHandler
import pandas as pd

# Initialize the quality handler
quality_handler = QualityHandler()

# Create a sample Pandas DataFrame
data = {
    "id": [1, 2, 3, None],
    "calories": [420, 380, 390, 500],
    "duration": [50, 40, 45, 60],
    "target": [1, 0, 1, 2]
}
df = pd.DataFrame(data)

# Validate data quality
try:
    quality_handler.validate_no_nulls(df, "id")
    quality_handler.validate_threshold(df, "id")
    expected_schema = {"id": "float64", "calories": "int64", "duration": "int64", "target": "int64"}
    quality_handler.validate_schema(df, expected_schema)
    quality_handler.validate_no_duplicates(df, "id")
    print("Data validation passed.")
except ValueError as e:
    print(f"Data validation failed: {e}")
```

#### Example: Validating Spark DataFrame Quality
```python
from siamflowetl.quality import QualityHandler
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("QualityApp").master("local[*]").getOrCreate()

# Create a sample Spark DataFrame
data = [
    (1, 420, 50, 1),
    (2, 380, 40, 0),
    (3, 390, 45, 1),
    (None, 500, 60, 2)
]
columns = ["id", "calories", "duration", "target"]
spark_df = spark.createDataFrame(data, columns)

# Initialize the quality handler
quality_handler = QualityHandler()

# Validate data quality
errors = []
try:
    quality_handler.validate_no_nulls(spark_df, "id")
except ValueError as e:
    errors.append(str(e))

try:
    quality_handler.validate_threshold(spark_df, "id")
except ValueError as e:
    errors.append(str(e))

expected_schema = {"id": "bigint", "calories": "bigint", "duration": "bigint", "target": "bigint"}
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
else:
    print("Data validation passed.")
```

### DimensionalModelSuggester
The `DimensionalModelSuggester` class is used to suggest Fact and Dimension tables based on Kimball's dimensional modeling theory.

#### Example: Suggesting Dimensional Model
```python
from siamflowetl.suggestdimfact import DimensionalModelSuggester

# Define the schema
schema = {
    "sales": [
        {"name": "sales_id", "type": "int", "constraints": "primary key"},
        {"name": "product_id", "type": "int"},
        {"name": "quantity", "type": "int"},
        {"name": "sales_date", "type": "date"}
    ],
    "products": [
        {"name": "product_id", "type": "int", "constraints": "primary key"},
        {"name": "product_name", "type": "varchar"},
        {"name": "category", "type": "varchar"}
    ]
}

# Initialize the suggester
suggester = DimensionalModelSuggester(scd_type="SCD2")

# Get the suggested dimensional model
model = suggester.suggest_dimensional_model(schema)
print(model)
```

#### Example: Generating ER Diagram
```python
# Generate ER diagram
suggester.generate_er_diagram("er_diagram.png")
```

#### Example: Generating SQL Scripts
```python
# Generate SQL scripts
suggester.generate_sql_scripts("dimensional_model.sql")
```

## License
This project is licensed under the MIT License.
