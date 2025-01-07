# siamflowetl

## Inspiration

สำหรับ python package นีถูกออกแบบโดยผู้มีความรักในการทำงานด้านข้อมูลและจากประสบการณ์ตรงเมื่อปี 2019 ผู้พัฒนาต้องการย้ายสายงานแต่ขาดประสบการณ์ทางด้านการพัฒนาโปรแกรมด้านข้อมูล ซึ่งหาความรู้ช่วงนั้นยากมากประกอบกับ ความไม่รู้ในการพัฒนา data pipeline/ETL job ดังนั้นผู้พัฒนาจึงเล็งเห็นว่าในปี 2025 เป็นช่วงเวลาที่ดีในการสร้างหรอแชร์ นวัตกรรม หรือ Innovation โดยคนไทย สำหรับคนไทย โดยเฉพาะอย่างยิ่ง ผู้เริ่มต้นในสายงาน data engineer / ETL developer หรือผู้ที่สนใจและผู้ที่มีความต้องการเดียวกันในการผลักดันสังคมไทยให้ดีขึ้น 

## Overview
`siamflowetl` is a Python package for data ingestion and quality validation using both Pandas and Spark DataFrames.

## ภาพรวม
`siamflowetl` เป็นแพ็กเกจ Python ที่ออกแบบโดยคนไทยสำหรับการนำเข้าข้อมูลและการตรวจสอบคุณภาพข้อมูลโดยใช้ทั้ง Pandas และ Spark DataFrames

## Installation
To install the package, use the following command:
```sh
pip install siamflowetl
```

## การติดตั้ง
ในการติดตั้งแพ็กเกจ โปรดทำตามคำสั่งต่อไปนี้:
```sh
pip install siamflowetl
```

## Usage

### IngestionHandler
The `IngestionHandler` class is used to read and write data in various formats using Pandas or Spark.

**Note:** In the initial version of the package, there is no option to specify the schema manually (like `StructType`), and it does not support the Delta format yet. These features will be developed in version 2.0 of the `siamflowetl` package.

### IngestionHandler
คลาส `IngestionHandler` ใช้สำหรับอ่านและเขียนข้อมูลในรูปแบบต่างๆ โดยใช้ Pandas หรือ Spark

**หมายเหตุ:** ในเวอร์ชันเริ่มต้นของแพ็กเกจนี้ผู้พัฒนายังคงออกแบบมาอย่างง่าย ไม่มีตัวเลือกในการระบุสคีมาแบบกำหนดเอง (เช่น `StructType`) และยังไม่รองรับรูปแบบ Delta ฟีเจอร์เหล่านี้จะถูกพัฒนาในเวอร์ชัน 2.0 ของแพ็กเกจ `siamflowetl`

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

#### ตัวอย่าง: การอ่านและเขียนข้อมูล
```python
from siamflowetl.ingestion import IngestionHandler

# เริ่มต้นการใช้งาน ingestion handler
ingestion = IngestionHandler()

# อ่านข้อมูลจากไฟล์ CSV โดยใช้ Pandas
data = ingestion.read_data(source="data/input.csv", source_format="csv", engine="pandas")

# เขียนข้อมูลไปยังไฟล์ Parquet โดยใช้ Pandas
ingestion.write_data(data, target="data/output.parquet", target_format="parquet", engine="pandas")
```

### QualityHandler
The `QualityHandler` class is used to validate data quality for both Pandas and Spark DataFrames.

### QualityHandler
คลาส `QualityHandler` ใช้สำหรับตรวจสอบคุณภาพข้อมูลสำหรับทั้ง Pandas และ Spark DataFrames

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

#### ตัวอย่าง: การตรวจสอบคุณภาพข้อมูล
```python
from siamflowetl.quality import QualityHandler
import pandas as pd

# เริ่มต้นการใช้งาน quality handler
quality_handler = QualityHandler()

# สร้างตัวอย่าง Pandas DataFrame
data = {
    "id": [1, 2, 3, None],
    "calories": [420, 380, 390, 500],
    "duration": [50, 40, 45, 60],
    "target": [1, 0, 1, 2]
}
df = pd.DataFrame(data)

# ตรวจสอบคุณภาพข้อมูล
try:
    quality_handler.validate_no_nulls(df, "id")
    quality_handler.validate_threshold(df, "id")
    expected_schema = {"id": "float64", "calories": "int64", "duration": "int64", "target": "int64"}
    quality_handler.validate_schema(df, expected_schema)
    quality_handler.validate_no_duplicates(df, "id")
    print("การตรวจสอบคุณภาพข้อมูลผ่าน")
except ValueError as e:
    print(f"การตรวจสอบคุณภาพข้อมูลล้มเหลว: {e}")
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

#### ตัวอย่าง: การตรวจสอบคุณภาพข้อมูล Spark DataFrame
```python
from siamflowetl.quality import QualityHandler
from pyspark.sql import SparkSession

# เริ่มต้นการใช้งาน Spark session
spark = SparkSession.builder.appName("QualityApp").master("local[*]").getOrCreate()

# สร้างตัวอย่าง Spark DataFrame
data = [
    (1, 420, 50, 1),
    (2, 380, 40, 0),
    (3, 390, 45, 1),
    (None, 500, 60, 2)
]
columns = ["id", "calories", "duration", "target"]
spark_df = spark.createDataFrame(data, columns)

# เริ่มต้นการใช้งาน quality handler
quality_handler = QualityHandler()

# ตรวจสอบคุณภาพข้อมูล
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
    print("การตรวจสอบคุณภาพข้อมูลผ่าน")
```

### DimensionalModelSuggester
The `DimensionalModelSuggester` class is used to suggest Fact and Dimension tables based on Kimball's dimensional modeling theory.

### DimensionalModelSuggester
คลาส `DimensionalModelSuggester` ใช้สำหรับแนะนำตาราง Fact และ Dimension ตามทฤษฎีการสร้างโมเดลเชิงมิติของ Kimball

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

#### ตัวอย่าง: การแนะนำโมเดลเชิงมิติ
```python
from siamflowetl.suggestdimfact import DimensionalModelSuggester

# กำหนด schema
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

# เริ่มต้นการใช้งาน suggester
suggester = DimensionalModelSuggester(scd_type="SCD2")

# รับโมเดลเชิงมิติที่แนะนำ
model = suggester.suggest_dimensional_model(schema)
print(model)
```

#### Example: Generating ER Diagram
```python
# Generate ER diagram
suggester.generate_er_diagram("er_diagram.png")
```

![ER Diagram](er_diagram.png)

#### ตัวอย่าง: การสร้าง ER Diagram
```python
# สร้าง ER diagram
suggester.generate_er_diagram("er_diagram.png")
```

#### Example: Generating SQL Scripts
```python
# Generate SQL scripts
suggester.generate_sql_scripts("dimensional_model.sql")
```

#### ตัวอย่าง: การสร้าง SQL Scripts
```python
# สร้าง SQL scripts
suggester.generate_sql_scripts("dimensional_model.sql")
```

#### Result: Fact and Dimension Tables
```json
{
    "Fact Tables": [
        {
            "table": "sales",
            "metrics": ["quantity"],
            "foreign_keys": ["product_id"]
        }
    ],
    "Dimension Tables": [
        {
            "table": "products",
            "attributes": ["product_name", "category"],
            "time_attributes": [],
            "surrogate_key": "products_sk",
            "start_date": "valid_from",
            "end_date": "valid_to",
            "current_flag": "is_current"
        }
    ],
    "SCD Type": "SCD2"
}
```

#### ผลลัพธ์: ตาราง Fact และ Dimension
```json
{
    "Fact Tables": [
        {
            "table": "sales",
            "metrics": ["quantity"],
            "foreign_keys": ["product_id"]
        }
    ],
    "Dimension Tables": [
        {
            "table": "products",
            "attributes": ["product_name", "category"],
            "time_attributes": [],
            "surrogate_key": "products_sk",
            "start_date": "valid_from",
            "end_date": "valid_to",
            "current_flag": "is_current"
        }
    ],
    "SCD Type": "SCD2"
}
```

## License
This project is licensed under the MIT License.

## ใบอนุญาต
โครงการนี้ได้รับอนุญาตภายใต้ใบอนุญาต MIT
