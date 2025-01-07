import eralchemy
from sqlalchemy import Table, Column, Integer, String, MetaData, Date, Boolean, create_engine

class DimensionalModelSuggester:
    def __init__(self, scd_type="SCD2"):
        self.scd_type = scd_type
        self.suggested_model = None

    def suggest_dimensional_model(self, schema):
        """
        Suggest Fact and Dimension tables based on Kimball's dimensional modeling theory, 
        with support for surrogate keys and Slowly Changing Dimensions (SCD).

        Parameters:
        schema (dict): A dictionary of table schemas.
                       Format: {table_name: [{"name": column_name, "type": column_type}, ...]}

        Returns:
        dict: Suggested Fact and Dimension tables with SCD and surrogate key details.
        """
        fact_tables = []
        dimension_tables = []

        for table, columns in schema.items():
            # Identify columns by type
            numeric_columns = [col["name"] for col in columns if "int" in str(col["type"]).lower() or "float" in str(col["type"]).lower()]
            textual_columns = [col["name"] for col in columns if "char" in str(col["type"]).lower() or "text" in str(col["type"]).lower()]
            date_columns = [col["name"] for col in columns if "date" in str(col["type"]).lower() or "time" in str(col["type"]).lower()]

            primary_keys = [col["name"] for col in columns if "id" in col["name"].lower() and "key" in col.get("constraints", "")]
            foreign_keys = [col["name"] for col in columns if "id" in col["name"].lower() and col["name"] not in primary_keys]

            # Classify Fact Tables
            if numeric_columns and foreign_keys:
                fact_tables.append({
                    "table": table,
                    "metrics": numeric_columns,
                    "foreign_keys": foreign_keys
                })

            # Classify Dimension Tables
            elif textual_columns or date_columns:
                dimension_table = {
                    "table": table,
                    "attributes": textual_columns,
                    "time_attributes": date_columns,
                    "surrogate_key": f"{table}_sk",
                }

                # Add SCD handling for dimensions
                if self.scd_type.upper() == "SCD2":
                    dimension_table["start_date"] = "valid_from"
                    dimension_table["end_date"] = "valid_to"
                    dimension_table["current_flag"] = "is_current"

                dimension_tables.append(dimension_table)

        # Return Dimensional Model
        self.suggested_model = {
            "Fact Tables": fact_tables,
            "Dimension Tables": dimension_tables,
            "SCD Type": self.scd_type.upper()
        }
        return self.suggested_model

    def generate_er_diagram(self, output_file="er_diagram.png"):
        """
        Generate an ER diagram for the suggested dimensional model.

        Parameters:
        output_file (str): The file path to save the ER diagram.
        """
        metadata = MetaData()
        tables = []

        for fact in self.suggested_model["Fact Tables"]:
            columns = []
            for fk in fact["foreign_keys"]:
                if fk not in [col.name for col in columns]:
                    columns.append(Column(fk, Integer))
            for metric in fact["metrics"]:
                if metric not in [col.name for col in columns]:
                    columns.append(Column(metric, Integer))
            table = Table(fact["table"], metadata, *columns)
            tables.append(table)

        for dim in self.suggested_model["Dimension Tables"]:
            columns = []
            for attr in dim["attributes"]:
                if attr not in [col.name for col in columns]:
                    columns.append(Column(attr, String))
            for time_attr in dim["time_attributes"]:
                if time_attr not in [col.name for col in columns]:
                    columns.append(Column(time_attr, Date))
            columns.append(Column(dim["surrogate_key"], Integer, primary_key=True))
            if self.scd_type.upper() == "SCD2":
                columns.append(Column("valid_from", Date))
                columns.append(Column("valid_to", Date))
                columns.append(Column("is_current", Boolean))
            table = Table(dim["table"], metadata, *columns)
            tables.append(table)

        eralchemy.render_er(metadata, output_file)

    def generate_sql_scripts(self, output_file="dimensional_model.sql"):
        """
        Generate SQL scripts for the suggested dimensional model.

        Parameters:
        output_file (str): The file path to save the SQL scripts.
        """
        metadata = MetaData()
        tables = []

        for fact in self.suggested_model["Fact Tables"]:
            columns = []
            for fk in fact["foreign_keys"]:
                if fk not in [col.name for col in columns]:
                    columns.append(Column(fk, Integer))
            for metric in fact["metrics"]:
                if metric not in [col.name for col in columns]:
                    columns.append(Column(metric, Integer))
            table = Table(fact["table"], metadata, *columns)
            tables.append(table)

        for dim in self.suggested_model["Dimension Tables"]:
            columns = []
            for attr in dim["attributes"]:
                if attr not in [col.name for col in columns]:
                    columns.append(Column(attr, String))
            for time_attr in dim["time_attributes"]:
                if time_attr not in [col.name for col in columns]:
                    columns.append(Column(time_attr, Date))
            columns.append(Column(dim["surrogate_key"], Integer, primary_key=True))
            if self.scd_type.upper() == "SCD2":
                columns.append(Column("valid_from", Date))
                columns.append(Column("valid_to", Date))
                columns.append(Column("is_current", Boolean))
            table = Table(dim["table"], metadata, *columns)
            tables.append(table)

        engine = create_engine('sqlite:///:memory:')
        metadata.create_all(engine)
        with open(output_file, 'w') as f:
            for table in tables:
                f.write(str(table.compile(engine)) + ";\n")

if __name__ == "__main__":
    schema = {
        "sales": [
            {"name": "sales_id", "type": "int", "constraints": "primary key"},
            {"name": "seller_id", "type": "int"},
            {"name": "quantity", "type": "int"},
            {"name": "sales_date", "type": "date"}
        ],
        "products": [
            {"name": "product_id", "type": "int", "constraints": "primary key"},
            {"name": "product_name", "type": "varchar"},
            {"name": "product_category", "type": "varchar"}
        ],
        "stock": [
            {"name": "stock_id", "type": "int", "constraints": "primary key"},
            {"name": "branch", "type": "varchar"},
            {"name": "stock_value", "type": "int"}
        ]
    }

    suggester = DimensionalModelSuggester(scd_type="SCD2")
    model = suggester.suggest_dimensional_model(schema)
    print(model)
    suggester.generate_er_diagram("er_diagram.png")
    suggester.generate_sql_scripts("dimensional_model.sql")
