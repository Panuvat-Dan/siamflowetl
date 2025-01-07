def suggest_dimensional_model(self, schema, scd_type="SCD2"):
    """
    Suggest Fact and Dimension tables based on Kimball's dimensional modeling theory, 
    with support for surrogate keys and Slowly Changing Dimensions (SCD).

    Parameters:
    schema (dict): A dictionary of table schemas.
                   Format: {table_name: [{"name": column_name, "type": column_type}, ...]}
    scd_type (str): Type of Slowly Changing Dimensions. 
                    Options: "SCD1" (overwrite) or "SCD2" (track history).

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
            if scd_type.upper() == "SCD2":
                dimension_table["start_date"] = "valid_from"
                dimension_table["end_date"] = "valid_to"
                dimension_table["current_flag"] = "is_current"

            dimension_tables.append(dimension_table)

    # Return Dimensional Model
    return {
        "Fact Tables": fact_tables,
        "Dimension Tables": dimension_tables,
        "SCD Type": scd_type.upper()
    }
