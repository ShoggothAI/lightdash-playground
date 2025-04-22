"""
DBT manager class for handling dbt file operations
"""

import os
from pathlib import Path
import pandas as pd


class DbtManager:
    """Class to handle all dbt file operations"""

    def __init__(self, project_root: str, model_relative_path: str):
        """Initialize the dbt manager with project root directory"""
        # Get the directory of the current script if project_root is not provided
        self.project_root = Path(project_root)

        # Define the directory for dbt models
        self.dbt_models_dir = self.project_root / model_relative_path

        # Ensure the directory exists
        os.makedirs(self.dbt_models_dir, exist_ok=True)

    def create_sql_file(self, df: pd.DataFrame, file_name: str):
        """Create a SQL file for dbt to access the time_series_data table if it doesn't exist

        Args:
            df: The pandas DataFrame containing the data, used to get column names
        """
        try:
            # Define the file path
            sql_file_path = self.dbt_models_dir / file_name

            # Check if the file already exists
            if sql_file_path.exists():
                print(f"\nSQL file already exists at: {sql_file_path}")
                return True

            # Generate SQL select statements dynamically from DataFrame columns
            select_statements = []
            for col in df.columns:
                # Convert column name to lowercase for the alias
                alias = col.lower()
                select_statements.append(f'    "{col}" as {alias}')

            # Join all select statements with commas
            select_clause = ",\n".join(select_statements)

            # SQL content - a simple SELECT statement
            sql_content = f"""-- {file_name}
-- This model selects data from the time_series_data table in the lightdash_data database

{{{{ config(
    materialized='table'
) }}}}

SELECT
{select_clause}
FROM {{{{ source('lightdash_data', 'time_series_data') }}}}
"""

            # Write the SQL file
            with open(sql_file_path, "w") as f:
                f.write(sql_content)

            print(f"\nCreated dbt SQL file at: {sql_file_path}")
            return True
        except Exception as e:
            print(f"Error creating dbt SQL file: {e}")
            return False

    def create_schema_file(self, df):
        """Create schema.yml and sources.yml files for dbt if they don't exist

        Args:
            df: The pandas DataFrame containing the data, used to get column names
        """
        try:
            # Define the file paths
            schema_file_path = self.dbt_models_dir / "schema.yml"
            sources_file_path = self.dbt_models_dir / "sources.yml"

            # Check if sources.yml already exists
            sources_exists = sources_file_path.exists()
            if not sources_exists:
                # Create sources.yml file to define the source
                sources_content = """version: 2

sources:
  - name: lightdash_data
    database: lightdash_data
    schema: public
    tables:
      - name: time_series_data
        description: "Time series data for wise pizza analysis"
"""

                with open(sources_file_path, "w") as f:
                    f.write(sources_content)

                print(f"\nCreated dbt sources file at: {sources_file_path}")
            else:
                print(f"\nSources file already exists at: {sources_file_path}")

            # Check if schema.yml already exists
            schema_exists = schema_file_path.exists()
            if not schema_exists:
                # Generate column definitions dynamically from DataFrame
                column_definitions = []

                # Dictionary of column descriptions based on common naming patterns
                # This can be expanded with more column types and descriptions
                description_map = {
                    "transaction_date": "The date of the record",
                    "date": "The date of the record",
                    "product": "The product type",
                    "region": "The geographical region",
                    "source_currency": "The source currency code",
                    "target_currency": "The target currency code",
                    "volume": "The transaction volume",
                    "active_customers": "Number of active customers",
                }

                # Add data tests for numeric columns
                numeric_columns = df.select_dtypes(
                    include=["number"]
                ).columns.tolist()

                for col in df.columns:
                    # Convert column name to lowercase for schema
                    col_name = col.lower()

                    # Get description from map or use a default
                    description = description_map.get(
                        col_name, f"The {col_name} value"
                    )

                    # Basic column definition
                    col_def = f'      - name: {col_name}\n        description: "{description}"'

                    # Add data tests for numeric columns
                    if col in numeric_columns:
                        col_def += "\n        data_tests:\n          - not_null"

                    column_definitions.append(col_def)

                # Join all column definitions
                columns_section = "\n".join(column_definitions)

                # Schema content
                schema_content = f"""version: 2

models:
  - name: time_series_data
    description: "Time series data for wise pizza analysis"
    columns:
{columns_section}
"""

                # Write the schema file
                with open(schema_file_path, "w") as f:
                    f.write(schema_content)

                print(f"Created dbt schema file at: {schema_file_path}")
            else:
                print(f"Schema file already exists at: {schema_file_path}")

            return True
        except Exception as e:
            print(f"Error creating dbt schema file: {e}")
            return False
