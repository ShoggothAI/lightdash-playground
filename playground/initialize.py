"""
Start a postgres instance by running
docker run --name motley-postgres -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=mysecretpassword -p 5432:5432 -d pgvector/pgvector:pg17
"""

import sys
import os
import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import create_engine
from pathlib import Path

# Database configuration
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_USER = 'postgres'
DB_PASSWORD = 'mysecretpassword'
DB_NAME = 'lightdash_data'

def get_csv_data():
    """Download the CSV file from GitHub directly into a pandas DataFrame"""
    # The raw URL for the CSV file
    url = 'https://raw.githubusercontent.com/transferwise/wise-pizza/main/data/synth_time_data.csv'

    print(f"Downloading CSV from {url}...")
    try:
        # Read CSV directly into pandas DataFrame
        df = pd.read_csv(url)
        print("CSV data downloaded successfully.")

        # Display information about the data
        print("\nCSV Column Names:")
        for col in df.columns:
            print(f"- {col}")

        print(f"\nTotal rows: {len(df)}")

        # Display sample data
        print("\nSample data (first 5 rows):")
        print(df.head())

        return df
    except Exception as e:
        print(f"Failed to download CSV: {e}")
        return None

def check_database_exists():
    """Check if the database already exists"""
    try:
        # Connect to the default postgres database
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database='postgres'
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()

        # Check if our database exists
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DB_NAME,))
        exists = cursor.fetchone() is not None

        cursor.close()
        conn.close()

        return exists
    except Exception as e:
        print(f"Error checking if database exists: {e}")
        sys.exit(1)

def drop_database():
    """Drop the existing database"""
    try:
        # Connect to the default postgres database
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database='postgres'
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()

        # Drop the database
        cursor.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(DB_NAME)))

        cursor.close()
        conn.close()

        print(f"Database '{DB_NAME}' dropped successfully.")
        return True
    except Exception as e:
        print(f"Error dropping database: {e}")
        return False

def create_database():
    """Create a new database"""
    try:
        # Connect to the default postgres database
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database='postgres'
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()

        # Create the database
        cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(DB_NAME)))

        cursor.close()
        conn.close()

        print(f"Database '{DB_NAME}' created successfully.")
        return True
    except Exception as e:
        print(f"Error creating database: {e}")
        return False

def create_table_and_load_data(df):
    """Create a table and load the CSV data into it"""
    try:
        # Create SQLAlchemy engine
        engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

        # Create table and load data
        df.to_sql('time_series_data', engine, if_exists='replace', index=False)

        print(f"Data loaded successfully into 'time_series_data' table.")
        return True
    except Exception as e:
        print(f"Error creating table and loading data: {e}")
        return False

def query_data():
    """Query the data back as a dataframe"""
    try:
        # Create SQLAlchemy engine
        engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

        # Query the data
        query = "SELECT * FROM time_series_data LIMIT 5"
        df = pd.read_sql(query, engine)

        print("\nQueried data from database:")
        print(df)

        return df
    except Exception as e:
        print(f"Error querying data: {e}")
        return None

def create_dbt_sql_file():
    """Create a SQL file for dbt to access the time_series_data table if it doesn't exist"""
    try:
        # Get the directory of the current script
        script_dir = Path(__file__).parent.absolute()

        # Define the directory and file path relative to the script location
        dbt_models_dir = script_dir.parent / 'my_lightdash_project' / 'models' / 'wise_pizza'
        sql_file_path = dbt_models_dir / 'time_series_data.sql'

        # Ensure the directory exists
        os.makedirs(dbt_models_dir, exist_ok=True)

        # Check if the file already exists
        if sql_file_path.exists():
            print(f"\nSQL file already exists at: {sql_file_path}")
            return True

        # SQL content - a simple SELECT statement
        sql_content = """-- time_series_data.sql
-- This model selects data from the time_series_data table in the lightdash_data database

{{ config(
    materialized='table'
) }}

SELECT
    "DATE" as date,
    "PRODUCT" as product,
    "REGION" as region,
    "SOURCE_CURRENCY" as source_currency,
    "TARGET_CURRENCY" as target_currency,
    "VOLUME" as volume,
    "ACTIVE_CUSTOMERS" as active_customers
FROM {{ source('lightdash_data', 'time_series_data') }}
"""

        # Write the SQL file
        with open(sql_file_path, 'w') as f:
            f.write(sql_content)

        print(f"\nCreated dbt SQL file at: {sql_file_path}")
        return True
    except Exception as e:
        print(f"Error creating dbt SQL file: {e}")
        return False

def create_dbt_schema_file():
    """Create schema.yml and sources.yml files for dbt if they don't exist"""
    try:
        # Get the directory of the current script
        script_dir = Path(__file__).parent.absolute()

        # Define the directory and file paths relative to the script location
        dbt_models_dir = script_dir.parent / 'my_lightdash_project' / 'models' / 'wise_pizza'
        schema_file_path = dbt_models_dir / 'schema.yml'
        sources_file_path = dbt_models_dir / 'sources.yml'

        # Ensure the directory exists
        os.makedirs(dbt_models_dir, exist_ok=True)

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

            with open(sources_file_path, 'w') as f:
                f.write(sources_content)

            print(f"\nCreated dbt sources file at: {sources_file_path}")
        else:
            print(f"\nSources file already exists at: {sources_file_path}")

        # Check if schema.yml already exists
        schema_exists = schema_file_path.exists()
        if not schema_exists:
            # Schema content
            schema_content = """version: 2

models:
  - name: time_series_data
    description: "Time series data for wise pizza analysis"
    columns:
      - name: date
        description: "The date of the record"
      - name: product
        description: "The product type"
      - name: region
        description: "The geographical region"
      - name: source_currency
        description: "The source currency code"
      - name: target_currency
        description: "The target currency code"
      - name: volume
        description: "The transaction volume"
        data_tests:
          - not_null
      - name: active_customers
        description: "Number of active customers"
        data_tests:
          - not_null
"""

            # Write the schema file
            with open(schema_file_path, 'w') as f:
                f.write(schema_content)

            print(f"Created dbt schema file at: {schema_file_path}")
        else:
            print(f"Schema file already exists at: {schema_file_path}")

        return True
    except Exception as e:
        print(f"Error creating dbt schema file: {e}")
        return False

def main():
    # Download CSV data directly into a pandas DataFrame
    df = get_csv_data()
    if df is None:
        print("Failed to load CSV data. Exiting.")
        return

    # Check if the database already exists
    if check_database_exists():
        print(f"WARNING: Database '{DB_NAME}' already exists. It will be dropped and recreated.")
        if not drop_database():
            print("Failed to drop the database. Exiting.")
            return

    # Create a new database
    if not create_database():
        print("Failed to create the database. Exiting.")
        return

    # Create table and load data
    if not create_table_and_load_data(df):
        print("Failed to load data into the database. Exiting.")
        return

    # Query the data back
    query_data()

    # Create dbt files
    create_dbt_sql_file()
    create_dbt_schema_file()

    print("\nyay! Database setup completed successfully.")

if __name__ == "__main__":
    main()