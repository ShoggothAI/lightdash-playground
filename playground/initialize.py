import os.path

import pandas as pd
from database_manager import DatabaseManager
from dbt_manager import DbtManager


def get_csv_data():
    """Download the CSV file from GitHub directly into a pandas DataFrame"""
    # The raw URL for the CSV file
    url = "https://raw.githubusercontent.com/transferwise/wise-pizza/main/data/synth_time_data.csv"

    print(f"Downloading CSV from {url}...")
    try:
        # Read CSV directly into pandas DataFrame
        df = pd.read_csv(url)

        # Rename DATE column to TRANSACTION_DATE if it exists
        if "DATE" in df.columns:
            df = df.rename(columns={"DATE": "TRANSACTION_DATE"})
            print("Renamed 'DATE' column to 'TRANSACTION_DATE' in the dataset.")

        print("CSV data downloaded successfully.")

        # Display information about the data
        print("\nCSV Column Names:")
        for col in df.columns:
            print(f"- {col}")

        print(f"\nTotal rows: {len(df)}")

        # Display sample data
        print("\nSample data (first 5 rows):")
        print(df.head())

        # Convert all column names to lowercase
        df.columns = df.columns.str.lower()
        print("\nConverted all column names to lowercase:")
        for col in df.columns:
            print(f"- {col}")

        return df
    except Exception as e:
        print(f"Failed to download CSV: {e}")
        return None


def main():
    # Download CSV data directly into a pandas DataFrame
    df = get_csv_data()
    if df is None:
        print("Failed to load CSV data. Exiting.")
        return

    # Initialize the database manager
    db_manager = DatabaseManager(
        host="localhost",
        port="5432",
        user="postgres",
        password="mysecretpassword",
        db_name="lightdash_data",
    )

    # Create a new database
    if not db_manager.create_database():
        print("Failed to create the database. Exiting.")
        return

    # Create table and load data
    if not db_manager.create_table_and_load_data(df):
        print("Failed to load data into the database. Exiting.")
        return

    # Query the data back
    db_manager.query_data()

    # Initialize the dbt manager
    dbt_manager = DbtManager(
        os.path.realpath(os.path.join(os.path.dirname(__file__), "..")),
        "my_dbt_project/models/wise_pizza",
    )

    # Create dbt files with dynamic column information from the DataFrame
    dbt_manager.create_sql_file(df, "time_series_data.sql")
    dbt_manager.create_schema_file(df)

    print("\nyay! Database setup completed successfully.")


if __name__ == "__main__":
    main()
