"""
Database manager class for handling database operations
"""

import sys
import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import create_engine


class DatabaseManager:
    """Class to handle all database operations"""

    def __init__(
        self,
        host="localhost",
        port="5432",
        user="postgres",
        password="mysecretpassword",
        db_name="lightdash_data",
    ):
        """Initialize the database manager with connection parameters"""
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.db_name = db_name

    def _execute_postgres_command(
        self, command, params=None, fetch_result=False
    ):
        """Execute a command on the postgres database

        Args:
            command: SQL command to execute
            params: Parameters for the SQL command (optional)
            fetch_result: Whether to fetch and return a result (optional)

        Returns:
            Result of the command if fetch_result is True, otherwise True on success
        """
        try:
            # Connect to the default postgres database
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.db_name,
            )
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = conn.cursor()

            # Execute the command
            cursor.execute(command, params)

            # Fetch result if needed
            result = None
            if fetch_result:
                result = cursor.fetchone()

            cursor.close()
            conn.close()

            return result if fetch_result else True
        except Exception as e:
            print(f"Error executing database command: {e}")
            return None if fetch_result else False

    def check_database_exists(self):
        """Check if the database already exists"""
        try:
            # Check if our database exists
            result = self._execute_postgres_command(
                "SELECT 1 FROM pg_database WHERE datname = %s",
                (self.db_name,),
                fetch_result=True,
            )
            exists = result is not None
            return exists
        except Exception as e:
            print(f"Error checking if database exists: {e}")
            sys.exit(1)

    def drop_database(self):
        """Drop the existing database"""
        try:
            # Drop the database
            success = self._execute_postgres_command(
                sql.SQL("DROP DATABASE {}").format(sql.Identifier(self.db_name))
            )

            if success:
                print(f"Database '{self.db_name}' dropped successfully.")
            return success
        except Exception as e:
            print(f"Error dropping database: {e}")
            return False

    def create_database(self):
        """Create a new database if it doesn't already exist"""
        # First check if the database already exists
        if self.check_database_exists():
            print(f"Database '{self.db_name}' already exists.")
            return True

        try:
            # Create the database
            success = self._execute_postgres_command(
                sql.SQL("CREATE DATABASE {}").format(
                    sql.Identifier(self.db_name)
                )
            )

            if success:
                print(f"Database '{self.db_name}' created successfully.")
            return success
        except Exception as e:
            print(f"Error creating database: {e}")
            return False

    def create_table_and_load_data(self, df):
        """Create a table and load the CSV data into it"""
        try:
            # Create SQLAlchemy engine
            engine = create_engine(
                f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db_name}"
            )

            # Create table and load data
            df.to_sql(
                "time_series_data", engine, if_exists="replace", index=False
            )

            print(f"Data loaded successfully into 'time_series_data' table.")
            return True
        except Exception as e:
            print(f"Error creating table and loading data: {e}")
            return False

    def query_data(self, limit: int = 5):
        """Query the data back as a dataframe"""
        try:
            # Create SQLAlchemy engine
            engine = create_engine(
                f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db_name}"
            )

            # Query the data
            query = "SELECT * FROM time_series_data"
            if limit is not None:
                query += f" LIMIT {limit}"
            df = pd.read_sql(query, engine)

            print("\nQueried data from database:")
            print(df)

            return df
        except Exception as e:
            print(f"Error querying data: {e}")
            return None
