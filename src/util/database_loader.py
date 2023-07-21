import json
import os
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine


class DatabaseLoader:
    """
    This class is used to load data into a PostgreSQL database.
    """
    connection_string: str = None
    conn: Connection = None
    engine: Engine = None
    sql_type: bool = False

    # Custom encoder for datetime objects
    class DateTimeEncoder(json.JSONEncoder):
        def default(self, o):
            if isinstance(o, datetime):
                return o.isoformat()
            return super().default(o)

    def __init__(self, connection_string_env_url=None):
        """
        Initialize the DatabaseLoader class with a connection string.
        """
        url_value = os.getenv(connection_string_env_url)
        if url_value is None:
            url_value = connection_string_env_url
        self.connection_string = url_value
        self.load_stats = []

    def connect_sql(self, force_reconnect: bool = False):
        """
        Singleton Connection to the PostgreSQL database.
        """
        if self.conn is not None:
            if not force_reconnect:
                if self.conn.in_transaction():
                    self.conn.rollback()
                return self.conn
            else:
                self.conn.close()

        self.engine = create_engine(self.connection_string)
        self.conn = self.engine.connect()
        return self.conn

    def read_table(self, table_name: str, schema: str = 'public') -> pd.DataFrame:
        """
        Read a table from the PostgreSQL database into a Pandas DataFrame.
        """
        self.connect_sql()
        df = pd.read_sql(text(f"select * from {schema}.{table_name}"), self.conn)
        return df

    def query_to_df(self, query: str):
        """
        Utility function to read a table from the PostgreSQL database into a Pandas DataFrame.
        """
        self.connect_sql()
        result = self.conn.execute(text(query))
        return pd.DataFrame(result.fetchall())

    def query(self, query: str):
        self.connect_sql()
        result = self.conn.execute(text(query))
        return result.fetchall()

    def get_stats(self):
        return self.load_stats

    def load_table(self,
                   df: pd.DataFrame,
                   table_name: str,
                   schema: str = "public",
                   handle_exists='replace',
                   source='not-provided') -> None:
        """
        Load a Pandas DataFrame into a PostgreSQL table.
        Parameters:
            df: Pandas DataFrame to load into the PostgreSQL table.
            table_name: Name of the PostgreSQL table to load the data into.
            schema: Name of the PostgreSQL schema to load the data into.
            handle_exists: How to handle existing data in the table. Options are 'replace', 'append', and 'fail'.
            source: Source of the data being loaded into the table.

            Returns:
                 None
        """

        df['ops_load_timestamp'] = datetime.now()

        self.connect_sql()
        df.to_sql(
            table_name,
            self.engine,
            schema=schema,
            if_exists=handle_exists, index=False)

        n = self.query(f"""select count(*) from {schema}.{table_name}""")
        self.load_stats.append(
            dict(load_date=datetime.now(), table=table_name, records=len(df), source=source, table_count=n[0][0]))

    def bunk_copy_table(self,
                        csv_file: str,
                        table_name: str,
                        dtypes: dict = None,
                        schema: str = "public",
                        handle_exists='replace',
                        source='not-provided') -> None:

        df = pd.read_csv(csv_file, dtype=dtypes)  # quick and dirty verify and count
        records = len(df)
        load_stat = dict(load_time=datetime.now(), table=table_name, records=records, source=source)

        # Establish a connection to the PostgreSQL database
        self.connect_sql()
        pg_conn = self.engine.raw_connection()
        cur = pg_conn.cursor()

        # Build the TRUNCATE statement to delete existing data from the table
        if handle_exists != 'append':
            cur.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}')")
            exists = cur.fetchone()[0]
            if exists == 1:
                truncate_query = f"TRUNCATE {table_name}"
                cur.execute(truncate_query)
            else:
                df.drop(df.index, inplace=True)
                df.to_sql(
                    table_name,
                    self.engine,
                    schema=schema,
                    if_exists=handle_exists, index=False)

        # Build the COPY command to load data from the CSV file
        copy_query = f"COPY {schema}.{table_name} FROM STDIN DELIMITER ',' CSV HEADER"

        # Open the CSV file in read mode
        with open(csv_file, 'r') as f:
            # Execute the COPY command to load the data
            cur.copy_expert(copy_query, f)

        # Commit the changes and close the cursor and connection
        pg_conn.commit()
        cur.close()


