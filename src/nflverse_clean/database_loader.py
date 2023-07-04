import os

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine


class DatabaseLoader:
    connection_string: str = None
    conn: Connection = None
    engine: Engine = None
    sql_type: bool = False

    def __init__(self, connection_string_env_url=None):

        url_value = os.getenv(connection_string_env_url)
        if url_value is None:
            url_value = connection_string_env_url
        self.connection_string = url_value

    def connect_sql(self, force_reconnect: bool = False):
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

    def read_table(self, table_name: str) -> pd.DataFrame:
        self.connect_sql()
        dataFrame = pd.read_sql(table_name, self.conn)
        return dataFrame

    def query_to_df(self, query: str):
        self.connect_sql()
        result = self.conn.execute(text(query))
        return pd.DataFrame(result.fetchall())

    def query(self, query: str):
        self.connect_sql()
        result = self.conn.execute(text(query))
        return result.fetchall()

    def load_table(self,
                   df: pd.DataFrame,
                   table_name: str,
                   schema: str = "public",
                   handle_exists='replace') -> None:

        self.connect_sql()
        df.to_sql(
            table_name,
            self.engine,
            schema=schema,
            if_exists=handle_exists, index=False)


