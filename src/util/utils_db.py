import os

from typing import Dict

from pandas import DataFrame

from src import *

from src.util.database_loader import DatabaseLoader
from src.util.utils import assert_and_alert

logger = configure_logging('pbp_logger')


def load_dims_to_db(results: Dict):
    """
    Load dimensions to the database
    Parameters:
        results: dictionary of dataframes
    """
    schema = results.get('schema', 'public')
    loader = DatabaseLoader(get_config('connection_string'))
    for table, data in results.items():
        if isinstance(data, DataFrame):
            logger.info(f"create table {table} in schema {schema}")
            loader.load_table(data, table_name=table, schema=schema)


def validate_positions(df: DataFrame, column_name='position', silent=True):
    """
    Validate that the positions in the dataframe match the positions in the positions table
    Parameters:
        df: dataframe to validate
        column_name: name of the column to validate
        silent: if True, raise an exception if the validation fails
    """
    positions_file = get_config('positions_data')
    positions = pd.read_csv(positions_file)
    index = set(positions.Pos)
    bad_set = set(df.loc[(~df[column_name].isin(index)), column_name].to_list())
    assert_and_alert(len(bad_set) == 0, msg=f"Unknown player positions: {bad_set}", silent=silent)


def store_df(df: DataFrame, file_name: str, db: DatabaseLoader = None, schema: str = 'public'):
    """
    Store a dataframe to a parquet file and optionally to a database table
    Parameters:
        df: dataframe to store
        file_name: name of the file to store
        db: database to store the dataframe in
        schema: schema to store the dataframe in

    if the data_directory does not exist, it will be created
    schema defaults to 'public'
    If db is None, the dataframe will not be stored in the database
    """
    data_directory = get_config('data_directory')
    if not os.path.exists(data_directory):
        os.makedirs(data_directory)
    output_path = os.path.join(data_directory, f"{file_name}.parquet")

    logger.info(f"writing file {file_name}")
    df.to_parquet(output_path, engine='fastparquet', compression='snappy')
    if db is not None:
        logger.info(f"writing table {file_name} in schema {schema}")
        db.load_table(df=df, table_name=file_name, schema=schema)
