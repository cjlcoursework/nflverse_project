import os

from typing import Dict

from pandas import DataFrame

from src import *
from src.utils import assert_and_alert

logger = configure_logging('pbp_logger')


def load_dims_to_db(results: Dict):
    schema = results.get('schema', 'public')
    loader = DatabaseLoader(get_config('connection_string'))
    for table, data in results.items():
        if isinstance(data, DataFrame):
            logger.info(f"create table {table} in schema {schema}")
            loader.load_table(data, table_name=table, schema=schema)


def validate_positions(df: DataFrame, column_name='position', silent=True):
    positions_file = get_config('positions_data')
    positions = pd.read_csv(positions_file)
    index = set(positions.Pos)
    bad_set = set(df.loc[(~df[column_name].isin(index)), column_name].to_list())
    assert_and_alert(len(bad_set) == 0, msg=f"Unknown player positions: {bad_set}", silent= silent)


def store_df(df, file_name, db=None, schema='public'):

    data_directory = get_config('data_directory')
    if not os.path.exists(data_directory):
        os.makedirs(data_directory)
    output_path = os.path.join(data_directory, f"{file_name}.parquet")

    logger.info(f"writing file {file_name}  to {output_path}")
    df.to_parquet(output_path, engine='fastparquet', compression='snappy')
    if db is not None:
        logger.info(f"writing table {file_name} in schema {schema}")
        db.load_table(df=df, table_name=file_name, schema=schema)