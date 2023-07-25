import glob
import os
from pandas import DataFrame
from sklearn.preprocessing import LabelEncoder
from typing import List, Dict, Set

# Configure logging
from src import *

logger = configure_logging("pbp_logger")


def assert_and_alert(assertion, msg, silent=True):
    """
    Placeholder for more sophisticated alerting
    """
    if assertion:
        return True
    logger.warning(msg)
    if not silent:
        raise Exception(msg)


def assert_not_null(df, column_name):
    """placeholder for more sophisticated alerting"""
    nulls = df[column_name].isnull().sum()
    return assert_and_alert(assertion=nulls == 0, msg=f"NULLS found for column {column_name}")


def publish_warning(msg, level):
    """placeholder for more sophisticated alerting"""
    logger.warning(msg)


def publish_error(msg):
    """placeholder for more sophisticated alerting"""
    logger.error(msg)


def check_valid_values(df: DataFrame, column_name: str, valid_values: Set = None):
    """
    Check that the unique values for a column match the set of valid values
    """
    if valid_values is None:
        valid_values = ([1, 0])
    return set(df[column_name].unique()) == valid_values


def conform_binary_column(df: DataFrame, column_name: str):
    """
    Impute a binary column with 0 or 1
    """
    # verify no nulls
    df[column_name] = df[column_name].fillna(0)

    # verify 1 or 0
    assert_and_alert(check_valid_values(df, column_name, {0, 1}),
                     msg=f"BAD column=[{column_name}] valid values are 1 or zero")


def impute_columns(df: DataFrame, columns, value=0):
    """
    Impute columns with a value
    Parameters:
        df: A DataFrame or slice of a DataFrame
        columns: List of columns to impute
        value: The value to impute
    Returns:
        DataFrame: The imputed DataFrame
    """
    logger.info(f"Impute columns to {value}")
    df.loc[:, columns] = df.loc[:, columns].fillna(value)


def create_dimension(df: DataFrame,
                     columns: Union[List[str], Dict[str, str]],
                     additional_fields: Dict = None,
                     keys: List[str] = None,
                     category: str = "general") -> DataFrame:
    """
    Create a dimension table from a dataframe
    Parameters:
        df: A DataFrame or slice of a DataFrame
        columns: List of columns to include in the dimension
        additional_fields: Dict of additional fields to add to the dimension
        keys: List of keys to use for the dimension
        category: Category of the dimension
    Returns:
        DataFrame
    """
    logger.info(f"Creating new {category} dimension...")

    if additional_fields is None:
        additional_fields = {}
    if keys is None:
        keys = ['season', 'game_id', 'play_id']

    if isinstance(columns, dict):
        query = keys + [x for x in columns.keys if x not in keys]
        dim_df = df[query].copy()
        dim_df.rename(columns=columns)
    else:
        query = keys + [x for x in columns if x not in keys]
        dim_df = df[query].copy()

    for k, v in additional_fields.items():
        dim_df[k] = v
    return dim_df


def explode_column(df: DataFrame, primary_key: str, column: str, sep: str =";") -> DataFrame:
    """
    Explode a column with a separator
    Parameters:
        df: A DataFrame or slice of a DataFrame
        primary_key: The primary key of the DataFrame
        column: The column to explode
        sep: The separator to use
    Returns:
        DataFrame: The exploded DataFrame
    """
    facts_df = df[[primary_key, column]].copy()
    facts_df[column] = facts_df[column].str.split(sep)
    return facts_df.explode(column)


def explode_column_with_cols(df, columns: List[str], column: str, sep=";"):
    cols = columns
    cols.append(column)
    facts_df = df[cols].copy()
    facts_df[column] = facts_df[column].str.split(sep)
    return facts_df.explode(column)


def get_duplicates_by_key(df, key_name):
    """
    Get the duplicate keys from a DataFrame
    """
    # Get the count of duplicate keys
    duplicate_counts = df.groupby(key_name).size().reset_index(name='count')

    # Filter the duplicate keys
    duplicate_keys = duplicate_counts[duplicate_counts['count'] > 1]

    return duplicate_keys


def load_files(data_subdir: str) -> Optional[DataFrame]:
    """
    Load all files from a directory
    Parameters:
        data_subdir: The subdirectory to load
    Returns:
        DataFrame: The loaded DataFrame or None
    """
    output_dir = get_config('output_directory')
    data_dir = os.path.join(output_dir, data_subdir)
    logger.info(f"Reading all files from {data_subdir}")
    data_files = []
    for extension in tested_extensions.keys():
        data_files.extend(glob.glob(f'{data_dir}/*.{extension}'))

    # Initialize an empty list to store individual DataFrames
    dfs = []

    # Iterate over each CSV file and load it into a DataFrame
    record_count = 0
    for file in data_files:
        logger.debug(f"  + Reading {os.path.basename(file)}")
        func = get_read_function(file)
        df = func(file)
        record_count += len(df)
        df['op_source'] = file
        dfs.append(df)

    # Concatenate all DataFrames into a single DataFrame
    assert_and_alert(record_count != 0, msg=f"No Records found for {data_subdir}")
    if record_count > 0:
        combined_df = pd.concat(dfs, ignore_index=True)
        assert_and_alert(len(combined_df) == record_count, msg=f"Records lost during loading of {data_subdir}")
        return combined_df

    # Print the combined DataFrame
    return None
