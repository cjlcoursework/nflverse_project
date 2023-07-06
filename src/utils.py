from typing import Union, List, Dict

import pandas as pd
from pandas import DataFrame


# Configure logging
from configs import configure_logging
logger = configure_logging("pbp_logger")


def assert_and_alert(assertion, msg, silent=True):
    if assertion:
        return True
    logger.warning(msg)
    if not silent:
        raise Exception(msg)


def assert_not_null(df, column_name):
    nulls = df[column_name].isnull().sum()
    return assert_and_alert(assertion=nulls == 0, msg=f"NULLS found for column {column_name}")


def publish_warning(msg, level):
    logger.warning(msg)


def publish_error(msg):
    logger.error(msg)


def check_valid_values(df, column_name, valid_values=None):
    if valid_values is None:
        valid_values = ([1, 0])
    return set(df[column_name].unique()) == valid_values


def conform_binary_column(df, column_name):
    # drop or ignore pass_attempt - it's not consistent
    # pass
    df[column_name] = df[column_name].fillna(0)

    # verify 1 or 0
    assert_and_alert(check_valid_values(df, column_name, {0, 1}),
                     msg=f"BAD column=[{column_name}] valid values are 1 or zero")


def impute_columns(df, columns, value=0):
    logger.info(f"Impute columns to {value}")
    df.loc[:, columns] = df.loc[:, columns].fillna(value)


def create_dimension(df,
                     columns: Union[List[str], Dict[str, str]],
                     additional_fields=None,
                     keys=None,
                     category="general") -> DataFrame:

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


def explode_column(df, primary_key, column, sep=";"):
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
    # Get the count of duplicate keys
    duplicate_counts = df.groupby(key_name).size().reset_index(name='count')

    # Filter the duplicate keys
    duplicate_keys = duplicate_counts[duplicate_counts['count'] > 1]

    return duplicate_keys
