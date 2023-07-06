import pandas as pd
from pandas import DataFrame

from utils import assert_and_alert, create_dimension

import logging
import warnings

warnings.filterwarnings('ignore')
# Configure logging
from logging_config import confgure_logging
logger = confgure_logging("pbp_logger")

pass_columns = [
    "passer_player_id",
    "passer_id",
    "passer_player_name",
    "passing_yards",
    "jersey_number",
    "complete_pass",
    "id",
    "name",
    "passer",
    "passer_jersey_number",
    "first_down_pass",
    "pass_defense_1_player_id",
    "pass_defense_2_player_id",
    "pass_defense_1_player_name",
    "pass_defense_2_player_name",
]

"""
incomplete_pass
qb_spike
pass_attempt
no_huddle
qb_dropback
shotgun
sack
qb_hit
interception
pass_length
yards_after_catch
air_yards
pass_location
qb_hit_1_player_name
qb_hit_1_player_id
qb_hit_2_player_id
qb_hit_2_player_name
lateral_receiver_player_id
lateral_reception
receiver_player_id
receiver_player_name
receiver_id
lateral_receiving_yards
receiver_jersey_number
lateral_receiver_player_name
receiving_yards
receiver


"""


def get_passes(df: DataFrame) -> DataFrame:
    logger.info("Pull pass-related fields from main dataframe")
    return df.loc[(df['pass'] == 1)]


def get_pass_yards(df: DataFrame) -> DataFrame:
    return df.loc[(df['pass'] == 1) & (df['receiving_yards'] > 0)]


def consolidate_pass_columns(pass_df):

    logger.info("consolidate redundant [passer_id, id, passer_player_id]  columns")
    pass_df['player_id'] = pass_df['id'].fillna(pass_df['passer_id'])
    pass_df['player_id'] = pass_df['player_id'].fillna(pass_df['passer_player_id'])

    logger.info("consolidate redundant [passer, name, passer_player_name]  columns")
    pass_df['player_name'] = pass_df['name'].fillna(pass_df['passer'])
    pass_df['player_name'] = pass_df['player_name'].fillna(pass_df['passer_player_name'])

    logger.info("consolidate redundant [jersey_number, passer_jersey_number]  columns")
    pass_df['passer_jersey_number'] = pass_df['jersey_number'].fillna(pass_df['passer_jersey_number'])
    pass_df.rename({'passing_yards': 'yards_gain_loss'})
    pass_df.drop(
        columns=['id', 'passer_id', 'name', 'passer', 'passer_player_id', 'passer_player_name', 'passer_jersey_number'], inplace=True)

    return pass_df


def create_pass_dimension(df):

    pass_df = get_passes(df)
    pass_df = create_dimension(df=pass_df, columns=pass_columns, category="pass")

    pass_df = consolidate_pass_columns(pass_df)

    logger.info("Leave the pass, and pass_attempt fields as a fact")
    pass_columns.remove("pass")  # just in case it's in the list
    pass_columns.remove("pass_attempt")
    df.drop(columns=pass_columns)
    return df, pass_df

