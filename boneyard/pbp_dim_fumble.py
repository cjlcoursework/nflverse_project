import logging
import logging
from typing import List

import pandas as pd
from pandas import DataFrame

from utils import assert_and_alert, create_dimension
import warnings

warnings.filterwarnings('ignore')

# Configure logging
from logging_config import confgure_logging
logger = confgure_logging("pbp_logger")


fumble_columns = [
    "fumble_forced",
    "fumble_lost",
    "fumble",
    "fumble_not_forced",
    "fumble_out_of_bounds",
    "fumble_recovery_1_player_id",
    "fumble_recovery_1_player_name",
    "fumble_recovery_1_team",
    "fumble_recovery_1_yards",
    "fumble_recovery_2_player_id",
    "fumble_recovery_2_player_name",
    "fumble_recovery_2_team",
    "fumble_recovery_2_yards",
    "fumbled_1_player_id",
    "fumbled_1_player_name",
    "fumbled_1_team",
    "fumbled_2_player_id",
    "fumbled_2_player_name",
    "fumbled_2_team"
]


def get_fumbles(df: DataFrame) -> DataFrame:
    logger.info("Pull fumble-related fields from main dataframe")
    return df.loc[(df['fumble'] == 1)]


def update_turnovers(df: DataFrame):
    logger.info("Create a turnovers field for fumbles resulting in turnovers")
    if 'turnover' not in df:
        df['turnover'] = 0

    mask = (df['fumble'] == 1) & \
           ((df['posteam'] != df['fumble_recovery_2_team']) |
            (df['posteam'] != df['fumble_recovery_1_team']))
    df.loc[mask, 'turnover'] = 1


def create_fumbles_dimension(df: DataFrame, get_stats=False) -> (DataFrame, DataFrame):
    logger.info("Keep facts: fumble-occured, and turnover-ocurred are facts")
    logger.info("Create a fumble dimension, removing from pbp facts")
    """
    We only want two facts from fumbles
    1. did a fumble occur, and
    2. did it result in a turnover (new column)
    the rest of the fumble information is a dimension

    If there was a fumble, then if the final recovering team was different than the possessing team - it was a turnover
    In which case we want to indicate that there was a turnover

    finally, we want to keep the fumble and recovery stats for separate player performance metrics

    """

    update_turnovers(df)

    fumbles_df = get_fumbles(df)

    fumbles_df = create_dimension(df=fumbles_df, columns=fumble_columns, category="fumble")

    if get_stats:
        stats_df = player_fumble_stats(fumbles_df)
        # save them here?

    logger.info("Leave the fumble field as a fact")
    fumble_columns.remove("fumble")  # just in case it's in the list
    df.drop(columns=fumble_columns)
    return df, fumbles_df


def player_fumble_stats(df: DataFrame) -> DataFrame:
    logger.info("Get player statistics on fumbles")
    """
    We only want two facts from fumbles
    1. did a fumble occur, and
    2. did it result in a turnover (new column)
    the rest of the fumble information is a dimension

    If there was a fumble, then if the final recovering team was different than the possessing team - it was a turnover
    In which case we want to indicate that there was a turnover

    finally, we want to keep the fumble and recovery stats for separate player performance metrics

    """
    update_turnovers(df)

    fumbles_df = get_fumbles(df)

    node1 = create_dimension(fumbles_df,
                           columns={
                               "fumbled_1_player_id": 'player_id',
                               "fumbled_1_player_name": 'player_name',
                               "fumbled_1_team": 'team_id',
                           },
                           additional_fields={'event': 'fumble', 'yards_gain_loss': 0})

    node2 = create_dimension(fumbles_df,
                           columns={
                               "fumbled_2_player_id": 'player_id',
                               "fumbled_2_player_name": 'player_name',
                               "fumbled_2_team": 'team_id',
                           },
                           additional_fields={'event': 'fumble', 'yards_gain_loss': 0})

    node3 = create_dimension(fumbles_df,
                           columns={
                               "fumble_recovery_1_player_id": 'player_id',
                               "fumble_recovery_1_player_name": 'player_name',
                               "fumble_recovery_1_team": 'team_id',
                               "fumble_recovery_1_yards": "yards_gain_loss"
                           },
                           additional_fields={'event': 'fumble-recovery'})

    node4 = create_dimension(fumbles_df,
                           columns={
                               "fumble_recovery_2_player_id": 'player_id',
                               "fumble_recovery_2_player_name": 'player_name',
                               "fumble_recovery_2_team": 'team_id',
                               "fumble_recovery_2_yards": "yards_gain_loss"
                           },
                           additional_fields={'event': 'fumble-recovery'})

    concatenated_df = pd.concat([node1, node2, node3, node4], axis=0)
    concatenated_df = concatenated_df.reset_index(drop=True)

    return concatenated_df

