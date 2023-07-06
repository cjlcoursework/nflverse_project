import logging

import pandas as pd
from pandas import DataFrame

from utils import assert_and_alert, create_dimension
import warnings

warnings.filterwarnings('ignore')

# Configure logging
from logging_config import confgure_logging
logger = confgure_logging("pbp_logger")

kicker_columns = [
    "kicker_player_id",
    "kicker_player_name",
    "kick_distance"

]

kickoff_columns = [
    "kickoff_downed",
    "kickoff_attempt",
    "kickoff_in_endzone",
    "kickoff_inside_twenty",
    "kickoff_out_of_bounds",
    "kickoff_fair_catch",
    "own_kickoff_recovery_td",
    "lateral_kickoff_returner_player_id",
    "kickoff_returner_player_id",
    "own_kickoff_recovery_player_name",
    "own_kickoff_recovery",
    "own_kickoff_recovery_player_id",
    "lateral_kickoff_returner_player_name",
    "kickoff_returner_player_name"
]


def get_kickoffs(df: DataFrame) -> DataFrame:
    logger.info("Pull fumble-related fields from main dataframe")
    return df.loc[(df["kickoff_attempt"] == 1)]


def create_kickoffs_dimension(df, get_stats=False):
    logger.info("Create a kickoff dimension, removing from pbp...")
    logger.info("Keep fact columns: kickoff_attempt is a fact")

    kickoffs_df = get_kickoffs(df)

    # , add kicker_columns but don't remove them - thEy are also used for punts
    kickoffs_df = create_dimension(kickoffs_df,
                                   columns=kickoff_columns + kicker_columns, category="kickoff")

    if get_stats:
        stats_df = player_kickoff_stats(kickoffs_df)
        # save them here?

    logger.info("Leave the kickoff field as a fact")

    kickoff_columns.remove("kickoff_attempt")  # just in case it's in the list
    df.drop(columns=kickoff_columns)
    return df, kickoff_columns


def player_kickoff_stats(kickoffs_df, stats=False):
    # todo - needs work - don't use it
    # the result ... returned for x yards,
    if not stats:
        logger.warning("Not creating player stats for kickoffs - this needs more work")
        return

    node1 = create_dimension(kickoffs_df,
                             columns={
                                 "kicker_player_id": "player_id",
                                 "kicker_player_name": "player_name",
                                 "kick_distance": "yards_gain_loss",
                                 "kickoff_in_endzone": 'kickoff_in_endzone',
                                 "kickoff_inside_twenty": 'kickoff_inside_twenty',
                                 "kickoff_out_of_bounds": 'kickoff_out_of_bounds',
                             },
                             additional_fields={'event': 'kickoff'})

    returner = create_dimension(kickoffs_df,
                                columns={
                                    "kickoff_returner_player_id": 'player_id',
                                    "kickoff_returner_player_name": "player_name",
                                    "kickoff_downed": 'kickoff_downed',
                                    "kickoff_fair_catch": 'kickoff_fair_catch',
                                    "lateral_kickoff_returner_player_id": 'lateral_kickoff_returner_player_id',
                                },
                                additional_fields={'event': 'kickoff_return'})

    own_return = create_dimension(kickoffs_df,
                                  columns={
                                      "own_kickoff_recovery_player_id": 'player_id',
                                      "own_kickoff_recovery_player_name": 'player_name',
                                      "own_kickoff_recovery_td": 'X',
                                      "own_kickoff_recovery": 'X',
                                  },
                                  additional_fields={'event': 'own_kickoff_recovery'})
