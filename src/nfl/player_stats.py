import pandas as pd
from pandas import DataFrame

from src import *

import warnings

from src.util.utils import assert_and_alert, impute_columns, assert_not_null

warnings.filterwarnings('ignore')
logger = configure_logging("pbp_logger")


empty_headshot_url = 'none'

# todo - move this to config
player_stats_impute_to_zero = [
    "completions",
    "attempts",
    "passing_yards",
    "passing_tds",
    "interceptions",
    "sacks",
    "sack_yards",
    "sack_fumbles",
    "sack_fumbles_lost",
    "passing_air_yards",
    "passing_yards_after_catch",
    "passing_first_downs",
    "passing_epa",
    "passing_2pt_conversions",
    "pacr",
    "dakota",
    "carries",
    "rushing_yards",
    "rushing_tds",
    "rushing_fumbles",
    "rushing_fumbles_lost",
    "rushing_first_downs",
    "rushing_epa",
    "rushing_2pt_conversions",
    "receptions",
    "targets",
    "receiving_yards",
    "receiving_tds",
    "receiving_fumbles",
    "receiving_fumbles_lost",
    "receiving_air_yards",
    "receiving_yards_after_catch",
    "receiving_first_downs",
    "receiving_epa",
    "receiving_2pt_conversions",
    "racr",
    "target_share",
    "air_yards_share",
    "wopr",
    "special_teams_tds",
    "fantasy_points",
    "fantasy_points_ppr"]

droppable_players = ["Arthur Williams", "Frank Stephens"]


def player_stats_fixes():
    return list([
        ('00-0027567', 'Steve Maneri', 'TE'),
        ('00-0028543', 'Jeff Maehl', 'WR'),
        ('00-0025569', 'Adam Hayward', 'LB'),
        ('00-0029675', 'Trent Richardson', 'RB')])


def check_keys(df):
    assert_not_null(df, 'season')
    assert_not_null(df, 'week')
    assert_not_null(df, 'player_id')
    assert_not_null(df, 'position')

    assert_and_alert(
        assertion=(df.isna().sum().sum() == 0),
        msg="Found unexpected Nulls in player_stats ")


def merge_injuries(player_stats_df: DataFrame, player_injuries_df: DataFrame) -> DataFrame:

    """
    merges injury information into player_stats for the same week

    Parameters:
        player_stats_df (pd.DataFrame): nflverse player_stats dataset
        player_injuries_df (pd.DataFrame): anflverse injuries dataset

    Returns:
        player_stats_df (pd.DataFrame): a new version of payer_stats with injury data

     """

    df = pd.merge(player_stats_df,
                  player_injuries_df[['season', 'week', 'player_id', 'primary_injury', 'report_status', 'practice_status']],
                  left_on=['season', 'week', 'player_id'],
                  right_on=['season', 'week', 'player_id'],
                  how='left')
    df.primary_injury.fillna('None', inplace=True)
    df.report_status.fillna('None', inplace=True)
    df.practice_status.fillna('None', inplace=True)
    sz_before = player_stats_df.shape[0]
    sz_after = df.shape[0]
    assert_and_alert(
        sz_before == sz_after,
        msg=f"After merge player_stats count changed - went from {sz_before} to {sz_after}"
    )
    return df


def transform_player_stats(player_stats_df: DataFrame) -> DataFrame:
    """
    main program for wrangling player_stats from nflverse,
    cleanup some specific records
    impute missing values from other redundant columns
    fill some binary columns (0 or 1) with zeros
    rename gsis_id to player_id for consistency with other data
]
    Parameters:
        player_stats_df (pd.DataFrame): an offense or defense stats dataframe that we can use for feature selection

    Returns:
        player_stats_df (pd.DataFrame): a 'clean' version of payer_stats

     """

    # iterate over our list and update the position column for ones we've looked up
    logger.info(f"fix specific player_stats: {player_stats_fixes}..")
    for im in player_stats_fixes():
        gsis_id = im[0]
        position = im[2]
        player_stats_df.loc[(player_stats_df.player_id == gsis_id) & (player_stats_df['position'].isnull()), 'position'] = position

    # Jus drop this record from 1999 with no player info
    player_stats_df = player_stats_df.drop(player_stats_df.loc[player_stats_df['player_id'] == '00-0005532'].index)

    # replace empty position_groups with position
    logger.info("replace empty position_groups with position info...")
    player_stats_df['position_group'] = player_stats_df['position_group'].fillna(player_stats_df['position'])

    logger.info("replace empty player_name with player_display_name info...")
    player_stats_df['player_name'] = player_stats_df['player_name'].fillna(player_stats_df['player_display_name'])

    logger.info(f"replace empty headshot_url with '{empty_headshot_url}'...")
    player_stats_df['headshot_url'] = player_stats_df['headshot_url'].fillna(empty_headshot_url)

    logger.info(f"fillna(0) for all binary columns...")
    impute_columns(player_stats_df, player_stats_impute_to_zero)

    player_stats_df.rename(columns={
        'gsis_id': 'player_id',
        'recent_team': 'team'}, inplace=True)

    check_keys(player_stats_df)
    return player_stats_df


def transform_players(player_df: DataFrame) -> DataFrame:
    """
    main program for wrangling players from nflverse,
    drop a few records missing the gsis_id key - they can't join to anything else
    rename gsis_id to player_id for consistency with other data

    Parameters:
        player_df (pd.DataFrame): an offense or defense stats dataframe that we can use for feature selection

    Returns:
        player_df (pd.DataFrame): a 'clean' version of payer_stats

     """
    logger.info("Process players dataset...")

    logger.info("drop players without gsis_ids - they won't link to player_stats")
    player_df = player_df.drop(
        player_df.loc[(player_df['display_name'].isin(droppable_players)) & (player_df['gsis_id'].isnull())].index)

    logger.info("fill empty players status to 'NONE'")
    player_df['status'] = player_df['status'].fillna('NONE')

    logger.info("rename gsis_id to player_id...")
    player_df.rename(columns={'gsis_id': 'player_id'}, inplace=True)

    return player_df





