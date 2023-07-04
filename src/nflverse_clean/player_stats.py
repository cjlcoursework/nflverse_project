import pandas as pd
from pandas import DataFrame

from utils import assert_and_alert, impute_columns, assert_not_null
from logging_config import confgure_logging
import warnings

warnings.filterwarnings('ignore')
logger = confgure_logging("pbp_logger")


empty_headshot_url = 'none'

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


#
#  player_stats
#
def merge_injuries(player_stats: DataFrame, player_injuries: DataFrame) -> DataFrame:
    df = pd.merge(player_stats,
                  player_injuries[['season', 'week', 'player_id', 'primary_injury', 'report_status', 'practice_status']],
                  left_on=['season', 'week', 'player_id'],
                  right_on=['season', 'week', 'player_id'],
                  how='left')
    df.primary_injury.fillna('None', inplace=True)
    df.report_status.fillna('None', inplace=True)
    df.practice_status.fillna('None', inplace=True)
    sz_before = player_stats.shape[0]
    sz_after = df.shape[0]
    assert_and_alert(
        sz_before == sz_after,
        msg=f"After merge player_stats count changed - went from {sz_before} to {sz_after}"
    )
    return df


def player_stats_fixes():
    return list([
        ('00-0027567', 'Steve Maneri', 'TE'),
        ('00-0028543', 'Jeff Maehl', 'WR'),
        ('00-0025569', 'Adam Hayward', 'LB'),
        ('00-0029675', 'Trent Richardson', 'RB')])


def transform_player_stats(df):

    # iterate over our list and update the position column for ones we've looked up
    logger.info(f"fix specific player_stats: {player_stats_fixes}..")
    for im in player_stats_fixes():
        gsis_id = im[0]
        position = im[2]
        df.loc[(df.player_id == gsis_id) & (df['position'].isnull()), 'position'] = position

    # Jus drop this record from 1999 with no player info
    df = df.drop(df.loc[df['player_id'] == '00-0005532'].index)

    # replace empty position_groups with position
    logger.info("replace empty position_groups with position info...")
    df['position_group'] = df['position_group'].fillna(df['position'])

    logger.info("replace empty player_name with player_display_name info...")
    df['player_name'] = df['player_name'].fillna(df['player_display_name'])

    logger.info(f"replace empty headshot_url with '{empty_headshot_url}'...")
    df['headshot_url'] = df['headshot_url'].fillna(empty_headshot_url)

    logger.info(f"fillna(0) for all binary columns...")
    impute_columns(df, player_stats_impute_to_zero)

    df.rename(columns={
        'gsis_id': 'player_id',
        'recent_team': 'team'}, inplace=True)

    check_keys(df)
    return df


def transform_players(player_df):

    logger.info("Process players dataset...")

    logger.info("drop players without gsis_ids - they won't link to player_stats")
    player_df = player_df.drop(
        player_df.loc[(player_df['display_name'].isin(droppable_players)) & (player_df['gsis_id'].isnull())].index)

    logger.info("fill empty players status to 'NONE'")
    player_df['status'] = player_df['status'].fillna('NONE')

    logger.info("rename gsis_id to player_id...")
    player_df.rename(columns={'gsis_id': 'player_id'}, inplace=True)

    return player_df


def check_merge(merged_df, stats_df):
    logger.info("Validate the players + player_stats merge...")

    n = len(merged_df)
    sn = len(stats_df)
    total_stats = len(stats_df)
    stats_without_players = (merged_df['_merge'] == 'left_only').sum() / n
    player_without_stats = (merged_df['_merge'] == 'right_only').sum() / n
    inner_joins = (merged_df['_merge'] == 'both').sum()
    inner_join_percentage = inner_joins / n
    stats_completeness = inner_joins / total_stats

    logger.info(f"The stats dataset has {sn} records")
    logger.info(f"The merged dataset has {n} records")
    logger.info(f"percent of stats_without_players: {stats_without_players}")
    logger.info(f"percent of players_without_stats - this is common: {player_without_stats}")
    logger.info(f"percent of matched players and stats : {inner_join_percentage}")
    logger.info(f"percent of stats that were consumed in the join: {stats_completeness}")

    assert_and_alert(assertion=stats_completeness > .98,
                     msg=f"expected at least 98% of player_stats to be linked to players: percentage={stats_completeness}")
    assert_and_alert(assertion=stats_without_players < .01,
                     msg=f"stats_without_players should be zero: percentage={stats_without_players}")


def get_duplicates_by_key(df, key_name):
    # Get the count of duplicate keys
    duplicate_counts = df.groupby(key_name).size().reset_index(name='count')

    # Filter the duplicate keys
    duplicate_keys = duplicate_counts[duplicate_counts['count'] > 1]

    return duplicate_keys


def check_keys(df):
    assert_not_null(df, 'season')
    assert_not_null(df, 'week')
    assert_not_null(df, 'player_id')
    assert_not_null(df, 'position')

    assert_and_alert(
        assertion=(df.isna().sum().sum() == 0),
        msg="Found unexpected Nulls in player_stats ")



