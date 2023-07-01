import pandas as pd

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


def player_stats_fixes():
    return list([
        ('00-0027567', 'Steve Maneri', 'TE'),
        ('00-0028543', 'Jeff Maehl', 'WR'),
        ('00-0025569', 'Adam Hayward', 'LB'),
        ('00-0029675', 'Trent Richardson', 'RB')])


def impute_player_stats(df):

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
    return df


def impute_payers(player_df):
    logger.info("Process players dataset...")

    logger.info("drop players without gsis_ids - they won't link to player_stats")
    player_df = player_df.drop(
        player_df.loc[(player_df['display_name'].isin(droppable_players)) & (player_df['gsis_id'].isnull())].index)

    logger.info("fill empty players status to 'NONE'")
    player_df['status'] = player_df['status'].fillna('NONE')

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
    assert_not_null(df, 'position_group')

    assert_and_alert(
        assertion=(df.isna().sum().sum() == 0),
        msg="Found unexpected Nulls in player_stats ")


# def job_player_stats_main(df: DataFrame) -> DataFrame:
#     stats_df = impute_player_stats(df)
#     check_keys(stats_df)
#     return stats_df


def test_player_stats_job():
    stats_df = pd.read_csv("../../output/player_stats.csv", low_memory=False)
    stats_df = impute_player_stats(stats_df)
    check_keys(stats_df)

    players_df = pd.read_csv("../../output/players.csv", low_memory=False)
    players_df = impute_payers(players_df)
    players_df.rename(columns={'gsis_id': 'player_id'}, inplace=True)

    merged_df = pd.merge(stats_df, players_df, left_on='player_id', right_on='player_id', how='outer', indicator=True)
    check_merge(merged_df, stats_df)

    print("Done")




#
# def rename_pbp_columns(pbp_df):
#     # ---------------------
#     pbp_df['play_counter'] = pbp_df['play_id']
#     pbp_df['play_id'] = pbp_df["game_id"].astype(str) + "_" + pbp_df["play_counter"].astype(str)
#     assert_and_alert(len(get_duplicates_by_key(pbp_df, 'play_id')) == 0,
#                      msg="Unexpected duplicate keys found creating play_id from game_id and play_id")
#
#
# def rename_pbp_participant_columns(participation_df):
#     # rename nflverse_game_id = game_id
#     participation_df.rename(columns={'nflverse_game_id': 'game_id'}, inplace=True)
#     participation_df['play_counter'] = participation_df['play_id']
#     participation_df['play_id'] = participation_df["game_id"].astype(str) + "_" + participation_df[
#         "play_counter"].astype(str)  ## temporary
#     assert_and_alert(len(get_duplicates_by_key(participation_df, 'play_id')) == 0,
#                      msg="Unexpected duplicate keys found creating play_id from game_id and play_id")
#
#
# def reconcile_join_keys(pbp_df, participation_df):
#     # pbp to participation is a 1 to 1 join
#     merged_df = pd.merge(pbp_df, participation_df, left_on='play_id', right_on='play_id', how='outer', indicator=True,
#                          suffixes=('_new', '_prev'))
#     merged_df = merged_df.copy()
#     assert_and_alert(len(get_duplicates_by_key(merged_df, 'play_id')) == 0,
#                      msg="Unexpected duplicate keys found creating play_id from game_id and play_id")
#
#     missing_participations = merged_df.loc[(merged_df['_merge'] == 'left_only')] \
#         [['play_id', 'play_counter_new', 'game_id_new', 'old_game_id_new']] \
#         .rename(columns={
#         'play_counter_new': 'play_counter',
#         'game_id_new': 'game_id',
#         'old_game_id_new': 'old_game_id'
#     }).drop_duplicates()
#
#     found = 0
#     nf = 0
#     for index, row in missing_participations.iterrows():
#         good_game_id = row['game_id']
#         good_play_id = row['play_id']
#         good_old_game_id = row['old_game_id']
#         fix = participation_df.loc[(participation_df['old_game_id'] == good_old_game_id), ['play_id', 'game_id']]
#         participation_df.loc[participation_df['old_game_id'] == good_old_game_id, ['play_id', 'game_id']] = [
#             good_play_id, good_game_id]
#
#         if len(fix) == 0:
#             nf += 1
#         else:
#             found += 1
#
#     print(f"Missing participants:   fixed {found},  not fixed: {nf}")
#
#     # create play_id : (pbp_df["game_id"]+"_"+pbp_df["play_id"])
#     participation_df['play_id'] = participation_df["game_id"].astype(str) + "_" + participation_df[
#         "play_counter"].astype(str)
#     assert_and_alert(len(get_duplicates_by_key(participation_df, 'play_id')) == 0,
#                      msg="Unexpected duplicate keys found creating play_id from game_id and play_id")
#
#     merged_df = pd.merge(pbp_df, participation_df, left_on='play_id', right_on='play_id', how='outer', indicator=True,
#                          suffixes=('_new', '_prev'))
#     merged_df = merged_df.copy()
#     print(merged_df['_merge'].value_counts())
# # %%
