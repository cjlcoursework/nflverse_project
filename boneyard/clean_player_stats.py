import pandas as pd
from pandas import DataFrame

from utils import assert_and_alert, conform_binary_column
import warnings

warnings.filterwarnings('ignore')

actions = {
    'pass': 'pass_attempt',
    'rush_attempt': 'ok',
    'punt_attempt': 'X',
    'field_goal_attempt': 'X',
    'two_point_conversion_attempt': 'X',
    'extra_point_attempt': 'X',
    'two_point_attempt': 'OK',
}

"""motivations/ situational
defensive_two_point_attempt
two_point_attempt
defensive_extra_point_attempt
extra_point_attempt

(first_down, td, etc)
"""


"""
ACTIONS
"""
actions = [
    'field_goal_attempt',
    'kickoff_attempt',
    'punt_attempt',
    'pass_attempt',
    'rush_attempt',
]

actions = [
    {'action': 'field_goal_attempt', 'result': 'field_goal_result'},
    {'action': 'kickoff_attempt', 'result': 'two_point_conv_result'},
    {'action': 'punt_attempt', 'result': 'extra_point_result'},
    {'action': 'pass_attempt', 'result': 'extra_point_result'},
    {'action': 'rush_attempt', 'result': 'extra_point_result'},
]

motivations = [
    {'action': 'defensive_two_point_attempt', 'result': 'XX'},
    {'action': 'two_point_attempt', 'result': 'two_point_conv_result'},
    {'action': 'extra_point_attempt', 'result': 'extra_point_result'},
]

outcomes = [
    {'action': 'defensive_extra_point_attempt', 'result': 'defensive_extra_point_attempt'},
    {'action': 'fourth_down_converted', 'result': None},  # defense blocked offensive play and attempted a conversion
    {'action': 'fumble', 'result': 'TURNOVER'},
]

binary_columns = [
    "play_deleted",
    "extra_point_attempt",
    "field_goal_attempt",
    "fumble",
    "lateral_recovery",
    "lateral_return",
    "lateral_rush",
    "lateral_reception",
    "first_down_pass",
    "first_down_penalty",
    "first_down_rush",
    "touchback",
    "assist_tackle",
    "fourth_down_converted",
    "third_down_converted",
    "fumble_forced",
    "fumble_lost",
    "fumble_not_forced",
    "fumble_out_of_bounds",
    "div_game",
    "own_kickoff_recovery_td",
    "own_kickoff_recovery",
    "kickoff_inside_twenty",
    "kickoff_fair_catch",
    "kickoff_downed",
    "kickoff_in_endzone",
    "kickoff_out_of_bounds",
    "complete_pass",
    "incomplete_pass",
    "interception",
    "sack",
    "touchdown",
    "pass_touchdown",
    "return_touchdown",
    "rush_touchdown",
    "pass_attempt",
    "rush_attempt",
    "fourth_down_failed",
    "third_down_failed",
    "punt_inside_twenty",
    "punt_blocked",
    "punt_fair_catch",
    "punt_downed",
    "punt_in_endzone",
    "punt_out_of_bounds",
    "tackle_with_assist",
    "kickoff_attempt",
    "punt_attempt",
    "two_point_attempt",
    "penalty",
    "replay_or_challenge",
    "safety",
    "sp",
    "tackled_for_loss",
    "timeout",
    "qb_dropback",
    "qb_scramble",
    "qb_spike",
    "qb_kneel",
    "no_huddle",
    "shotgun",
    "goal_to_go",
    "quarter_end",
    "special_teams_play",
    "qb_hit",
    "drive_inside20",
    "aborted_play",
    "first_down",
    "solo_tackle",
    "pass",
    "rush",
    "special",
    "drive_ended_with_score",
    "success",
    "defensive_extra_point_conv",
    "defensive_two_point_conv",
    "defensive_two_point_attempt",
    "defensive_extra_point_attempt",
    "play"
]

"""
TIER ONE - initial cleanp

"""


# def conform_binary_flags(df):
#     for column_name in binary_columns:
#         conform_binary_column(df, column_name)



def impute_binary_columns(df):
    df.loc[:, binary_columns] = df.loc[:, binary_columns].fillna(0)


def populate_action_key(df, column_name):
    if 'action' not in df:
        df['action'] = None
    df.loc[(df[column_name] == 1), 'action'] = column_name


def conform_actions(df: DataFrame):
    df.drop(columns=['pass_attempt'])  # this column is either inconsistent, mundane, or I don't understand it
    df.rename(columns={'pass': 'pass_attempt'})
    conform_binary_column(df, 'pass_attempt')
    populate_action_key(df, column_name='pass_attempt')

    for column_name in actions:
        conform_binary_column(df, column_name)
        populate_action_key(df, column_name=column_name)


#
#  player_stats
#
def player_stats_fixes():
    return list([
        ('00-0027567', 'Steve Maneri', 'TE'),
        ('00-0028543', 'Jeff Maehl', 'WR'),
        ('00-0025569', 'Adam Hayward', 'LB'),
        ('00-0029675', 'Trent Richardson', 'RB')])


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


def impute_player_stats(df):
    # iterate over our list and update the position column for ones we've looked up
    for im in player_stats_fixes():
        gsis_id = im[0]
        position = im[2]
        df.loc[(df.player_id == gsis_id) & (df['position'].isnull()), 'position'] = position

    # Jus drop this record from 1999 with no player info
    df = df.drop(df.loc[df['player_id'] == '00-0005532'].index)

    # replace empty position_groups wiht our position
    df.loc[df['position_group'].isnull(), 'position_group'] = df['position']
    df.loc[df['player_name'].isnull(), 'player_name'] = df['player_display_name']
    df.loc[df['headshot_url'].isnull(), 'headshot_url'] = 'none'
    #
    df[player_stats_impute_to_zero] = df[player_stats_impute_to_zero].fillna(0)
    return df


#
#  players
#


droppable_players = ["Arthur Williams", "Frank Stephens"]


def impute_payers(player_df):
    # drop players with no gsis_id - I can't really link them to anything and they are ...
    player_df = player_df.drop(
        player_df.loc[(player_df['display_name'].isin(droppable_players)) & (player_df['gsis_id'].isnull())].index)

    # just set null status = 'NONE' -- there are just a few
    player_df.loc[(player_df['status'].isnull()), 'status'] = 'NONE'

    return player_df


def check_merge(merged_df, stats_df):
    n = len(merged_df)
    sn = len(stats_df)
    total_stats = len(stats_df)
    stats_without_players = (merged_df['_merge'] == 'left_only').sum() / n
    player_without_stats = (merged_df['_merge'] == 'right_only').sum() / n
    inner_joins = (merged_df['_merge'] == 'both').sum()
    inner_join_percentage = inner_joins / n
    stats_completeness = inner_joins / total_stats

    print(f"The stats dataset has {sn} records")
    print(f"The merged dataset has {n} records")
    print("percent of stats_without_players", stats_without_players)
    print("percent of players_without_stats - this is common", player_without_stats)
    print("percent of matched players and stats", inner_join_percentage)
    print("percent of stats that were consumed in the join", stats_completeness)

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


def rename_pbp_participant_columns(participation_df):
    # rename nflverse_game_id = game_id
    participation_df.rename(columns={'nflverse_game_id': 'game_id'}, inplace=True)
    participation_df['play_counter'] = participation_df['play_id']
    participation_df['play_id'] = participation_df["game_id"].astype(str) + "_" + participation_df[
        "play_counter"].astype(str)  ## temporary
    assert_and_alert(len(get_duplicates_by_key(participation_df, 'play_id')) == 0,
                     msg="Unexpected duplicate keys found creating play_id from game_id and play_id")


def reconcile_join_keys(pbp_df, participation_df):
    # pbp to participation is a 1 to 1 join
    merged_df = pd.merge(pbp_df, participation_df, left_on='play_id', right_on='play_id', how='outer', indicator=True,
                         suffixes=('_new', '_prev'))
    merged_df = merged_df.copy()
    assert_and_alert(len(get_duplicates_by_key(merged_df, 'play_id')) == 0,
                     msg="Unexpected duplicate keys found creating play_id from game_id and play_id")

    missing_participations = merged_df.loc[(merged_df['_merge'] == 'left_only')] \
        [['play_id', 'play_counter_new', 'game_id_new', 'old_game_id_new']] \
        .rename(columns={
        'play_counter_new': 'play_counter',
        'game_id_new': 'game_id',
        'old_game_id_new': 'old_game_id'
    }).drop_duplicates()

    found = 0
    nf = 0
    for index, row in missing_participations.iterrows():
        good_game_id = row['game_id']
        good_play_id = row['play_id']
        good_old_game_id = row['old_game_id']
        fix = participation_df.loc[(participation_df['old_game_id'] == good_old_game_id), ['play_id', 'game_id']]
        participation_df.loc[participation_df['old_game_id'] == good_old_game_id, ['play_id', 'game_id']] = [
            good_play_id, good_game_id]

        if len(fix) == 0:
            nf += 1
        else:
            found += 1

    print(f"Missing participants:   fixed {found},  not fixed: {nf}")

    # create play_id : (pbp_df["game_id"]+"_"+pbp_df["play_id"])
    participation_df['play_id'] = participation_df["game_id"].astype(str) + "_" + participation_df[
        "play_counter"].astype(str)
    assert_and_alert(len(get_duplicates_by_key(participation_df, 'play_id')) == 0,
                     msg="Unexpected duplicate keys found creating play_id from game_id and play_id")

    merged_df = pd.merge(pbp_df, participation_df, left_on='play_id', right_on='play_id', how='outer', indicator=True,
                         suffixes=('_new', '_prev'))
    merged_df = merged_df.copy()
    print(merged_df['_merge'].value_counts())
# %%
