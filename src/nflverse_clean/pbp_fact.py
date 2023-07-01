from typing import Dict

import pandas as pd
from pandas import DataFrame

from NFLVersReader.src.nflverse_clean.configs import get_config
from NFLVersReader.src.nflverse_clean.database_loader import DatabaseLoader
from utils import assert_and_alert, impute_columns, get_duplicates_by_key, conform_binary_column, \
    create_dimension, load_dims_to_db
import logging_config
import warnings

warnings.filterwarnings('ignore')
logger = logging_config.confgure_logging("pbp_logger")

analytics_columns = ["no_score_prob",
              "opp_fg_prob",
              "opp_safety_prob",
              "opp_td_prob",
              "fg_prob",
              "safety_prob",
              "td_prob",
              "extra_point_prob",
              "two_point_conversion_prob",
              "ep",
              "epa",
              "total_home_epa",
              "total_away_epa",
              "total_home_rush_epa",
              "total_away_rush_epa",
              "total_home_pass_epa",
              "total_away_pass_epa",
              "air_epa",
              "yac_epa",
              "comp_air_epa",
              "comp_yac_epa",
              "total_home_comp_air_epa",
              "total_away_comp_air_epa",
              "total_home_comp_yac_epa",
              "total_away_comp_yac_epa",
              "total_home_raw_air_epa",
              "total_away_raw_air_epa",
              "total_home_raw_yac_epa",
              "total_away_raw_yac_epa",
              "wp",
              "def_wp",
              "home_wp",
              "away_wp",
              "wpa",
              "vegas_wpa",
              "vegas_home_wpa",
              "home_wp_post",
              "away_wp_post",
              "vegas_wp",
              "vegas_home_wp",
              "total_home_rush_wpa",
              "total_away_rush_wpa",
              "total_home_pass_wpa",
              "total_away_pass_wpa",
              "air_wpa",
              "yac_wpa",
              "comp_air_wpa",
              "comp_yac_wpa",
              "total_home_comp_air_wpa",
              "total_away_comp_air_wpa",
              "total_home_comp_yac_wpa",
              "total_away_comp_yac_wpa",
              "total_home_raw_air_wpa",
              "total_away_raw_air_wpa",
              "total_home_raw_yac_wpa",
              "total_away_raw_yac_wpa",
              "qb_epa",
              "xyac_epa",
              "xyac_mean_yardage",
              "xyac_median_yardage",
              "xyac_success",
              "xyac_fd",
              "xpass",
              "pass_oe" ]


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

situational_columns = [
    "away_score",
    "defteam_score",
    "defteam_score_post",
    "posteam_score",
    "posteam_score_post",
    "score_differential",
    "score_differential_post",
    "total_away_score",
    "total_home_score",
    "time",
    "time_of_day",
    "defteam_timeouts_remaining",
    "away_timeouts_remaining",
    "play_clock",
    "home_timeouts_remaining",
    "posteam_timeouts_remaining",
    "qtr",
    "game_half",
    "yrdln",
    "down",
    "half_seconds_remaining",
    "goal_to_go",
    "quarter_end",
    "game_seconds_remaining",
    "drive",
    "ydstogo",
    "quarter_seconds_remaining",
    "side_of_field",
    "timeout_team",
]

drive_columns = [
    "drive_end_yard_line",
    "drive_end_transition",
    "drive_quarter_end",
    "drive_ended_with_score",
    "drive_start_yard_line",
    "drive_play_id_started",
    "drive_quarter_start",
    "drive_start_transition",
    "drive_inside20",
    "drive_yards_penalized",
    "series",
    "series_result",
    "series_success",
    "drive_game_clock_end",
    "drive_time_of_possession",
    "drive_first_downs",
    "drive_game_clock_start",
    "drive_play_id_ended",
    "drive_play_count"
]

game_columns = [
    "stadium",
    "div_game",
    "home_opening_kickoff",
    "stadium_id",
    "season_type",
    "week",
    "roof",
    "season",
    "weather",
    "home_team",
    "old_game_id",
    "wind",
    "location",
    "game_stadium",
    "temp",
    "game_id",
    "surface",
    "away_team",
    "total_line",
    "game_date",
    "start_time",
    "home_score",
    "away_score"
]

play_core = [
    'pass_attempt',
    'rush_attempt',
    'kickoff_attempt',
    'punt_attempt',
    'field_goal_attempt',
    'extra_point_attempt',
    "touchdown",
    "touchback",
    "safety",
    "fumble",
    "complete_pass",
    "incomplete_pass",
    "interception",
    "sack",
    "penalty",
    "two_point_conv_result",
    "two_point_attempt",
    "qb_spike",
    "qb_kneel",
    "penalty_yards",
]

play_identities = [
    # Identity columns
    "posteam",
    "defteam",
    "away_coach",
    "home_coach"
]

play_info = [
    "penalty_team",
    "return_team",
    "return_yards",
    "kick_distance",
    "punt_blocked",
    "qb_hit",
    "solo_tackle",
    "yards_after_catch",

    "posteam_type",
    "rushing_yards",
    "fumble_forced",

    # "lateral_recovery",
    # "fourth_down_failed",
    # "third_down_converted",
    # "air_yards",
    # "td_team",
    # "first_down_pass",
    # "first_down_rush",

    "no_huddle",
    "field_goal_result",
    # "third_down_failed",

    "fourth_down_converted",
    # "first_down_penalty",
    # "shotgun",
    "fumble_not_forced",
    "kickoff_downed",
    "fumble_lost",

    "punt_fair_catch",
    # "pass_attempt",
    "kickoff_in_endzone",
    "kickoff_inside_twenty",
    "kickoff_out_of_bounds",
    "fumble_out_of_bounds",

    "punt_out_of_bounds",

    "kickoff_fair_catch",
    "punt_in_endzone",
    "punt_downed",
    # "rush_attempt",
    "own_kickoff_recovery_td",
    # "punt_attempt",
    "lateral_reception",
    "two_point_attempt",
    "rush_touchdown",
    "passing_yards",
    "replay_or_challenge",
    "first_down",
    "defensive_extra_point_attempt",
    "defensive_extra_point_conv",
    "lateral_rushing_yards",
    "lateral_receiving_yards",
    "special_teams_play",
    "aborted_play",
    "fumble_recovery_1_yards",
    "receiver",
    "replay_or_challenge_result",
    "defensive_two_point_attempt",
    "defensive_two_point_conv",
    "yards_gained",
    "ydsnet",
    "assist_tackle",
    "punt_inside_twenty",
    "pass_touchdown",
    "return_touchdown",
    "lateral_return",
    "tackled_for_loss",
    "own_kickoff_recovery",
    "timeout",
    "fumble_recovery_2_yards",
    "kickoff_attempt",
    "penalty_type",
    "play_type_nfl",
    "out_of_bounds",
    "play",
    "lateral_rush",
    "st_play_type",
    "special",
    "play_id",
    "play_type",
    "run_gap",
    "pass_length",
    "qb_dropback",
    "run_location",
    "pass_location",
    "qb_scramble",

]

player_id_columns = {'td_player_id': 'touchdown', 'tackle_with_assist_1_player_id': 'tackle_with_assist_1',
                      'tackle_for_loss_1_player_id': 'tackle_for_loss_1', 'solo_tackle_2_player_id': 'solo_tackle_2',
                      'solo_tackle_1_player_id': 'solo_tackle_1', 'assist_tackle_4_player_id': 'assist_tackle_4',
                      'assist_tackle_3_player_id': 'assist_tackle_3', 'assist_tackle_2_player_id': 'assist_tackle_2',
                      'assist_tackle_1_player_id': 'assist_tackle_1', 'safety_player_id': 'safety',
                      'sack_player_id': 'sack', 'rusher_player_id': 'rusher', 'receiver_player_id': 'receiver',
                      'qb_hit_2_player_id': 'qb_hit_2', 'qb_hit_1_player_id': 'qb_hit_1', 'punter_player_id': 'punter',
                      'punt_returner_player_id': 'punt_returner', 'penalty_player_id': 'penalty',
                      'passer_player_id': 'passer', 'pass_defense_2_player_id': 'pass_defense_2',
                      'pass_defense_1_player_id': 'pass_defense_1',
                      'own_kickoff_recovery_player_id': 'own_kickoff_recovery',
                      'lateral_rusher_player_id': 'lateral_rusher', 'lateral_receiver_player_id': 'lateral_receiver',
                      'lateral_punt_returner_player_id': 'lateral_punt_returner',
                      'lateral_kickoff_returner_player_id': 'lateral_kickoff_returner',
                      'lateral_interception_player_id': 'lateral_interception',
                      'kickoff_returner_player_id': 'kickoff_returner', 'kicker_player_id': 'kicker',
                      'interception_player_id': 'interception', 'half_sack_2_player_id': 'half_sack_2',
                      'half_sack_1_player_id': 'half_sack_1', 'fumbled_2_player_id': 'fumbled_2',
                      'fumbled_1_player_id': 'fumbled_1', 'forced_fumble_player_2_player_id': 'forced_fumble_player_2',
                      'forced_fumble_player_1_player_id': 'forced_fumble_player_1',
                      'fumble_recovery_2_player_id': 'fumble_recovery_2',
                      'fumble_recovery_1_player_id': 'fumble_recovery_1', 'blocked_player_id': 'blocked'}


def rename_pbp_columns(pbp_df):
    logger.info("moving play_id to play_counter, and creating a joinable play_id key")
    pbp_df['play_counter'] = pbp_df['play_id']
    pbp_df['play_id'] = pbp_df["game_id"].astype(str) + "_" + pbp_df["play_counter"].astype(str)
    pbp_df['play_type'] = pbp_df['play_type'].fillna(pbp_df['play_type_nfl'].str.lower())

    assert_and_alert(len(get_duplicates_by_key(pbp_df, 'play_id')) == 0,
                     msg="Unexpected duplicate keys found creating play_id from game_id and play_id")


def conform_actions(df: DataFrame):
    logger.info(
        "Conform key actions like pass, rush, kickoff, etc. and add a single category field called actions... ")
    if 'action' not in df:
        df['action'] = None

    df.drop(columns=['pass_attempt'])  # this column is either inconsistent, mundane, or I don't understand it
    df.rename(columns={'pass': 'pass_attempt'})

    df.drop(columns=['rush_attempt'])  # this column is either inconsistent, mundane, or I don't understand it
    df.rename(columns={'rush': 'rush_attempt'})

    df.loc[(df['pass_attempt'] == 1) &
           (df['rush_attempt'] == 0) &
           (df['kickoff_attempt'] == 0) &
           (df['punt_attempt'] == 0) &
           (df['field_goal_attempt'] == 0) &
           (df['extra_point_attempt'] == 0), "action"] = 'pass'

    df.loc[(df['pass_attempt'] == 0) &
           (df['rush_attempt'] == 1) &
           (df['kickoff_attempt'] == 0) &
           (df['punt_attempt'] == 0) &
           (df['field_goal_attempt'] == 0) &
           (df['extra_point_attempt'] == 0), "action"] = 'rush'

    df.loc[(df['pass_attempt'] == 0) &
           (df['rush_attempt'] == 0) &
           (df['kickoff_attempt'] == 1) &
           (df['punt_attempt'] == 0) &
           (df['field_goal_attempt'] == 0) &
           (df['extra_point_attempt'] == 0), "action"] = 'kickoff'

    df.loc[(df['pass_attempt'] == 0) &
           (df['rush_attempt'] == 0) &
           (df['kickoff_attempt'] == 0) &
           (df['punt_attempt'] == 1) &
           (df['field_goal_attempt'] == 0) &
           (df['extra_point_attempt'] == 0), "action"] = 'punt'

    df.loc[(df['pass_attempt'] == 0) &
           (df['rush_attempt'] == 0) &
           (df['kickoff_attempt'] == 0) &
           (df['punt_attempt'] == 0) &
           (df['field_goal_attempt'] == 1) &
           (df['extra_point_attempt'] == 0), "action"] = 'field_goal'

    df.loc[(df['pass_attempt'] == 0) &
           (df['rush_attempt'] == 0) &
           (df['kickoff_attempt'] == 0) &
           (df['punt_attempt'] == 0) &
           (df['field_goal_attempt'] == 0) &
           (df['extra_point_attempt'] == 1), "action"] = 'extra_point'

    df.loc[(df['pass_attempt'] == 0) &
           (df['rush_attempt'] == 0) &
           (df['kickoff_attempt'] == 0) &
           (df['punt_attempt'] == 0) &
           (df['field_goal_attempt'] == 0) &
           (df['extra_point_attempt'] == 0) &
           # (df['timeout'] == 0) &
           # (df['penalty'] == 0) &
           # (df['qb_spike'] == 0) &
           (df['desc'].str.lower().str.contains("extra point")), ["extra_point_attempt", "action"]] = [1, "extra_point"]

    df.loc[(df['pass_attempt'] == 0) &
           (df['rush_attempt'] == 0) &
           (df['kickoff_attempt'] == 0) &
           (df['punt_attempt'] == 0) &
           (df['field_goal_attempt'] == 0) &
           (df['extra_point_attempt'] == 0) &
           # (df['timeout'] == 0) &
           # (df['penalty'] == 0) &
           # (df['qb_spike'] == 0) &
           (df['desc'].str.lower().str.contains("penalty")), ["penalty", "action"]] = [1, "aborted-penalty"]

    df.loc[(df['pass_attempt'] == 0) &
           (df['rush_attempt'] == 0) &
           (df['kickoff_attempt'] == 0) &
           (df['punt_attempt'] == 0) &
           (df['field_goal_attempt'] == 0) &
           (df['extra_point_attempt'] == 0) &
           # (df['timeout'] == 0) &
           # (df['penalty'] == 0) &
           # (df['qb_spike'] == 0) &
           (df['desc'].str.lower().str.contains("timeout")), ["timeout", "action"]] = [1, "aborted-timeout"]

    df.loc[(df['pass_attempt'] == 0) &
           (df['rush_attempt'] == 0) &
           (df['kickoff_attempt'] == 0) &
           (df['punt_attempt'] == 0) &
           (df['field_goal_attempt'] == 0) &
           (df['extra_point_attempt'] == 0) &
           (df['timeout'] == 0) &
           (df['penalty'] == 0), "action"] = "clock-event"

    df.rename(columns={"yards_gained": "offense_yards", "return_yards": "defense_yards"}, inplace=True)

    actions = [
        'season', 'game_id', 'week', 'play_type', 'posteam', 'defteam',
        'play_id',
        'play_counter',
        'old_game_id',
        'action',
        'pass_attempt',
        'rush_attempt',
        'kickoff_attempt',
        'punt_attempt',
        'field_goal_attempt',
        'extra_point_attempt',
        'offense_yards',
        'defense_yards', 'timeout', 'penalty', 'qb_spike',
        'desc'
    ]

    actions_df = df[actions].copy()
    actions_df['offense_yards'] = actions_df['offense_yards'].fillna(0)
    actions_df['defense_yards'] = actions_df['defense_yards'].fillna(0)

    return actions_df


def create_player_events(df: DataFrame) -> DataFrame:
    logger.info("Create stats for pbp player involvement by play ...")

    df['rusher_player_id'] = df['rusher_player_id'].fillna("rusher")  # merge redundant info
    df['passer_player_id'] = df['passer_id'].fillna("passer")  # merge redundant info

    contributions_df = pd.DataFrame(columns=['season', 'week', 'game_id', 'play_id', 'player_id', 'event'])

    for column_name, contribution in player_id_columns.items():
        foo = df.loc[
            (df[column_name].notna() & (df[column_name].str.match(r"^\d{2}-\d+"))), ['season', 'week', 'game_id',
                                                                                     'play_id', column_name]]
        foo.rename(columns={column_name: 'player_id'}, inplace=True)
        foo['event'] = contribution
        lfoo = len(foo)
        if lfoo > 0:
            logger.debug(f"Adding {len(foo)} {column_name} individual stats.... ")
            contributions_df = pd.concat([contributions_df, foo], ignore_index=True)


    return contributions_df


def validate_dimension(df, name, target_size=None):
    logger.info(f"Validate {name} dimension ...")
    nac = df.isnull().sum().sum()
    assert_and_alert(nac == 0, msg=f"There are {nac} nulls in the {name} dataset!!!")

    if target_size is not None:
        assert_and_alert(len(df) == target_size, msg=f"{name} df size {len(df)} is not equal to {target_size}")


def perform_specific_imputes(df: DataFrame):
    logger.info("impute non binary pbp columns ...")

    df.drive_end_transition = df.drive_end_transition.fillna(df.series_result.str.upper())

    numeric_columns = df.select_dtypes(include='number').columns.tolist()
    impute_columns(df, value=0, columns=numeric_columns)

    impute_columns(df, value='0:00', columns=[
        'drive_game_clock_start', 'drive_game_clock_end', 'drive_time_of_possession', 'drive_play_id_ended'
    ])

    object_columns = df.select_dtypes(include='object').columns.tolist()
    impute_columns(df, columns=object_columns, value='NA')


def clean_columns(df: DataFrame):
    # perform imputes
    impute_columns(df, binary_columns)  # all 'binary' columns
    perform_specific_imputes(df)  # everything else

    # conform play_id into a joinable key and other initial renames
    rename_pbp_columns(df)


def dimensionalize_plays(pbp_df):
    data_size = len(pbp_df)

    # make a copy of the df
    df = pbp_df.copy()

    # perform imputes
    clean_columns(df)

    # separate the key 'play call' and milestone columns into a fact dimension
    actions_df = conform_actions(df)
    validate_dimension(actions_df, "actions", data_size)

    # separate information pertaining to the 'drive' into a fact dimension
    drive_df = create_dimension(df, columns=drive_columns, category="drive", keys=['play_id'])
    validate_dimension(drive_df, "drive_df", data_size)

    # separate information about the clock, and other game admin data inot a 'fact' dimension
    situation_df = create_dimension(df, columns=situational_columns, category="situations", keys=['play_id'])
    validate_dimension(situation_df, "situation_df", data_size)

    # separate metrics, such as score, yards, etc into a separate dim
    play_metrics_df = create_dimension(df, columns=play_core, category="metrics", keys=['play_id'])
    play_metrics_df["two_point_conv_result"].fillna('NA')
    play_metrics_df["penalty_yards"].fillna(0, inplace=True)
    validate_dimension(play_metrics_df, "play_metrics_df", data_size)

    # create records for the 'who' for each play - e,g, possession team, defence team.
    play_identities_df = create_dimension(df, columns=play_identities, category="identities", keys=['play_id'])
    validate_dimension(play_metrics_df, "play_metrics_df", data_size)

    # create game-level data e.g. game date, home and away teams, etc.
    game_df = create_dimension(
        df, columns=game_columns,
        keys=['season', 'game_id'], category='game')

    game_df.drop_duplicates(inplace=True)

    unique_games = pbp_df['game_id'].nunique()
    found_games = game_df['game_id'].nunique()
    assert_and_alert(found_games == unique_games,
                     msg=f"mis-count of games dimension. Expected {unique_games}, got {found_games} ...")
    validate_dimension(game_df, "games")

    # create records for each player's specific contribution, e.g. touchdowns, sacks, etc.
    players_df = create_player_events(df)

    analytics_df = create_dimension(df,
                                    columns=analytics_columns,
                                    category="analytics", keys=['season', 'week', 'game_id', 'play_id'])

    results = {
        'play_actions': actions_df,
        'game_drive': drive_df,
        'play_analytics': analytics_df,
        'play_situations': situation_df,
        'play_metrics': play_metrics_df,
        'play_identities': play_identities_df,
        'player_events': players_df,
        'game_info': game_df
    }

    return results


def test_loader():
    pbp_df = pd.read_csv("../../output/playbyplay_2021.csv", low_memory=False)
    results = dimensionalize_plays(pbp_df)
    load_dims_to_db(results)
    print("Done")


def test_job_pbp_main():
    pbp_df = pd.read_csv("../../output/playbyplay_2021.csv", low_memory=False)
    results = dimensionalize_plays(pbp_df)
    print("Done")
