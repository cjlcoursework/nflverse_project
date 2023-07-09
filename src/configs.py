import logging
from typing import Optional, Callable, Union
import pandas as pd

configurations = {
    'project_name': "nfl",
    'file_type': "parquet",
    'pbp_url': 'https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_{year}.{file_type}',
    'pbp_participation_url': 'https://github.com/nflverse/nflverse-data/releases/download/pbp_participation/pbp_participation_{year}.{file_type}',
    'injuries_url': 'https://github.com/nflverse/nflverse-data/releases/download/injuries/injuries_{year}.{file_type}',
    'player_stats_url': 'https://github.com/nflverse/nflverse-data/releases/download/player_stats/player_stats.{file_type}',
    'advstats_url': 'https://github.com/nflverse/nflverse-data/releases/download/pfr_advstats/advstats_season_{stats_type}.{file_type}',
    'advstats_stat_types': ['def', 'pass', 'rush', 'pass', 'rec'],
    'players_url': 'https://github.com/nflverse/nflverse-data/releases/download/players/players.{file_type}',
    'ng_stats_url': 'https://github.com/nflverse/nflverse-data/releases/download/nextgen_stats/ngs_{year}_{stat_type}.csv.gz',
    'ng_stats_types': ['passing', 'rushing', 'receiving'],
    'output_directory': "/Users/christopherlomeli/Source/courses/datascience/Springboard/capstone/NFL/NFLVersReader/output",
    'data_directory': "/Users/christopherlomeli/Source/courses/datascience/Springboard/capstone/NFL/NFLVersReader/data/nfl",

    'model_directory': "/Users/christopherlomeli/Source/courses/datascience/Springboard/capstone/NFL/NFLVersReader/model",

    'schema_directory': "/Users/christopherlomeli/Source/courses/datascience/Springboard/capstone/NFL/NFLVersReader/schemas",
    'connection_string': 'postgresql://postgres:chinois1@localhost',
    'positions_data': "/Users/christopherlomeli/Source/courses/datascience/Springboard/capstone/NFL/nfl_capstone/data"
                      "/raw/positions.csv"
}


def get_read_function(file_path: str) -> Optional[Union[Callable[..., pd.DataFrame]]]:
    file_type = None

    for x in tested_extensions:
        if str(file_path).lower().endswith(x):
            file_type = x
            break

    if file_type is None:
        return None

    return tested_extensions[file_type]


def get_config(name, default=None):
    if name in configurations:
        return configurations[name]
    return default


def configure_logging(name='pbp_logger'):
    # Create the logger if it doesn't exist
    logger = logging.getLogger(name)
    logger.propagate = False

    # Check if handlers are already present
    if not logger.handlers:

        # Create handlers for different logging levels
        error_handler = logging.StreamHandler()
        info_handler = logging.StreamHandler()

        # Set the logging level for each handler
        error_handler.setLevel(logging.ERROR)
        info_handler.setLevel(logging.INFO)

        # Create formatters for different logging formats
        error_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s - Line %(lineno)d - %(message)s')
        info_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        # Set the formatters for each handler
        error_handler.setFormatter(error_formatter)
        info_handler.setFormatter(info_formatter)

        # Add the handlers to the logger
        logger.addHandler(error_handler)
        logger.addHandler(info_handler)

    return logger


tested_extensions = {
    'csv': pd.read_csv,
    'csv.gz': pd.read_csv,
    'parquet': pd.read_parquet
}

action_columns = [
    'season',
    'game_id',
    'week',
    'drive',
    'down',
    'drive_id',
    'play_id',
    'play_counter',
    'play_type',
    'qtr',
    'action',
    'half_seconds_remaining',
    'game_seconds_remaining',
    'ydstogo',
    'posteam',
    'posteam_score',
    'posteam_score_post',
    'posteam_timeouts_remaining',
    'defteam',
    'defteam_score',
    'defteam_score_post',
    'defteam_timeouts_remaining',
    'offense_yards_gained',
    'defense_yards_gained',
    'old_game_id',
    'pass_attempt',
    'rush_attempt',
    'kickoff_attempt',
    'punt_attempt',
    'field_goal_attempt',
    'extra_point_attempt',
    'timeout',
    'penalty',
    'qb_spike',
    'desc'
]

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
                     "pass_oe"]

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
    "away_score",
    "away_coach",
    "home_coach"
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

player_id_columns = [
    ("fumbled_2_player_id", "fumble", "offense"),
    ("forced_fumble_player_2_player_id", "fumble", "offense"),
    ("forced_fumble_player_1_player_id", "fumble", "offense"),
    ("fumbled_1_player_id", "fumble", "offense"),
    ("interception_player_id", "interception", "defense"),
    ("lateral_interception_player_id", "interception", "defense"),
    ("own_kickoff_recovery_player_id", "own_kickoff_recovery", "offense"),
    ("qb_hit_1_player_id", "qb_hit", "defense"),
    ("qb_hit_2_player_id", "qb_hit", "defense"),
    ("half_sack_1_player_id", "sack", "defense"),
    ("half_sack_2_player_id", "sack", "defense"),
    ("sack_player_id", "sack", "defense"),
    ("safety_player_id", "safety", "defense"),
    ("tackle_for_loss_1_player_id", "tackle", "defense"),
    ("assist_tackle_2_player_id", "tackle", "defense"),
    ("solo_tackle_2_player_id", "tackle", "defense"),
    ("assist_tackle_3_player_id", "tackle", "defense"),
    ("assist_tackle_1_player_id", "tackle", "defense"),
    ("tackle_with_assist_1_player_id", "tackle", "defense"),
    ("solo_tackle_1_player_id", "tackle", "defense"),
    ("assist_tackle_4_player_id", "tackle", "defense"),
    ("td_player_id", "touchdown", "offense")
]

