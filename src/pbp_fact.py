from typing import Dict

import pandas as pd
from pandas import DataFrame

from src import *
import warnings

from src.db_utils import load_dims_to_db
from src.utils import assert_and_alert, impute_columns, get_duplicates_by_key, create_dimension

warnings.filterwarnings('ignore')
logger = configure_logging("pbp_logger")


# Function to calculate the new numeric column
def calculate_yards_to_goal(row):
    yardline = row['yrdln']
    if yardline is None:
        return 0
    yardline_parts =yardline.split()
    if len(yardline_parts) != 2:
        return 0
    side_of_field = yardline_parts[0]
    numeric_value = int(yardline_parts[1])

    if row['posteam'] == side_of_field:
        return 100 - numeric_value
    else:
        return numeric_value


def conform_pbp_actions(df: DataFrame):
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
           (df['desc'].str.lower().str.contains("extra point")), ["extra_point_attempt", "action"]] = [1, "extra_point"]

    df.loc[(df['pass_attempt'] == 0) &
           (df['rush_attempt'] == 0) &
           (df['kickoff_attempt'] == 0) &
           (df['punt_attempt'] == 0) &
           (df['field_goal_attempt'] == 0) &
           (df['extra_point_attempt'] == 0) &
           (df['desc'].str.lower().str.contains("penalty")), ["penalty", "action"]] = [1, "aborted-penalty"]

    df.loc[(df['pass_attempt'] == 0) &
           (df['rush_attempt'] == 0) &
           (df['kickoff_attempt'] == 0) &
           (df['punt_attempt'] == 0) &
           (df['field_goal_attempt'] == 0) &
           (df['extra_point_attempt'] == 0) &
           (df['desc'].str.lower().str.contains("timeout")), ["timeout", "action"]] = [1, "aborted-timeout"]

    df.loc[(df['pass_attempt'] == 0) &
           (df['rush_attempt'] == 0) &
           (df['kickoff_attempt'] == 0) &
           (df['punt_attempt'] == 0) &
           (df['field_goal_attempt'] == 0) &
           (df['extra_point_attempt'] == 0) &
           (df['timeout'] == 0) &
           (df['penalty'] == 0), "action"] = "clock-event"

    df["offense_yards_gained"] = df["yards_gained"].fillna(0)
    df["defense_yards_gained"] = df["return_yards"].fillna(0)

    df["action"] = df["action"].fillna(df["play_type"])
    df['yards_to_goal'] = df.apply(calculate_yards_to_goal, axis=1)

    actions_df = df[action_columns].copy()

    return actions_df


def create_player_events(df: DataFrame) -> DataFrame:
    logger.info("Create stats for pbp player involvement by play ...")

    df['rusher_player_id'] = df['rusher_player_id'].fillna("rusher")  # merge redundant info
    df['passer_player_id'] = df['passer_id'].fillna("passer")  # merge redundant info

    contributions_df = pd.DataFrame(columns=['season', 'week', 'game_id', 'posteam', 'defteam', 'play_counter', 'player_id', 'event'])

    for column_name, contribution, lineup in player_id_columns:
        foo = df.loc[
            (df[column_name].notna() & (df[column_name].str.lower().str.match(r"^\d{2}-\d+"))), ['season', 'week',
                                                                                                'game_id', 'play_id',
                                                                                                column_name]]
        foo.rename(columns={column_name: 'player_id'}, inplace=True)
        foo['event'] = contribution
        foo['lineup'] = lineup
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


def conform_pbp_columns(pbp_df):
    logger.info("moving play_id to play_counter, and creating a joinable play_id key")
    pbp_df['play_counter'] = pbp_df['play_id']
    pbp_df['drive_id'] = pbp_df["game_id"].astype(str) + "_" + pbp_df["drive"].astype(int).astype(str)
    pbp_df['play_id'] = pbp_df["game_id"] + "_" + pbp_df["play_counter"].astype(str)
    pbp_df['play_type'] = pbp_df['play_type'].fillna(pbp_df['play_type_nfl'].str.lower())

    assert_and_alert(len(get_duplicates_by_key(pbp_df, 'play_id')) == 0,
                     msg="Unexpected duplicate keys found creating play_id from game_id and play_id")


def clean_pbp_columns(df: DataFrame):
    # perform imputes
    impute_columns(df, binary_columns)  # all 'binary' columns

    perform_specific_imputes(df)  # everything else
    # conform play_id into a joinable key and other initial renames
    conform_pbp_columns(df)


def transform_pbp(pbp_df):
    data_size = len(pbp_df)

    # make a copy of the df
    df = pbp_df.copy()

    # perform imputes
    clean_pbp_columns(df)

    # separate the key 'play call' and milestone columns into a fact dimension
    actions_df = conform_pbp_actions(df)
    validate_dimension(actions_df, "actions", data_size)

    # separate information pertaining to the 'drive' into a fact dimension
    drive_df = create_dimension(df, columns=drive_columns, category="drive",
                                keys=['drive_id', 'game_id', 'play_id', 'play_counter'])
    validate_dimension(drive_df, "drive_df", data_size)

    # separate information about the clock, and other game admin data inot a 'fact' dimension
    situation_df = create_dimension(df, columns=situational_columns, category="situations",
                                    keys=['drive_id', 'game_id', 'play_id', 'play_counter'])
    validate_dimension(situation_df, "situation_df", data_size)

    # separate metrics, such as score, yards, etc into a separate dim
    play_metrics_df = create_dimension(df, columns=play_core, category="metrics",
                                       keys=['season', 'week', 'drive_id', 'game_id', 'play_id', 'play_counter'])
    play_metrics_df["two_point_conv_result"].fillna('NA')
    play_metrics_df["penalty_yards"].fillna(0, inplace=True)
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
                                    category="analytics", keys=['season', 'week', 'game_id', 'home_team', 'away_team', 'posteam', 'defteam','play_id', 'play_counter'])

    results = {
        'play_actions': actions_df,
        'game_drive': drive_df,
        'play_analytics': analytics_df,
        'play_situations': situation_df,
        'play_metrics': play_metrics_df,
        'player_events': players_df,
        'game_info': game_df
    }

    return results


if __name__ == '__main__':
    pbp_df = pd.read_parquet(
        "/Users/christopherlomeli/Source/courses/datascience/Springboard/capstone/NFL/NFLVersReader/output/pbp/pbp_2016.parquet")
    results = transform_pbp(pbp_df)
    load_dims_to_db(results)
    print("Done")



