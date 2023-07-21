from typing import Dict

import pandas as pd
import warnings
from pandas import DataFrame
from src import *
from src.nfl.inline_validation import perform_inline_play_action_tests
from src.util.utils import assert_and_alert, impute_columns, get_duplicates_by_key, create_dimension

warnings.filterwarnings('ignore')
logger = configure_logging("pbp_logger")


def calculate_yards_to_goal(row: pd.Series) -> int:
    """
    Calculate yards to the goal line based on the yard line and posteam information.

    Parameters:
        row (pd.Series): A Series representing a single row of the DataFrame.

    Returns:
        int: The number of yards to the goal line.

    This function takes a row from the DataFrame and calculates the yards to the goal line
    based on the 'yrdln' (yard line) and 'posteam' (team possessing the ball) columns.

    If the 'yrdln' is missing or improperly formatted, the function returns 0.

    Example:
        If 'yrdln' is 'OPP 25' and 'posteam' is 'OPP', the function will return 75,
        as the distance to the opponent's goal line is 75 yards.
    """
    yard_line = row['yrdln']
    if yard_line is None:
        return 0
    yard_line_parts = yard_line.split()
    if len(yard_line_parts) != 2:
        return 0
    side_of_field = yard_line_parts[0]
    numeric_value = int(yard_line_parts[1])

    if row['posteam'] == side_of_field:
        return 100 - numeric_value
    else:
        return numeric_value


def conform_pbp_actions(df: DataFrame) -> DataFrame:
    """
    - Create an action column based on various elements in the dataframe
    - Get the action_columns list from configuration to filter only the actions we need in the play_actions dimension
    - perform some minor cleanup of action-related columns

    Parameters:
        df (pd.DataFrame): The original play-by-play dataset.

    Returns:
        df (pd.DataFrame): A new dataset with just the columns we want for the play_actions dimension

    todo: come up with a more elegant ways of doing this - it's pretty brute force
    """
    logger.info(
        "Conform key actions like pass, rush, kickoff, etc.... ")
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
    """
    Create a contributions dataframe that consists of all player events
    Columns in the input dataframe have columns like
        - fumbled_2_player_id = 0001
        - qb_hit_1_player_id = 0003

    The config.player_id_columns array has instructions for how to pivot these:
        e.g.
        - ("fumbled_2_player_id", "fumble", "offense")
        -("qb_hit_1_player_id", "qb_hit", "defense")

    The contributions_df dataframe will contain:
        player_id=0001, event=fumble, lineup=offense
        player_id=0003, event=qb_hit, lineup=defense

    Configurations:
        config.player_id_columns

    Parameters:
        df (pd.DataFrame): The original play-by-play dataset.

    Returns:
        contributions_df (pd.DataFrame): A new dataset with player events
    """

    logger.info("Create stats for pbp player involvement by play ...")

    df['rusher_player_id'] = df['rusher_player_id'].fillna("rusher")  # merge redundant info
    df['passer_player_id'] = df['passer_id'].fillna("passer")  # merge redundant info

    contributions_df = pd.DataFrame(
        columns=['season', 'week', 'game_id', 'posteam', 'defteam', 'play_counter', 'player_id', 'event'])

    #
    for column_name, contribution, lineup in player_id_columns:
        # find non-null columns with this column name
        foo = df.loc[
            (df[column_name].notna() & (df[column_name].str.lower().str.match(r"^\d{2}-\d+"))),
            ['season', 'week', 'game_id', 'play_id', column_name]]

        # create a contributions_df row
        foo.rename(columns={column_name: 'player_id'}, inplace=True)
        foo['event'] = contribution
        foo['lineup'] = lineup
        len_foo = len(foo)
        if len_foo > 0:
            # add to the contributions_df
            logger.debug(f"Adding {len(foo)} {column_name} individual stats.... ")
            contributions_df = pd.concat([contributions_df, foo], ignore_index=True)

    return contributions_df


def validate_dimension(df: DataFrame, name: str, target_size: int =None):
    """
    Validate that a dataframe
    Configurations:
        config.player_id_columns

    Parameters:
        df (pd.DataFrame): A slice of the play-by-play dataset.
        name (string): this is just an identifier for the dataset - just for logging purposes
        target_size (int): how many rows we expect to see in this df

    Returns:
        None or alert
    """
    logger.info(f"Validate {name} dimension ...")
    nac = df.isnull().sum().sum()
    assert_and_alert(nac == 0, msg=f"There are {nac} nulls in the {name} dataset!!!")

    if target_size is not None:
        assert_and_alert(len(df) == target_size, msg=f"{name} df size {len(df)} is not equal to {target_size}")


def perform_specific_imputes(df: DataFrame):
    """
    impute null elements - this is not a generic function.  It's specific to nfl verse cleanup

    Parameters:
        df (pd.DataFrame): An nflverse dataset.

    Returns:
        None - the input dataframe is changed in place  todo - this should return the changed df instead
    """
    logger.info("impute non binary pbp columns ...")

    # conform drive_end_transition values to upper case
    df.drive_end_transition = df.drive_end_transition.fillna(df.series_result.str.upper())

    # impute missing numeric columns to zero
    # careful here - we have already cleaned values that should not be 0
    numeric_columns = df.select_dtypes(include='number').columns.tolist()
    impute_columns(df, value=0, columns=numeric_columns)

    # impute quasi time elements
    impute_columns(df, value='0:00', columns=[
        'drive_game_clock_start', 'drive_game_clock_end', 'drive_time_of_possession', 'drive_play_id_ended'
    ])

    # impute missing object columns to "NA"
    # careful here - we have already cleaned values that should not be NA
    object_columns = df.select_dtypes(include='object').columns.tolist()
    impute_columns(df, columns=object_columns, value='NA')


def conform_pbp_columns(pbp_df):
    """
    clean and conform columns that we could use as join keys

    Parameters:
        pbp_df (pd.DataFrame): The play-by-play (pbp) dataset from nflverse

    Returns:
        None - the input dataframe is changed in place  todo - this should return the changed df instead
        alert if there are unexpected duplicates in the resulting dataset
    """
    logger.info("moving play_id to play_counter, and creating a joinable play_id key")
    pbp_df['play_counter'] = pbp_df['play_id']
    pbp_df['drive_id'] = pbp_df["game_id"].astype(str) + "_" + pbp_df["drive"].astype(int).astype(str)
    pbp_df['play_id'] = pbp_df["game_id"] + "_" + pbp_df["play_counter"].astype(str)
    pbp_df['play_type'] = pbp_df['play_type'].fillna(pbp_df['play_type_nfl'].str.lower())

    assert_and_alert(len(get_duplicates_by_key(pbp_df, 'play_id')) == 0,
                     msg="Unexpected duplicate keys found creating play_id from game_id and play_id")


def clean_pbp_columns(df: DataFrame):
    """
    This is a wrapper for most of the impute functions for play-by-play dataset

    Parameters:
        df (pd.DataFrame): The play-by-play (pbp) dataset from nflverse

    Returns:
        None - the input dataframe is changed in place
    """

    # perform imputes on columns we identiy as binary (1 or 0)
    impute_columns(df, binary_columns)  # all 'binary' columns

    perform_specific_imputes(df)  # everything else

    # conform play_id into a joinable key and other initial renames
    conform_pbp_columns(df)


def transform_pbp(pbp_df: DataFrame) -> Dict:
    """
     This is a workflow that dos some initial cleanup, then sequentially calls functions that divide the play-by-play (pbp)
     into smaller dimensions and stored each dimension in a Dict

    Parameters:
        pbp_df (pd.DataFrame): The play-by-play (pbp) dataset from nflverse

    Returns:
        a dict of table names and dataframes:

    Example:
             The pbp (play-by-play) dataset has one row for each play in a game.
             But there is only one game for many plays so pbp has the same game score for every play
              This is the case for several game_related columns (game date, score, arena, home and away teams, etc.)
             So we are shrinking game-level data into a unique game_df
    """

    data_size = len(pbp_df)

    # make a copy of the df
    df = pbp_df.copy()

    # perform imputes
    clean_pbp_columns(df)

    # separate the key 'play call' and milestone columns into a fact dimension
    actions_df = conform_pbp_actions(df)
    validate_dimension(actions_df, "actions", data_size)
    perform_inline_play_action_tests(actions_df)

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
                                    category="analytics",
                                    keys=['season', 'week', 'game_id', 'home_team', 'away_team', 'posteam', 'defteam',
                                          'play_id', 'play_counter'])

    # pass back all the dimensions we've created, with the table name of the dataset and the dataframe itslef
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


"""
This is not called - it's a sanity check for testing and experimenting 
"""
if __name__ == '__main__':
    pbp_df = pd.read_parquet(
        "/Users/christopherlomeli/Source/courses/datascience/Springboard/capstone/NFL/NFLVersReader/output/pbp/pbp_2016.parquet")
    results = transform_pbp(pbp_df)
    # load_dims_to_db(results)
    print("Done")
