import numpy as np
import os
from pandas import DataFrame
from typing import List, Dict

from src import *
from src.nfl.inline_validation import perform_inline_play_action_tests
from src.util.utils import assert_and_alert
from src.util.utils_db import store_df

logger = configs.configure_logging("pbp_logger")
logger.setLevel(logging.INFO)


def drop_extras(df: pd.DataFrame):
    """
    Drop the extra columns that were created during the merge process.
    """
    drops = ['team']
    for col in df.columns.values:
        if str(col).endswith("_y") or str(col).endswith("_x"):
            drops.append(col)
    if len(drops) > 0:
        df.drop(columns=drops, inplace=True)


def merge_powers(action_df: DataFrame, powers_df: DataFrame, left_on: List[str], renames: Dict =None, msg: str ='play_counter') -> DataFrame:
    """
    Merge the offensive stats with the play-by-play events and game info.
    Parameters:
        action_df (DataFrame): The game info data.
        powers_df (DataFrame): The offensive stats data.
        left_on (list): The columns to merge on.
        renames (dict): The columns to rename.
        msg (str): The message to display in the log.

        Returns:
        _df (DataFrame): The offensive stats data with the play-by-play events and game info merged in.

    """
    expected_shape = action_df.shape
    _df = pd.merge(action_df, powers_df, left_on=left_on, right_on=['season', 'week', 'team']).drop_duplicates()
    drop_extras(_df)
    _df.rename(columns=renames, inplace=True)

    perform_inline_play_action_tests(_df, msg=msg)
    assert_and_alert(expected_shape[0] == _df.shape[0],
                     msg=f"merge of actions to offense power "
                         f"changed the row count {action_df.shape} + {powers_df.shape} ==> {_df.shape}")
    return _df


def load_file(directory: str, file: str) -> DataFrame:
    """
    Load a file from the data directory.
    Parameters:
        directory (str): The directory to load the file from.
        file (str): The name of the file to load.
    Returns:
        df (DataFrame): The loaded data.
    """
    logger.info(f"Reading from {file}")

    if not os.path.exists(directory):
        os.makedirs(directory)

    full_path = os.path.join(directory, f"{file}.parquet")
    df = pd.read_parquet(full_path)
    return df


def load_and_merge_weekly_features():
    """
    Load the weekly features and merge them into a single game dataset.
    Returns:
        df (DataFrame): The merged stats data.
    """
    logger.info("loading weekly features into a single game dataset...")
    directory = get_config('data_directory')

    pbp_actions_df = load_file(directory,
                               get_config('action_week_prep'))

    offense_powers_df = load_file(directory,
                                  get_config('offense_week_features'))

    defense_powers_df = load_file(directory,
                                  get_config('defense_week_features'))

    logger.info("merge stats into play_actions...")
    df = merge_powers(pbp_actions_df, offense_powers_df, left_on=['season', 'week', 'posteam'],
                      renames={'offense_power': 'offense_op'}, msg="merging offense_OP")
    
    df = merge_powers(df, defense_powers_df, left_on=['season', 'week', 'posteam'], renames={'defense_power': 'offense_dp'},
                      msg="merging offense_DP")
    
    df = merge_powers(df, offense_powers_df, left_on=['season', 'week', 'defteam'], renames={'offense_power': 'defense_op'},
                      msg="merging defense_OP")
    
    df = merge_powers(df, defense_powers_df, left_on=['season', 'week', 'defteam'], renames={'defense_power': 'defense_dp'},
                      msg="merging defense_DP")
    
    assert_and_alert(pbp_actions_df.shape[0]==df.shape[0], 
                     msg=f"merged row count should equal original: "
                         f"original {pbp_actions_df.shape}, "
                         f"after merge {df.shape} ")
    return df    


def aggregate_game_stats(df: DataFrame):
    """
    Aggregate the weekly stats into a single game dataset
    that has the stats for both the offense and defense for each team.

    Parameters:
        df (DataFrame): The merged weekly stats data.
    Returns:
        games_df (DataFrame): The game stats data.

    """

    logger.info("aggregate game dataset weekly stats by season, week, team...")

    # add a point spread field
    df['point_spread'] = df['posteam_final_score'] - df['defteam_final_score']

    # Group by season, week, game_id
    #   also by defense and offense so separate row are created for each team
    grouped_df = df.groupby(['season', 'week', 'game_id', 'posteam', 'defteam']).agg(
        drive_count=('drive', 'count'),
        first_downs=('down', lambda x: (x == 1).sum()),
        point_spread=('point_spread', 'max'),  # Calculate the point spread explicitly
        team_final_score=('posteam_final_score', 'max'),
        opposing_team_final_score=('defteam_final_score', 'max'),
        yards_gained=('yards_gained', 'sum'),
        pass_attempts=('pass_attempt', 'sum'),
        rush_attempts=('rush_attempt', 'sum'),
        kickoff_attempt=('kickoff_attempt', 'sum'),
        punt_attempt=('punt_attempt', 'sum'),
        field_goal_attempt=('field_goal_attempt', 'sum'),
        two_point_attempt=('two_point_attempt', 'sum'),
        extra_point_attempt=('extra_point_attempt', 'sum'),
        timeout=('timeout', 'sum'),
        penalty=('penalty', 'sum'),
        qb_spike=('qb_spike', 'sum'),
        team_offense_power=('offense_op', 'mean'),
        team_defense_power=('offense_dp', 'mean'),
        opposing_team_offense_power=('defense_op', 'mean'),
        opposing_team_defense_power=('defense_dp', 'mean')
    )
    
    # Reset the index to transform the grouped DataFrame back to a regular DataFrame
    grouped_df.reset_index(inplace=True)
    
    # Select the desired columns for the final result
    games_df = grouped_df[['season', 'week', 'game_id', 'posteam', 'defteam',
                           'team_offense_power', 'team_defense_power', 'opposing_team_offense_power',
                           'opposing_team_defense_power',
                           'point_spread', 'drive_count', 'first_downs', 'team_final_score',
                           'opposing_team_final_score', 'yards_gained', 'pass_attempts', 'rush_attempts',
                           'kickoff_attempt', 'punt_attempt', 'field_goal_attempt', 'two_point_attempt',
                           'extra_point_attempt', 'timeout', 'penalty', 'qb_spike']]
    
    games_df.rename(columns={'posteam': 'team', 'defteam': 'opposing_team'}, inplace=True)
    
    # Create a new column 'loss_tie_win' based on conditions
    games_df['loss_tie_win'] = np.where(
        games_df['point_spread'] > 0, 2,
        np.where(
            games_df['point_spread'] < 0, 0, 1)
    )
    
    games_df['team_power_sum'] = games_df['team_offense_power'] + games_df['team_defense_power']
    games_df['opposing_team_power_sum'] = games_df['opposing_team_offense_power'] + games_df['opposing_team_defense_power']
    games_df['power_difference'] = games_df['team_power_sum'] - games_df['opposing_team_power_sum']
    games_df['point_spread'] = games_df['point_spread'].astype('float')
    games_df[['team_offense_power', 'team_defense_power', 'opposing_team_offense_power', 'opposing_team_defense_power',
              'team_power_sum', 'opposing_team_power_sum', 'power_difference']].head()

    return games_df


def merge_team_week_features():
    """
    Merge the weekly features into a single game dataset.
    """
    df = load_and_merge_weekly_features()
    game_df = aggregate_game_stats(df)

    output_file = get_config('game_stats')
    store_df(game_df, output_file)


if __name__ == '__main__':
    merge_team_week_features()
