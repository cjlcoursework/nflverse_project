import numpy as np
import os
from pandas import DataFrame
from typing import List, Dict, Tuple

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


def merge_powers(action_df: DataFrame, powers_df: DataFrame, suffixes: Tuple[str, str], left_on: List[str],
                 renames: Dict = None, msg: str = 'play_counter') -> DataFrame:
    """
    Merge the offensive stats with the play-by-play events and game info.
    Parameters:
        action_df (DataFrame): The game info data.
        powers_df (DataFrame): The offensive stats data.
        left_on (list): The columns to merge on.
        renames (dict): The columns to rename.
        msg (str): The message to display in the log.
        suffixes (Tuple[str,str]): The suffixes to use for the merge.

        Returns:
        _df (DataFrame): The offensive stats data with the play-by-play events and game info merged in.

    """
    expected_shape = action_df.shape
    _df = pd.merge(action_df, powers_df, left_on=left_on, suffixes=suffixes,
                   right_on=['season', 'week', 'team']).drop_duplicates()
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

    drop_columns = ['spread', 'target', 'opposing_team', 'opposing_coach', 'opposing_score', 'count']

    logger.info("loading weekly features into a single game dataset...")
    directory = get_config('data_directory')

    pbp_actions_df = load_file(directory,
                               get_config('action_week_prep'))

    offense_powers_df = load_file(directory,
                                  get_config('offense_week_features')).drop(columns=drop_columns)

    defense_powers_df = load_file(directory,
                                  get_config('defense_week_features')).drop(columns=drop_columns)

    logger.info("merge stats into play_actions...")

    df = merge_powers(pbp_actions_df, offense_powers_df, left_on=['season', 'week', 'home_team'],
                      renames={'offense_power': 'offense_op'}, suffixes=('_pbp', '_hop'), msg="merging offense_OP")

    df = merge_powers(df, defense_powers_df, left_on=['season', 'week', 'home_team'], suffixes=('_xx', '_hdp'),
                      renames={'defense_power': 'offense_dp'},
                      msg="merging offense_DP")

    df = merge_powers(df, offense_powers_df, left_on=['season', 'week', 'away_team'], suffixes=('_hop', '_aop'),
                      renames={'offense_power': 'defense_op'},
                      msg="merging defense_OP")

    df = merge_powers(df, defense_powers_df, left_on=['season', 'week', 'defteam'], suffixes=('_hdp', '_adp'),
                      renames={'defense_power': 'defense_dp'},
                      msg="merging defense_DP")

    assert_and_alert(pbp_actions_df.shape[0] == df.shape[0],
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

    cols = df.columns.values
    # Group by season, week, game_id
    #   also by defense and offense so separate row are created for each team
    grouped_df = df.groupby(['season', 'week', 'game_id', 'home_team', 'away_team']).agg(
        drive_count=('drive', 'count'),
        carries_hop=('carries_hop', 'sum'),
        carries_aop=('carries_aop', 'sum'),
        receiving_tds_hop=('receiving_tds_hop', 'sum'),
        receiving_tds_aop=('receiving_tds_aop', 'sum'),
        passer_rating_hop=('passer_rating_hop', 'mean'),
        passer_rating_aop=('passer_rating_aop', 'mean'),
        pass_touchdowns_hop=('pass_touchdowns_hop', 'mean'),
        pass_touchdowns_aop=('pass_touchdowns_aop', 'mean'),
        special_teams_tds_hop=('special_teams_tds_hop', 'sum'),
        special_teams_tds_aop=('special_teams_tds_aop', 'sum'),
        rushing_yards_hop=('rushing_yards_hop', 'sum'),
        rushing_yards_aop=('rushing_yards_aop', 'sum'),
        rushing_tds_hop=('rushing_tds_hop', 'sum'),
        rushing_tds_aop=('rushing_tds_aop', 'sum'),
        receiving_yards_hop=('receiving_yards_hop', 'sum'),
        receiving_yards_aop=('receiving_yards_aop', 'sum'),
        receiving_air_yards_hop=('receiving_air_yards_hop', 'sum'),
        receiving_air_yards_aop=('receiving_air_yards_aop', 'sum'),
        ps_interceptions_hdp=('ps_interceptions_hdp', 'sum'),
        ps_interceptions_adp=('ps_interceptions_adp', 'sum'),
        interception_hdp=('interception_hdp', 'sum'),
        interception_adp=('interception_adp', 'sum'),
        qb_hit_hdp=('qb_hit_hdp', 'sum'),
        qb_hit_adp=('qb_hit_adp', 'sum'),
        sack_hdp=('sack_hdp', 'sum'),
        sack_adp=('sack_adp', 'sum'),
        tackle_hdp=('tackle_hdp', 'sum'),
        tackle_adp=('tackle_adp', 'sum'),
        sack_yards_hdp=('sack_yards_hdp', 'sum'),
        sack_yards_adp=('sack_yards_adp', 'sum'),
        first_downs=('down', lambda x: (x == 1).sum()),
        home_final_score=('home_final_score', 'max'),
        away_final_score=('away_final_score', 'max'),
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
        home_team_offense_power=('offense_op', 'mean'),
        home_team_defense_power=('offense_dp', 'mean'),
        away_team_offense_power=('defense_op', 'mean'),
        away_team_defense_power=('defense_dp', 'mean')
    )

    # Reset the index to transform the grouped DataFrame back to a regular DataFrame
    grouped_df.reset_index(inplace=True)

    # Select the desired columns for the final result
    games_df = grouped_df.copy()

    # Create a new column 'loss_tie_win' based on conditions
    games_df['loss_tie_win'] = np.where(
        games_df['home_final_score'] >= games_df['away_final_score'], 1, 0)

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
