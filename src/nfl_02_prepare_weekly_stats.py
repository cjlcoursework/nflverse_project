import pandas as pd
from pandas import DataFrame

from src import *
from src.nfl.nfl_power_scores import backfill_missing_metrics
from src.sql.nfl_queries import build_all_weeks_template, build_play_actions_dataset, build_game_info_dataset, \
    build_ngs_air_stats, build_ngs_ground_stats, build_pbp_events_dataset, build_defense_player_stats, \
    build_offense_player_stats
from src.util.utils import assert_and_alert
from src.util.utils_db import store_df

logger = configs.configure_logging("pbp_logger")
logger.setLevel(logging.INFO)

db = DatabaseLoader(get_config('connection_string'))
COMMIT_TO_DATABASE = True
SCHEMA = 'controls'


def show_coverage(title: str, df: DataFrame):
    """
    Show the coverage of the data in the dataframe.
    """
    first = df.season.min()
    last = df.season.max()
    first_wk = df.week.min()
    last_wk = df.week.max()
    seasons = df.season.nunique()
    logger.debug(
        f"Shape of {title:30}:  {df.shape},\t Contains {seasons} seasons, starting with {first} and ending in {last} min week: {first_wk}, max week : {last_wk}")


def check_for_merge_columns(merged: DataFrame):
    """
    Check for columns that were duplicated during the merge process.
    """
    overlaps = 0
    for col in merged.columns:
        if str(col).endswith("_y") or str(col).endswith("_x") or str(col) == "rn":
            print(col)
            overlaps += 1

    assert overlaps == 0, f"found {overlaps} columns that were duplicated during the merge process"


def merge_offensive_stats(game_df: DataFrame, possession_stats: DataFrame, ngs_air_power: DataFrame,
                          ngs_ground_power: DataFrame) -> DataFrame:
    """
    Merge the offensive stats with the play-by-play events and game info.
    Parameters:
        game_df (DataFrame): The game info data.
        possession_stats (DataFrame): The offensive stats data.
        ngs_air_power (DataFrame): The ngs_air_power data.
        ngs_ground_power (DataFrame): The ngs_ground_power data.
    Returns:
        possession_stats (DataFrame): The offensive stats data with the play-by-play events and game info merged in.
    """
    logger.info("merge offense events info into a single offense stats dataset...")

    starting_shape = possession_stats.shape
    possession_stats = pd.merge(possession_stats, ngs_air_power, on=['season', 'week', 'team'])
    possession_stats = pd.merge(possession_stats, ngs_ground_power, on=['season', 'week', 'team'])
    logger.info(f"possession_stats before: {starting_shape}, after: {possession_stats.shape}")

    assert_and_alert(starting_shape[0] == possession_stats.shape[0],
                     msg=f"possession_stats before: {starting_shape}, after: {possession_stats.shape}")

    possession_stats = pd.merge(possession_stats, game_df, on=['season', 'week', 'team'])

    logger.info(
        f"offense_shape before: {starting_shape}, after : {possession_stats.shape}")

    assert_and_alert(0 == possession_stats.isnull().sum().sum(), msg=f"found unexpected nulls in possession_stats")
    check_for_merge_columns(possession_stats)
    return possession_stats


def merge_defensive_stats(game_df: DataFrame, pbp_events: DataFrame, defense_stats: DataFrame) -> DataFrame:
    """
    Merge the defensive stats with the play-by-play events and game info.
    Parameters:
        game_df (DataFrame): The game info data.
        pbp_events (DataFrame): The play-by-play events data.
        defense_stats (DataFrame): The defensive stats data.

    Returns:
        defense_stats (DataFrame): The defensive stats data with the play-by-play events and game info merged in.
    """
    logger.info("merge defense events info into a single defense stats dataset...")

    starting_shape = defense_stats.shape
    defense_stats = pd.merge(defense_stats, pbp_events, on=['season', 'week', 'team'])
    logger.info(f"defense shape before: {starting_shape}, after: {defense_stats.shape}")

    assert_and_alert(starting_shape[0] == defense_stats.shape[0],
                     msg=f"possession_stats before: {starting_shape}, after: {defense_stats.shape}")

    defense_stats = pd.merge(defense_stats, game_df, on=['season', 'week', 'team'])

    logger.info(
        f"defense_stats before: {starting_shape}, after game_info: {defense_stats.shape} ")

    assert_and_alert(0 == defense_stats.isnull().sum().sum(), msg=f"found unexpected nulls in possession_stats")
    check_for_merge_columns(defense_stats)
    return defense_stats


def store_weekly_datasets(play_actions_df: DataFrame, offense_stats: DataFrame, defense_stats: DataFrame, store_to_db: bool):
    """
    Transform and load the play-by-play data into a fact table and a few dimension tables.
    Parameters:
        play_actions_df (DataFrame): The play-by-play data.
        offense_stats (DataFrame): The offensive stats data.
        store_to_db (bool): Whether to store the data to the database or not.
    Returns:
        None - data is stored to the database.

    """
    logger.info("save play action and merged offense and defense stats ...")
    action_table_name = get_config('action_week_prep')
    offense_table_name = get_config('offense_week_prep')
    defense_table_name = get_config('defense_week_prep')

    store_df(offense_stats, offense_table_name, db=db if store_to_db else None, schema=SCHEMA)
    store_df(defense_stats, defense_table_name, db=db if store_to_db else None, schema=SCHEMA)
    store_df(play_actions_df, action_table_name, db=db if store_to_db else None, schema=SCHEMA)


def build_weekly_datasets():
    """ Load the player stats and injuries data and transform it into a fact table and a few dimension tables.
    Returns:
        datasets (dict): The same dictionary of dataframes, with the stats and injuries data added.
    """
    all_team_weeks = build_all_weeks_template()
    play_actions_df = build_play_actions_dataset()
    game_df = build_game_info_dataset()
    ngs_air_power = build_ngs_air_stats()
    ngs_ground_power = build_ngs_ground_stats()
    pbp_events = build_pbp_events_dataset()
    defense_stats = build_defense_player_stats()
    possession_stats = build_offense_player_stats()

    ngs_air_power = backfill_missing_metrics(ngs_air_power, all_team_weeks, 'ngs_air_power')
    ngs_ground_power = backfill_missing_metrics(ngs_ground_power, all_team_weeks, 'ngs_ground_power')
    pbp_events = backfill_missing_metrics(pbp_events, all_team_weeks, 'pbp_events')
    defense_stats = backfill_missing_metrics(defense_stats, all_team_weeks, 'defense_stats')
    possession_stats = backfill_missing_metrics(possession_stats, all_team_weeks, 'possession_stats')
    ngs_air_power = backfill_missing_metrics(ngs_air_power, all_team_weeks, 'ngs_air_power')

    # calc_coverage("ngs_air_power  ", ngs_air_power)
    show_coverage("ngs_ground_power ", ngs_ground_power)
    show_coverage("pbp_events  ", pbp_events)
    show_coverage("defense_stats  ", defense_stats)
    show_coverage("possession_stats  ", possession_stats)
    show_coverage("game info  ", game_df)

    all_offense_stats = merge_offensive_stats(game_df, possession_stats, ngs_air_power, ngs_ground_power)
    all_defense_stats = merge_defensive_stats(game_df, pbp_events, defense_stats)
    return play_actions_df, all_offense_stats, all_defense_stats


def prepare_team_week_dataset(store_to_db=False):
    """
    Transform and load the play-by-play data into a fact table and a few dimension tables.
    Parameters:
        store_to_db (bool): Whether to store the data to the database or not.

    """
    play_actions_df, all_offense_stats, all_defense_stats = build_weekly_datasets()
    store_weekly_datasets(play_actions_df, all_offense_stats, all_defense_stats, store_to_db)


if __name__ == '__main__':
    """
    Just for sanity testing the code in this file.
    """
    prepare_team_week_dataset(COMMIT_TO_DATABASE)
