import warnings
from pandas import DataFrame

from src import *
from src.util.utils import assert_not_null
from src.util.utils_db import validate_positions

# from utils_db import validate_positions

warnings.filterwarnings('ignore')
logger = configure_logging("pbp_logger")


def check_keys(injuries_df: DataFrame):
    """
    Conform any keys in this function.
    In this case we are only renaming the gsis_id column to player_id which is consistent with other data
    Parameters:
        injuries_df (pd.DataFrame): an offense or defense stats dataframe that we can use for feature selection

    Returns:
        None - we'll alert if we find any problems

     """

    logger.info("""Checking for nulls...)""")
    assert_not_null(injuries_df, 'season')
    assert_not_null(injuries_df, 'week')
    assert_not_null(injuries_df, 'player_id')
    assert_not_null(injuries_df, 'team')
    assert_not_null(injuries_df, 'position')
    assert_not_null(injuries_df, 'report_status')


def conform_names(injuries_df: DataFrame) -> DataFrame:
    """
    Conform any keys in this function.
    In this case we are only renaming the gsis_id column to player_id which is consistent with other data
    Parameters:
        injuries_df (pd.DataFrame): an offense or defense stats dataframe that we can use for feature selection

    Returns:
        injuries_df (pd.DataFrame): a cleaned version of itself

     """

    logger.info("""Conforming names (e.g. gsis_id -> player_id)""")
    return injuries_df.rename(columns={'gsis_id': 'player_id'})


def transformations(injuries_df: DataFrame) -> DataFrame:

    """
    Add a report_status column that is a more consistent version of a players injury
    This is just a series of if else logic that we use to determine the best report_status value

    Parameters:
        injuries_df (pd.DataFrame): an offense or defense stats dataframe that we can use for feature selection

    Returns:
        injuries_df (pd.DataFrame): a cleaned version of itself

     """

    logger.info("""Merge sparse injury columns""")
    # merge sparse injury report fields
    injuries_df['primary_injury'] = injuries_df['report_primary_injury'].fillna(injuries_df['practice_primary_injury'])

    # best efforts to fill empty report status fields
    logger.info("""Get best values for null report_statuses...""")
    injuries_df.loc[(injuries_df.report_status.isna()) & (
        injuries_df.primary_injury.str.lower().str.contains('resting')), 'report_status'] = 'Resting'

    injuries_df.loc[(injuries_df.report_status.isna()) & (
        injuries_df.primary_injury.str.lower().str.contains('personal')), 'report_status'] = 'Personal'

    injuries_df.loc[(injuries_df.report_status.isna()) & (
        injuries_df.practice_status.str.lower().str.contains('full participation')), 'report_status'] = 'Optimistic'

    injuries_df.loc[(injuries_df.report_status.isna()) & (
        injuries_df.practice_status.str.lower().str.contains('did not participate')), 'report_status'] = 'Doubtful'

    injuries_df.loc[(injuries_df.report_status.isna()) & (injuries_df.practice_status.str.lower().str.contains(
        'limited participation')), 'report_status'] = 'Questionable'

    injuries_df.loc[(injuries_df.report_status.isna()), 'report_status'] = 'Uncertain'

    return injuries_df


def prep_player_injuries(injuries_df: DataFrame) -> DataFrame:

    """
    Here we start with the injuries dataset from nflverse we
    - conform any columns we'll use to join later
    - validate that there are no nulls in key columns
    - re-work some of the status data into a more consistent and complete 'report_status' column

    Parameters:
        injuries_df (pd.DataFrame): an offense or defense stats dataframe that we can use for feature selection

    Returns:
        injuries_df (pd.DataFrame): a cleaned version of itself

     """

    logger.info("""Prep injury data...""")

    injuries_df = conform_names(injuries_df)
    injuries_df = transformations(injuries_df)

    logger.info("check that all positions are correct...")
    validate_positions(injuries_df, silent=True)
    return injuries_df


def test_player_stats_job():
    injuries_df = pd.read_csv("../../output/injuries_2021.csv", low_memory=False, parse_dates=['date_modified'])
    prep_player_injuries(injuries_df)
