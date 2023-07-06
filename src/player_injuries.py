import pandas as pd
from pandas import DataFrame

from src import *

import warnings

from src.db_utils import validate_positions

warnings.filterwarnings('ignore')
logger = configure_logging("pbp_logger")


def check_keys(df):
    logger.info("""Checking for nulls...)""")
    assert_not_null(df, 'season')
    assert_not_null(df, 'week')
    assert_not_null(df, 'player_id')
    assert_not_null(df, 'team')
    assert_not_null(df, 'position')
    assert_not_null(df, 'report_status')


def conform_names(injuries_df: DataFrame):
    logger.info("""Conforming names (e.g. gsis_id -> player_id)""")
    return injuries_df.rename(columns={'gsis_id': 'player_id'})


def transformations(injuries_df):
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


def prep_player_injuries(df: DataFrame):
    logger.info("""Prep injury data...""")

    df = conform_names(df)
    df = transformations(df)

    logger.info("check that all positions are correct...")
    validate_positions(df, silent=True)
    return df


def test_player_stats_job():
    injuries_df = pd.read_csv("../../output/injuries_2021.csv", low_memory=False, parse_dates=['date_modified'])
    prep_player_injuries(injuries_df)
