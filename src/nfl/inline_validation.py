import pandas as pd
from src import *
from src.util.utils import assert_and_alert

logger = configure_logging("pbp_logger")

"""
This is a sample inline test with a single test
  for example, in this single test we validate that the sum of all yards gained in the '2016_01_BUF_BAL' game equals 468
"""

tests = {
    '2016_01_BUF_BAL': {'yards_gained': 468}
}


def check_values(df: pd.DataFrame, game_id, testcase: dict, msg):
    """
    This function checks the sum of yards gained for a specific game_id
    Parameters:
        df (pd.DataFrame): The DataFrame to be filtered
        game_id (str): The game_id to be filtered
        testcase (dict): The expected value of the sum of yards gained
        msg (str): The message to be logged if we have an error
    """
    logger.info(f"{msg}...")
    # Filter the DataFrame based on the game_id and select the 'down' and 'yards_gained' columns
    filtered_df = df.loc[df['game_id'] == game_id, ['down', 'yards_gained']]

    # Calculate the sum of 'downs' and 'yards_gained' for the specific game_id
    sum_yards = filtered_df['yards_gained'].sum()

    assert_and_alert(sum_yards == testcase['yards_gained'], msg=f"expected  {testcase['yards_gained']} yards, but got {sum_yards}")
    return


def perform_inline_play_action_tests(play_action_df: pd.DataFrame, msg=None):
    """
    This function performs inline tests on the play_action DataFrame
    """
    if msg is None:
        msg = "checking play_action counts"
    for game_id, testcase in tests.items():
        check_values(play_action_df, game_id, testcase, msg)


