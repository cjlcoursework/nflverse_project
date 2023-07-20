import pandas as pd
from src import *
from src.util.utils import assert_and_alert

logger = configure_logging("pbp_logger")

tests = {
    '2016_01_BUF_BAL': {'yards_gained': 468}
}


def check_values(df: pd.DataFrame, game_id, testcase: dict, msg):
    logger.info(f"Validating game {game_id} values at location: {msg}...")
    # Filter the DataFrame based on the game_id and select the 'down' and 'yards_gained' columns
    filtered_df = df.loc[df['game_id'] == game_id, ['down', 'yards_gained']]

    # Calculate the sum of 'downs' and 'yards_gained' for the specific game_id
    sum_yards = filtered_df['yards_gained'].sum()

    assert_and_alert(sum_yards == testcase['yards_gained'], msg=f"expected  {testcase['yards_gained']} yards, but got {sum_yards}")
    return


def perform_inline_play_action_tests(play_action_df: pd.DataFrame, msg=None):
    if msg is None:
        msg = "checking play_action counts"
    for game_id, testcase in tests.items():
        check_values(play_action_df, game_id, testcase, msg)


