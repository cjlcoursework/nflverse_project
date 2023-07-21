from src.nfl_00_load_nflverse_data import read_nflverse_datasets
from src.nfl_01_load_nfl_database import create_nfl_database
from src.nfl_02_prepare_weekly_stats import prepare_team_week_dataset
from src.nfl_03_perform_feature_selection import perform_team_week_feature_selection
from src.nfl_04_merge_game_feature_selection import merge_team_week_features


def ingest_nflverse_data():
    """
    Steps to ingest the nflverse data.
    """
    read_nflverse_datasets()
    create_nfl_database()


def prepare_team_week_data():
    """
    Steps to prepare the team-week dataset.
    """
    prepare_team_week_dataset(store_to_db=False)
    perform_team_week_feature_selection()
    merge_team_week_features()


if __name__ == '__main__':
    """ 
    Test the workflow.
    """
    ingest_nflverse_data()
    prepare_team_week_data()
