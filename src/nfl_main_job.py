from src.nfl_00_load_nflverse_data import read_nflverse_datasets
from src.nfl_01_build_nfl_database import create_nfl_database
from src.nfl_02_prepare_weekly_stats import prepare_team_week_dataset
from src.nfl_03_perform_feature_selection import perform_team_week_feature_selection
from src.nfl_04_merge_game_features import merge_team_week_features


def ingest_nflverse_data():
    """
    Steps to ingest the nflverse data.
    """
    read_nflverse_datasets()  # read data from nflverse
    create_nfl_database()  # clean and transform the data and create the nfl database


def prepare_team_week_data():
    """
    Steps to prepare the team-week dataset.
    """
    prepare_team_week_dataset(store_to_db=False) # merge all stats we care about int oa single dataset
    perform_team_week_feature_selection()
    merge_team_week_features()


if __name__ == '__main__':
    """ 
    Test the workflow.
    """
    ingest_nflverse_data()
    prepare_team_week_data()
