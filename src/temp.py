import numpy as np
import os

import pandas as pd
from src import *


def main(weekly_stats_df: pd.DataFrame) :

    unique_teams = np.sort(weekly_stats_df['team'].unique())
    unique_seasons = np.sort(weekly_stats_df['season'].unique())
    max_week = weekly_stats_df['week'].max()
    max_weeks = list(range(1, max_week+1))

    skeleton = []
    for team in unique_teams:
        for season in unique_seasons:
            for week in max_weeks:
                skeleton.append(dict(team=team, season=season, week=week))


    # all_weeks = pd.DataFrame({
    #     'team': df['team'].unique(),
    #     'season': df['season'].unique(),
    #     'week': range(1, 21)  # Assuming there are 20 weeks in a season
    # })


if __name__ == '__main__':
    file_name = "nfl_ml_weekly_stats"
    data_directory = get_config('data_directory')
    input_path = os.path.join(data_directory,  f"{file_name}.parquet")
    weekly_stats_df = pd.read_parquet(input_path)

    main(weekly_stats_df)
