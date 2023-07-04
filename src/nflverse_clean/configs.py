import pandas as pd
import numpy as np

configurations = {
    'pbp_url': 'https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_{year}.{file_type}',
    'pbp_participation_url': 'https://github.com/nflverse/nflverse-data/releases/download/pbp_participation/pbp_participation_{year}.{file_type}',
    'injuries_url': 'https://github.com/nflverse/nflverse-data/releases/download/injuries/injuries_{year}.{file_type}',
    'player_stats_url': 'https://github.com/nflverse/nflverse-data/releases/download/player_stats/player_stats.{file_type}',
    'advstats_url': 'https://github.com/nflverse/nflverse-data/releases/download/pfr_advstats/advstats_season_{stats_type}.{file_type}',
    'advstats_stat_types': ['def', 'pass', 'rush', 'pass', 'rec'],
    'players_url': 'https://github.com/nflverse/nflverse-data/releases/download/players/players.{file_type}',
    'ng_stats_url': 'https://github.com/nflverse/nflverse-data/releases/download/nextgen_stats/ngs_{year}_{stat_type}.csv.gz',
    'ng_stats_types': ['passing', 'rushing', 'receiving'],
    'output_directory': "/Users/christopherlomeli/Source/courses/datascience/Springboard/capstone/NFL/NFLVersReader/output",
    'schema_directory': "/Users/christopherlomeli/Source/courses/datascience/Springboard/capstone/NFL/NFLVersReader/schemas",
    'connection_string': 'postgresql://postgres:chinois1@localhost',
    'playbyplay': "/Users/christopherlomeli/Source/courses/datascience/Springboard/capstone/NFL/NFLVersReader/output/playbyplay_2021.csv",
    'participation': "./Users/christopherlomeli/Source/courses/datascience/Springboard/capstone/NFL/NFLVersReader/output/playbyplay2021_participation.csv",
    'player_stats': "/Users/christopherlomeli/Source/courses/datascience/Springboard/capstone/NFL/NFLVersReader/output/player_stats.csv",
    'players': "/Users/christopherlomeli/Source/courses/datascience/Springboard/capstone/NFL/NFLVersReader/output/players.csv",
    'injuries': "/Users/christopherlomeli/Source/courses/datascience/Springboard/capstone/NFL/NFLVersReader/output/injuries_2021.csv",
    # 'advstats': "/Users/christopherlomeli/Source/courses/datascience/Springboard/capstone/NFL/NFLVersReader/output/pfr_advstats_all.csv",
    'nextgen_passing': "/Users/christopherlomeli/Source/courses/datascience/Springboard/capstone/NFL/NFLVersReader/output/next_gen_stats_passing_2016.csv.gz",
    'nextgen_receiving': "/Users/christopherlomeli/Source/courses/datascience/Springboard/capstone/NFL/NFLVersReader/output/next_gen_stats_receiving_2016.csv.gz",
    'nextgen_rushing': "/Users/christopherlomeli/Source/courses/datascience/Springboard/capstone/NFL/NFLVersReader/output/next_gen_stats_rushing_2016.csv.gz",
    'depth_chart': "/Users/christopherlomeli/Source/courses/datascience/Springboard/capstone/NFL/NFLVersReader/output/depth_charts_2016.csv",
    'positions_data': "/Users/christopherlomeli/Source/courses/datascience/Springboard/capstone/NFL/nfl_capstone/data"
                      "/raw/positions.csv"
}


def get_config(name, default=None):
    if name in configurations:
        return configurations[name]
    return default
