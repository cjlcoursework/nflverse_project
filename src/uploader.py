# Ideas borrowed from @cooperdff
# Original code: https://github.com/cooperdff/nfl_data_py.git
# Modified by: Chris Lomeli
#

import requests


def read_source(url, local_file_path):
    # Make the HTTP request to get the Parquet file content
    response = requests.get(url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Create a temporary file to write the response content
        with open(local_file_path, "wb") as file:
            file.write(response.content)
            print("file saved successfully.")
    else:
        print("Error: Failed to fetch the file.")
    return response.status_code

def load_pdp(year, file_type, local_file_path):
    url = f'https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_{year}.{file_type}'
    return read_source(url, local_file_path)


def load_pbp_participation(year, file_type, local_file_path):
    url =  f'https://github.com/nflverse/nflverse-data/releases/download/pbp_participation/pbp_participation_{year}.{file_type}'
    return read_source(url, local_file_path)


def load_injuries(year, file_type, local_file_path):
    url = f"https://github.com/nflverse/nflverse-data/releases/download/injuries/injuries_{year}.{file_type}"
    return read_source(url, local_file_path)


def load_player_stats(file_type, local_file_path, stats_type=None, year=None):
    stats = "player_stats"
    if stats_type == 'kicking':
        stats = "player_stats_kicking"

    base_url = "https://github.com/nflverse/nflverse-data/releases/download/player_stats"
    if year is None:
        url = f"{base_url}/{stats}.{file_type}"
    else:
        url = f"{base_url}/{stats}_{year}.{file_type}"

    return read_source(url, local_file_path)


def load_pfr_advstats_stats(file_type, local_file_path, stats_type, year=None):
    if year is None:
        url = f"https://github.com/nflverse/nflverse-data/releases/download/pfr_advstats/advstats_season_{stats_type}.{file_type}"
    else:
        url = f"https://github.com/nflverse/nflverse-data/releases/download/pfr_advstats/advstats_week_{stats_type}_{year}.{file_type}"
    return read_source(url, local_file_path)


def load_players(file_type, local_file_path):
    url = f"https://github.com/nflverse/nflverse-data/releases/download/players/players.{file_type}"
    return read_source(url, local_file_path)


def load_rosters(year, file_type, local_file_path):
    url = f"https://github.com/nflverse/nflverse-data/releases/download/rosters/roster_{year}.{file_type}"
    return read_source(url, local_file_path)


def load_rosters_weekly(year, file_type, local_file_path):
    url = f"https://github.com/nflverse/nflverse-data/releases/download/weekly_rosters/roster_weekly_{year}.{file_type}"
    return read_source(url, local_file_path)


def load_depth_charts(year, file_type, local_file_path):
    url = f"https://github.com/nflverse/nflverse-data/releases/download/depth_charts/depth_charts_{year}.{file_type}"
    return read_source(url, local_file_path)


def load_next_gen_stats(year,  file_type, local_file_path, stat_type="passing"):
    # passing, rushing, receiving
    url = f"https://github.com/nflverse/nflverse-data/releases/download/nextgen_stats/ngs_{year}_{stat_type}.{file_type}"
    return read_source(url, local_file_path)


def load_officials(file_type, local_file_path):
    url = f"https://github.com/nflverse/nflverse-data/releases/download/officials/officials.{file_type}"
    return read_source(url, local_file_path)


def load_snap_counts(year,  file_type, local_file_path):
    url = f"https://github.com/nflverse/nflverse-data/releases/download/snap_counts/snap_counts_{year}.{file_type}"
    return read_source(url, local_file_path)




