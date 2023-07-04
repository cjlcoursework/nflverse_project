# Ideas borrowed from @cooperdff
# Original code: https://github.com/cooperdff/nfl_data_py.git
# Modified by: Chris Lomeli
#
import os.path

import requests
import pandas as pd

from NFLVersReader.src.nflverse_clean.utils import assert_and_alert

# Configure logging
import logging_config
logger = logging_config.confgure_logging("pbp_logger")


def validate_schema(local_file_path, schema_file, silent=True):
    schema_df = pd.read_csv(schema_file, header=0)

    df = pd.read_csv(local_file_path, low_memory=False)
    df_dtypes = pd.DataFrame(df.dtypes, columns=['type']).reset_index().rename(columns={"index": "column"})
    assert_and_alert(
        df_dtypes.equals(schema_df),
        msg=f"Schemas do not match:  {schema_file} --> {local_file_path}",
        silent=silent
    )


def read_source(url, output_dir, local_file_base, schema_file_path=None, silent=True):
    # Make the HTTP request to get the Parquet file content
    response = requests.get(url)

    base_name = os.path.basename(url)
    extension = base_name[base_name.index('.'):]

    dir_path = os.path.join(output_dir, *local_file_base.split("_")[:-1])

    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

    full_path = os.path.join(dir_path, local_file_base)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:

        # Write to the local_file_path
        with open(full_path, "wb") as file:
            file.write(response.content)
            print(f"Success: {url}")

        # validate against the schema if one was sent in
        if schema_file_path is not None:
            validate_schema(output_dir, schema_file_path, silent)

    else:
        print(f"Failed  : {url}")
    return response.status_code

#
# def load_pdp(year, file_type, local_file_path, schema_file=None, silent=True):
#     url = f'https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_{year}.{file_type}'
#     return read_source(url, local_file_path, schema_file, silent)
#
#
# def load_pbp_participation(year, file_type, local_file_path,  schema_file=None, silent=True):
#     url = f'https://github.com/nflverse/nflverse-data/releases/download/pbp_participation/pbp_participation_{year}.{file_type}'
#     return  read_source(url, local_file_path, schema_file, silent)
#
#
# def load_injuries(year, file_type, local_file_path, schema_file=None, silent=True):
#     url = f"https://github.com/nflverse/nflverse-data/releases/download/injuries/injuries_{year}.{file_type}"
#     return  read_source(url, local_file_path, schema_file, silent)
#
#
# def load_player_stats(file_type, local_file_path, stats_type=None, year=None, schema_file=None, silent=True):
#     stats = "player_stats"
#     if stats_type == 'kicking':
#         stats = "player_stats_kicking"
#
#     base_url = "https://github.com/nflverse/nflverse-data/releases/download/player_stats.csv"
#     base_url = "https://github.com/nflverse/nflverse-data/releases/download/player_stats"
#     if year is None:
#         url = f"{base_url}/{stats}.{file_type}"
#     else:
#         url = f"{base_url}/{stats}_{year}.{file_type}"
#
#     return  read_source(url, local_file_path, schema_file, silent)
#
#
# def load_pfr_advstats_stats(file_type, local_file_path, stats_type, year=None, schema_file=None, silent=True):
#     if year is None:
#         url = f"https://github.com/nflverse/nflverse-data/releases/download/pfr_advstats/advstats_season_{stats_type}.{file_type}"
#     else:
#         url = f"https://github.com/nflverse/nflverse-data/releases/download/pfr_advstats/advstats_week_{stats_type}_{year}.{file_type}"
#     return  read_source(url, local_file_path, schema_file, silent)
#
#
# def load_players(file_type, local_file_path, schema_file=None, silent=True):
#     url = f"https://github.com/nflverse/nflverse-data/releases/download/players/players.{file_type}"
#     return  read_source(url, local_file_path, schema_file, silent)
#
#
# def load_rosters(year, file_type, local_file_path, schema_file=None, silent=True):
#     url = f"https://github.com/nflverse/nflverse-data/releases/download/rosters/roster_{year}.{file_type}"
#     return  read_source(url, local_file_path, schema_file, silent)
#
#
# def load_rosters_weekly(year, file_type, local_file_path, schema_file=None, silent=True):
#     url = f"https://github.com/nflverse/nflverse-data/releases/download/weekly_rosters/roster_weekly_{year}.{file_type}"
#     return  read_source(url, local_file_path, schema_file, silent)
#
#
# def load_depth_charts(year, file_type, local_file_path, schema_file=None, silent=True):
#     url = f"https://github.com/nflverse/nflverse-data/releases/download/depth_charts/depth_charts_{year}.{file_type}"
#     return  read_source(url, local_file_path, schema_file, silent)
#
#
# def load_next_gen_stats(year,  file_type, local_file_path, stat_type="passing", schema_file=None, silent=True):
#     # passing, rushing, receiving
#     url = f"https://github.com/nflverse/nflverse-data/releases/download/nextgen_stats/ngs_{year}_{stat_type}.{file_type}"
#     return  read_source(url, local_file_path, schema_file, silent)
#
#
# def load_officials(file_type, local_file_path, schema_file=None, silent=True):
#     url = f"https://github.com/nflverse/nflverse-data/releases/download/officials/officials.{file_type}"
#     return  read_source(url, local_file_path, schema_file, silent)
#
#
# def load_snap_counts(year,  file_type, local_file_path, schema_file=None, silent=True):
#     url = f"https://github.com/nflverse/nflverse-data/releases/download/snap_counts/snap_counts_{year}.{file_type}"
#     return  read_source(url, local_file_path, schema_file, silent)
#
#
#
#
