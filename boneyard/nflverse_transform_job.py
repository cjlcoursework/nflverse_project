import glob
import logging
import os
from typing import Optional

from pandas import DataFrame

# Configure logging
from src import *
from src.utils_db import load_dims_to_db

from src.pbp_fact import transform_pbp
from src.pbp_participation import transform_pbp_participation
from src.player_injuries import prep_player_injuries
from src.player_stats import transform_player_stats, merge_injuries, transform_players
from src.utils import assert_and_alert

logger = configure_logging("pbp_logger")

def load_files(data_subdir: str) -> Optional[DataFrame]:
    output_dir = get_config('output_directory')
    data_dir = os.path.join(output_dir, data_subdir)
    logger.info(f"Reading all files from {data_subdir}")
    data_files = []
    for extension in tested_extensions.keys():
        data_files.extend(glob.glob(f'{data_dir}/*.{extension}'))

    # Initialize an empty list to store individual DataFrames
    dfs = []

    # Iterate over each CSV file and load it into a DataFrame
    record_count = 0
    for file in data_files:
        logger.info(f"  + Reading {os.path.basename(file)}")
        func = get_read_function(file)
        df = func(file)
        record_count += len(df)
        df['op_source'] = file
        dfs.append(df)

    # Concatenate all DataFrames into a single DataFrame
    assert_and_alert(record_count!=0, msg=f"No Records found for {data_subdir}")
    if record_count > 0:
        combined_df = pd.concat(dfs, ignore_index=True)
        assert_and_alert(len(combined_df) == record_count, msg=f"Records lost during loading of {data_subdir}")
        return combined_df

    # Print the combined DataFrame
    return None


