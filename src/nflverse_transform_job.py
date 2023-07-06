import glob
import logging
import os
from typing import Optional

from pandas import DataFrame

# Configure logging
from src import *
from src.db_utils import load_dims_to_db

from src.pbp_fact import transform_pbp
from src.pbp_participation import transform_pbp_participation
from src.player_injuries import prep_player_injuries
from src.player_stats import transform_player_stats, merge_injuries, transform_players
from src.utils import assert_and_alert

logger = configure_logging("pbp_logger")


# def load_file(config_name):
#     return pd.read_csv(get_config(config_name), low_memory=False)


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


def perform_workflow():
    """
    Workflow to call all loaders at once in one job
    """

    """
    Play by play
    """
    pbp = load_files(data_subdir='pbp')
    datasets = transform_pbp(pbp)

    """
    Play by play participation
    """
    pbp_participation_df = load_files('pbp-participation')

    player_df, player_events_df = transform_pbp_participation(
        participation_df=pbp_participation_df,
        player_events=datasets['player_events'])

    datasets.update({
        'player_participation': player_df,
        'player_events': player_events_df,
    })

    """
    Injuries
    """
    injuries_df = load_files('injuries')
    injuries_df = prep_player_injuries(injuries_df)

    """
    Player stats
    """
    stats_df = load_files('player-stats')
    stats_df = transform_player_stats(stats_df)
    stats_df = merge_injuries(player_stats=stats_df, player_injuries=injuries_df)

    """
    Players
    """
    players_df = load_files('players')
    players_df = transform_players(players_df)

    """
    Adv stats
    """
    advstats_def_df = load_files('advstats-season-def')
    advstats_pass_df = load_files('advstats-season-pass')
    advstats_rec_df = load_files('advstats-season-rec')
    advstats_rush_df = load_files('advstats-season-rush')
    next_pass_df = load_files('nextgen-passing')
    next_rec_df = load_files('nextgen-receiving')
    next_rush_df = load_files('nextgen-rushing')

    datasets.update({
        'players': players_df,
        'player_stats': stats_df,
        'adv_stats_def': advstats_def_df,
        'adv_stats_pass': advstats_pass_df,
        'adv_stats_rec': advstats_rec_df,
        'adv_stats_rush': advstats_rush_df,
        'nextgen_pass': next_pass_df,
        'nextgen_rec': next_rec_df,
        'nextgen_rush': next_rush_df
    })

    return datasets


if __name__ == '__main__':
    load_to_db = False
    logger.setLevel(logging.INFO)

    results = perform_workflow()

    if load_to_db:
        results['schema'] = 'controls'
        load_dims_to_db(results)
