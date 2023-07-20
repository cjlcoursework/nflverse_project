import logging

from src import configs
from src.nfl.pbp_fact import transform_pbp
from src.nfl.pbp_participation import transform_pbp_participation
from src.nfl.player_injuries import prep_player_injuries
from src.nfl.player_stats import transform_player_stats, merge_injuries, transform_players
from src.util.utils import load_files
from src.util.utils_db import load_dims_to_db

LOAD_TO_DB = True
database_schema = 'controls'

# Get the logger
logger = configs.configure_logging("pbp_logger")
logger.setLevel(logging.INFO)


def load_pbp():
    pbp = load_files(data_subdir='pbp')
    datasets = transform_pbp(pbp)
    return datasets


def load_participation(datasets):
    pbp_participation_df = load_files('pbp-participation')

    player_df, player_events_df = transform_pbp_participation(
        participation_df=pbp_participation_df,
        player_events=datasets['player_events'])

    datasets.update({
        'player_participation': player_df,
        'player_events': player_events_df,
    })
    return datasets


def load_stats(datasets):
    injuries_df = load_files('injuries')
    injuries_df = prep_player_injuries(injuries_df)

    datasets.update({
        'injuries': injuries_df,
    })

    stats_df = load_files('player-stats')
    stats_df = transform_player_stats(stats_df)
    stats_df = merge_injuries(player_stats=stats_df, player_injuries=injuries_df)

    players_df = load_files('players')
    players_df = transform_players(players_df)

    datasets.update({
        'players': players_df,
        'player_stats': stats_df,
    })
    return datasets


def load_advanced_stats(datasets):
    advstats_def_df = load_files('advstats-season-def')
    advstats_pass_df = load_files('advstats-season-pass')
    advstats_rec_df = load_files('advstats-season-rec')
    advstats_rush_df = load_files('advstats-season-rush')
    next_pass_df = load_files('nextgen-passing')
    next_rec_df = load_files('nextgen-receiving')
    next_rush_df = load_files('nextgen-rushing')
    datasets.update({
        'adv_stats_def': advstats_def_df,
        'adv_stats_pass': advstats_pass_df,
        'adv_stats_rec': advstats_rec_df,
        'adv_stats_rush': advstats_rush_df,
        'nextgen_pass': next_pass_df,
        'nextgen_rec': next_rec_df,
        'nextgen_rush': next_rush_df
    })
    return datasets


def load_all_datasets_to_db(data: dict):
    data['schema'] = database_schema
    load_dims_to_db(data)


def create_nfl_database():
    datasets = load_pbp()
    datasets = load_participation(datasets)
    datasets = load_stats(datasets)
    datasets = load_advanced_stats(datasets)

    if LOAD_TO_DB:
        load_all_datasets_to_db(datasets)


if __name__ == '__main__':
    create_nfl_database()

