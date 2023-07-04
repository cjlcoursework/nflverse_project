import pandas as pd

# Configure logging
import logging_config
from NFLVersReader.src.nflverse_clean.configs import get_config
from NFLVersReader.src.nflverse_clean.pbp_fact import transform_pbp
from NFLVersReader.src.nflverse_clean.pbp_participation import transform_pbp_participation
from NFLVersReader.src.nflverse_clean.player_injuries import prep_player_injuries
from NFLVersReader.src.nflverse_clean.player_stats import transform_player_stats, transform_players, \
    check_merge, merge_injuries
from NFLVersReader.src.nflverse_clean.utils import load_dims_to_db

logger = logging_config.confgure_logging("pbp_logger")


def load_file(config_name):
    return pd.read_csv(get_config(config_name), low_memory=False)


def perform_workflow():
    """
    Workflow to call all loaders at once in one job
    """

    """
    Play by play
    """
    pbp = load_file('playbyplay')
    datasets = transform_pbp(pbp)

    """
    Play by play participation
    """
    pbp_participation_df = load_file('participation')

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
    injuries_df = load_file('injuries')
    injuries_df = prep_player_injuries(injuries_df)

    """
    Player stats
    """
    stats_df = load_file('player_stats')
    stats_df = transform_player_stats(stats_df)
    stats_df = merge_injuries(player_stats=stats_df, player_injuries=injuries_df)

    """
    Players
    """
    players_df = load_file('players')
    players_df = transform_players(players_df)

    datasets.update({
        'players': players_df
    })

    """
    Adv stats
    """
    advstats_df = load_file('advstats')
    next_pass_df = load_file('nextgen_passing')
    next_rec_df = load_file('nextgen_receiving')
    next_rush_df = load_file('nextgen_rushing')

    """
    Depth chart
    """
    depth_chart_df = load_file('depth_chart').rename(columns={'gsis_id': 'player_id'})

    datasets.update({
        'depth_chart': depth_chart_df,
        'adv_stats': advstats_df,
        'nextgen_pass': next_pass_df,
        'nextgen_rec': next_rec_df,
        'nextgen_rush': next_rush_df
    })

    return datasets


if __name__ == '__main__':
    load_to_db = True

    results = perform_workflow()

    if load_to_db:
        results['schema'] = 'controls'
        load_dims_to_db(results)
