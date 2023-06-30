import logging

import pandas as pd

from NFLVersReader.src.nflverse_clean.injuries import prep_player_injuries
from NFLVersReader.src.nflverse_clean.pbp_fact import job_pbp_main
from NFLVersReader.src.nflverse_clean.pbp_participation import conform_pbp_participation_play_id, reconcile_join_keys, \
    explode_player_lists

# Configure logging
import logging_config
from NFLVersReader.src.nflverse_clean.player_stats import impute_player_stats, check_keys, impute_payers, check_merge
from NFLVersReader.src.nflverse_clean.utils import load_dims_to_db

logger = logging_config.confgure_logging("pbp_logger")

if __name__ == '__main__':
    logger.setLevel(logging.DEBUG)

    """
    Play by play
    """

    # load pbp from local file and transform to star-like schema
    pbp_df = pd.read_csv("../../output/playbyplay_2021.csv", low_memory=False)
    results = job_pbp_main(pbp_df)
    results['schema'] = "controls"

    # load semi-prepped data to db
    load_dims_to_db(results)

    """
    Play by play participation
    """

    # load pbp-participation from local file
    pbp_df = results['play_actions']
    pbp_participation_df = pd.read_csv("../../output/playbyplay2021_participation.csv", low_memory=False)

    # make the as join-able as possible
    conform_pbp_participation_play_id(
        participation_df=pbp_participation_df
    )

    """
    PBP + Play by play participation
    """

    reconcile_join_keys(pbp_df, pbp_participation_df)

    offense_df, defense_df = explode_player_lists(pbp_participation_df)

    # load participation data to db
    load_dims_to_db({
        'schema': 'controls',
        'offense_participants': offense_df,
        'defense_participants': defense_df,
        'pbp_participation': pbp_participation_df.drop(columns=['defense_players', 'offense_players'])
    })

    """
    Player stats
    """

    stats_df = pd.read_csv("../../output/player_stats.csv", low_memory=False)
    stats_df = impute_player_stats(stats_df)
    check_keys(stats_df)

    """
    Players
    """

    players_df = pd.read_csv("../../output/players.csv", low_memory=False)
    players_df = impute_payers(players_df)
    players_df.rename(columns={'gsis_id': 'player_id'}, inplace=True)

    """
    Player stats + Players
    """

    merged_df = pd.merge(stats_df, players_df, left_on='player_id', right_on='player_id', how='outer', indicator=True)
    check_merge(merged_df, stats_df)

    load_dims_to_db({
        'schema': 'controls',
        'player_stats': stats_df,
        'players': players_df
    })

    """
    Injuries
    """

    injuries_df = pd.read_csv("../../output/injuries_2021.csv", low_memory=False, parse_dates=['date_modified'])
    prep_player_injuries(injuries_df)
    load_dims_to_db({
        'schema': 'controls',
        'player_injuries': injuries_df
    })



