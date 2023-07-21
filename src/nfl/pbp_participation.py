import pandas as pd
import warnings
from pandas import DataFrame

from src import configure_logging
from src.util.utils import assert_and_alert, get_duplicates_by_key, explode_column_with_cols

warnings.filterwarnings('ignore')
logger = configure_logging("pbp_logger")


def conform_participation_keys(participation_df: DataFrame) -> DataFrame:
    """
    Cleanup and conform key names and check for duplicates

    Parameters:
        participation_df (pd.DataFrame): The participation dataset from nflverse

    Returns:
        participation_df (pd.DataFrame): The wrangled data

    """

    logger.info(
        "pbp_participation:  create a joinable play_id column ...")
    participation_df.rename(columns={'nflverse_game_id': 'game_id'}, inplace=True)
    participation_df['play_counter'] = participation_df['play_id']
    participation_df['play_id'] = participation_df["game_id"].astype(str) + "_" + participation_df[
        "play_counter"].astype(str)  ## temporary
    assert_and_alert(len(get_duplicates_by_key(participation_df, 'play_id')) == 0,
                     msg="Unexpected duplicate keys found creating play_id from game_id and play_id")
    return participation_df


def reconcile_join_keys(pbp_df: DataFrame, participation_df: DataFrame):
    """
    Cleanup and conform key names and check for duplicates.
    We want to verify that the two datasets will join properly

    Parameters:
        pbp_df (pd.DataFrame): The original pbp data from nflverse
        participation_df (pd.DataFrame): The original participation dataset from nflverse

    Returns:
        None - we'll alert if any checks fail

    """

    # Try to join pbp and participations tabkes
    logger.info("Ensure that pbp and pbp_participation are merge-able...")

    logger.info("merge pbp and pbp participants...")
    merged_df = pd.merge(pbp_df, participation_df, left_on='play_id', right_on='play_id', how='outer', indicator=True,
                         suffixes=('_new', '_prev'))

    assert_and_alert(len(get_duplicates_by_key(merged_df, 'play_id')) == 0,
                     msg="Unexpected duplicate keys found creating play_id from game_id and play_id")

    # Find keys that did not join
    missing_participations = merged_df.loc[(merged_df['_merge'] == 'left_only')]
    missing_pbp = merged_df.loc[(merged_df['_merge'] == 'right_only')]

    logger.info(f"not found in pbp {len(missing_pbp)}, not found in participations: {len(missing_participations)}...")

    missing_participations = missing_participations[['play_id', 'play_counter', 'game_id_new', 'old_game_id']] \
        .rename(columns={'game_id_new': 'game_id'}).drop_duplicates()

    # Fix what we can
    logger.info("find any that will join on the old game id...")
    found = 0
    nf = 0
    for index, row in missing_participations.iterrows():
        good_game_id = row['game_id']
        good_play_id = row['play_id']
        good_old_game_id = row['old_game_id']
        fix = participation_df.loc[(participation_df['old_game_id'] == good_old_game_id), ['play_id', 'game_id']]
        participation_df.loc[participation_df['old_game_id'] == good_old_game_id, ['play_id', 'game_id']] = [
            good_play_id, good_game_id]

        if len(fix) == 0:
            nf += 1
        else:
            found += 1
    logger.info("find any done...")

    logger.warning(f"After fix : missing participants:   fixed {found},  not fixed: {nf}")

    # re-create play_id : (pbp_df["game_id"]+"_"+pbp_df["play_id"])
    logger.info("Re-create a joinable play_id column ...")
    participation_df['play_id'] = participation_df["game_id"].astype(str) + "_" + participation_df[
        "play_counter"].astype(str)

    assert_and_alert(len(get_duplicates_by_key(participation_df, 'play_id')) == 0,
                     msg="Unexpected duplicate keys found creating play_id from game_id and play_id")

    merged_df = pd.merge(pbp_df, participation_df, left_on='play_id', right_on='play_id', how='outer', indicator=True,
                         suffixes=('_new', '_prev'))

    logger.info(f"\n{merged_df['_merge'].value_counts()}")


def create_player_participation(participation_df: DataFrame) -> DataFrame:

    """
    Explode the offense and defense arrays in participation_df
    into a separate dataset that has one row per player

    Parameters:
         participation_df (pd.DataFrame): The original participation dataset from nflverse

    Returns:
         players (pd.DataFrame): The exploded offense and defense datasets concatenated into a single dataset

    """

    logger.info("Calculating defense and offense team names by player and play...")
    participation_df[['season', 'game_order', 'home_team', 'away_team']] = \
        participation_df['game_id'].str.split('_', expand=True)[[0, 1, 2, 3]]

    participation_df['defense_team'] = \
        participation_df.apply(lambda row:
                               row['away_team'] if row['possession_team'] == row['home_team']
                               else row['home_team'], axis=1)

    participation_df['season'] = participation_df['season'].astype('int')

    logger.info("Exploding offensive players to their own dataset...")
    offense_players = explode_column_with_cols(participation_df,
                                               ['season', 'game_id', 'game_order', 'play_id', 'possession_team'],
                                               'offense_players')
    offense_players.rename(columns={'offense_players': 'player_id', 'possession_team': 'team'}, inplace=True)
    offense_players['lineup'] = 'offense'

    logger.info("Exploding defense_players to their own dataset...")
    defense_players = explode_column_with_cols(participation_df,
                                               ['season', 'game_id', 'game_order', 'play_id', 'defense_team'],
                                               'defense_players')
    defense_players.rename(columns={'defense_players': 'player_id', 'defense_team': 'team'}, inplace=True)
    defense_players['lineup'] = 'defense'

    logger.info("Merge offense & defense players into a single dataset ...")
    player_count = defense_players.shape[0] + offense_players.shape[0]

    players = pd.concat([offense_players, defense_players], axis=0).dropna().drop_duplicates()

    assert_and_alert(
        player_count - len(players) < 100,
        msg=f"count after merge: {player_count} != {len(players)}"
    )

    return players


def update_player_events(player_events_df: DataFrame, player_participation_df: DataFrame) -> DataFrame:

    """
    Explode the offense and defense arrays in participation_df
    into a separate dataset that has one row per player

    Parameters:
         player_events_df (pd.DataFrame): The player events (e.g. fumble_player_id) from the original pbp data
         player_participation_df (pd.DataFrame): The player level data with one row per player

    Returns:
         merge_df (pd.DataFrame): The player_events_df dataset with merged info from player_participation_df

    """

    known_fubars = {'00-0036906': 'CHI'}

    # both datasets should have one row per player
    merge_df = pd.merge(player_events_df, player_participation_df, on=['play_id', 'player_id'], how='left',
                        suffixes=('', '_y'))

    merge_df['season'] = merge_df['season'].fillna(merge_df['season_y'])
    merge_df['game_id'] = merge_df['game_id'].fillna(merge_df['game_id_y'])
    drop_columns = []
    for col in merge_df.columns:
        if str(col).endswith("_y") or str(col).endswith("_x"):
            drop_columns.append(col)

    merge_df.drop(columns=drop_columns, inplace=True)

    # Sort the DataFrame by player, season, and week
    merge_df = merge_df.sort_values(by=['player_id', 'season'])

    # Group the DataFrame by player and season, and fill any null team_ids
    merge_df['team'] = merge_df.groupby(['player_id', 'season'])['team'].fillna(method='ffill')
    merge_df['team'] = merge_df.groupby(['player_id', 'season'])['team'].fillna(method='bfill')

    for player, team in known_fubars.items():
        merge_df.loc[(merge_df.player_id == player) & (merge_df.team.isnull()), 'team'] = team

    return merge_df


def transform_pbp_participation(participation_df: DataFrame, player_events: DataFrame) -> (DataFrame, DataFrame):
    """
    This is the main function for all cleanup routines for the participation dataset
    The participation dataset contains offense and defense arrays of players (player_ids) who participated in each play
    But those player_ids can't link to other player-event player_ids like tackle_player_id

    We explode the offense and defense arrays into a new dataset that contains one row for each player,
    and use that exploded data to identify the player and team for events like tackle, sack, receiver, etc. that come from player_events

    Parameters:
        participation_df (pd.DataFrame): The play-by-play (pbp) dataset from nflverse
        player_events (pd.DataFrame): The events (e.g. tackle, sack, receiver, etc) scraped from play-by-play dataset

    Returns:
        players_participation_df (pd.DataFrame): The exploded dataframe with one row per player
        player_events (pd.DataFrame): The payer_events data enriched with additional info from players_participation_df

    """

    participation_df = conform_participation_keys(participation_df)
    players_participation_df = create_player_participation(participation_df)
    player_events = update_player_events(player_events, players_participation_df)
    return players_participation_df, player_events
