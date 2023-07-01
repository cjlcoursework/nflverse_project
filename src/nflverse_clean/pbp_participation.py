import pandas as pd
from pandas import DataFrame

from utils import assert_and_alert, impute_columns, get_duplicates_by_key, conform_binary_column, explode_column, \
    explode_column_with_cols

import warnings
from logging_config import confgure_logging

warnings.filterwarnings('ignore')
logger = confgure_logging("pbp_logger")


def conform_pbp_participation_play_id(participation_df):
    logger.info(
        "pbp_participation:  move play_id to a play_count column and create a unique play_id that can be used in joins...")
    participation_df.rename(columns={'nflverse_game_id': 'game_id'}, inplace=True)
    participation_df['play_counter'] = participation_df['play_id']
    participation_df['play_id'] = participation_df["game_id"].astype(str) + "_" + participation_df[
        "play_counter"].astype(str)  ## temporary
    assert_and_alert(len(get_duplicates_by_key(participation_df, 'play_id')) == 0,
                     msg="Unexpected duplicate keys found creating play_id from game_id and play_id")


def reconcile_join_keys(pbp_df, participation_df):
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

    missing_participations = missing_participations[['play_id', 'play_counter_new', 'game_id_new', 'old_game_id_new']] \
        .rename(columns={
            'play_counter_new': 'play_counter',
            'game_id_new': 'game_id',
            'old_game_id_new': 'old_game_id'
    }).drop_duplicates()

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


def update_player_lists(pbp_df: DataFrame, players_df: DataFrame):
    logger.info("Add team names to the players_df table ...")

    # create an index column to lookup the team based on the play
    offense_teams = pbp_df[['play_id', 'posteam']].drop_duplicates()
    defense_teams = pbp_df[['play_id', 'defteam']].drop_duplicates()

    # confrm the team names to 'team'
    offense_teams.rename(columns={'posteam': 'team'}, inplace=True)
    defense_teams.rename(columns={'defteam': 'team'}, inplace=True)

    # separate the players_df into offense and defense plays
    offense_players = players_df[players_df['lineup'] == 'offense']
    defense_players = players_df[players_df['lineup'] == 'defense']

    # merge the offense players with their team and same for the defense players
    offense_merged_df = pd.merge(offense_players, offense_teams, on='play_id')
    defense_merged_df = pd.merge(defense_players, defense_teams, on='play_id')

    # merge off
    merged_df = pd.concat([offense_merged_df, defense_merged_df])

    return merged_df


def update_player_events(player_events: DataFrame, pbp_participation: DataFrame):
    """
    with player_teams as (select distinct player_id, game_id, team  from  player_participation)
        select count(*) from player_events E
         full join player_teams T on T.player_id=E.player_id and T.game_id = E.game_id
    """
    player_teams = pbp_participation[['game_id', 'player_id', 'team']].drop_duplicates()
    merged_df = pd.merge(player_events, player_teams, on=['player_id', 'game_id'], how="outer", indicator=True)


def create_player_participation(participation_df: DataFrame):
    logger.info("Exploding offensive players to their own dataset...")
    offense_players = explode_column_with_cols(participation_df, ['game_id', 'play_id'], 'offense_players')
    offense_players.rename(columns={'offense_players': 'player_id'}, inplace=True)
    offense_players['lineup'] = 'offense'

    logger.info("Exploding defense_players to their own dataset...")
    defense_players = explode_column_with_cols(participation_df, ['game_id', 'play_id'], 'defense_players')
    defense_players.rename(columns={'defense_players': 'player_id'}, inplace=True)
    defense_players['lineup'] = 'defense'

    player_count = defense_players.shape[0] + offense_players.shape[0]

    players = pd.concat([offense_players, defense_players], axis=0).dropna().drop_duplicates()

    assert_and_alert(
        player_count - len(players) < 100,
        msg=f"combining offense and defense players - counts are incorrect {player_count} != {len(players)}"
    )

    return players


def load_pbp_participation_file(file_path: str) -> DataFrame:
    df = pd.read_csv(file_path, header=0)
    return df


def test_sanity():
    df = load_pbp_participation_file("../../output/playbyplay2021_participation.csv")
    print(df.shape)
    print("bye")
