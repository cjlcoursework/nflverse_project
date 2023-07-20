import pandas as pd
from pandas import DataFrame


import warnings
from src import configure_logging
from src.util.utils import assert_and_alert, get_duplicates_by_key, explode_column_with_cols

warnings.filterwarnings('ignore')
logger = configure_logging("pbp_logger")


def conform_participation_keys(participation_df):
    logger.info(
        "pbp_participation:  create a joinable play_id column ...")
    participation_df.rename(columns={'nflverse_game_id': 'game_id'}, inplace=True)
    participation_df['play_counter'] = participation_df['play_id']
    participation_df['play_id'] = participation_df["game_id"].astype(str) + "_" + participation_df[
        "play_counter"].astype(str)  ## temporary
    assert_and_alert(len(get_duplicates_by_key(participation_df, 'play_id')) == 0,
                     msg="Unexpected duplicate keys found creating play_id from game_id and play_id")
    return participation_df


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


# def update_player_lists(events_df: DataFrame, participation_df: DataFrame):
#     logger.info("Add team names to the players_df table ...")
#
#     # looking for any lineup columns in events
#     events_df['lineup'] = events_df['lineup'].fillna(
#         events_df['player_id'].map(participation_df.set_index('player_id')['lineup']))
#
#     lookup_dict = participation_df.set_index(['season', 'week', 'player'])['lineup'].to_dict()
#     events_df['team'] = events_df.apply(lambda row: row['lineup'] if pd.notnull(row['lineup']) else lookup_dict.get((row['season'], row['week'], row['player'])), axis=1)
#
#
#     # create an index column to lookup the team based on the play
#     offense_teams = events_df[['season', 'week', 'play_id', 'posteam']] \
#         .drop_duplicates() \
#         .rename(columns={'posteam': 'team'})
#
#     # confrm the team names to 'team'
#     defense_teams = events_df[['season', 'week', 'play_id', 'defteam']] \
#         .drop_duplicates() \
#         .rename(columns={'defteam': 'team'})
#
#     # separate the players_df into offense and defense plays
#     offense_players = participation_df[participation_df['lineup'] == 'offense']
#     offense_merged_df = pd.merge(offense_players, offense_teams, on='play_id').drop_duplicates()
#     assert_and_alert(offense_players.shape[0] == offense_merged_df.shape[0],
#                      msg=f'merge on offensive players fanned out {offense_players.shape[0]} !== {offense_merged_df.shape[0]}')
#
#     # merge the offense players with their team and same for the defense players
#     defense_players = participation_df[participation_df['lineup'] == 'defense']
#     defense_merged_df = pd.merge(defense_players, defense_teams, on='play_id').drop_duplicates()
#     assert_and_alert(defense_players.shape[0] == defense_merged_df.shape[0],
#                      msg=f'merge on defensive players fanned out {defense_players.shape[0]} !== {defense_merged_df.shape[0]}')
#
#     # merge off
#     player_participation_df = pd.concat([offense_merged_df, defense_merged_df])
#
#     # update player events - get the right team for each player in the play\
#     # player_teams = player_participation_df[['play_id', 'player_id', 'team']].drop_duplicates()
#     # merged_df = pd.merge(events_df, player_teams, on=['player_id', 'play_id'], how="outer", indicator=True)
#
#     return player_participation_df


# def update_player_events(player_events: DataFrame, pbp_participation: DataFrame):
#     """
#     with player_teams as (select distinct player_id, game_id, team  from  player_participation)
#         select count(*) from player_events E
#          full join player_teams T on T.player_id=E.player_id and T.game_id = E.game_id
#     """
#     player_teams = pbp_participation[['game_id', 'player_id', 'team']].drop_duplicates()
#     merged_df = pd.merge(player_events, player_teams, on=['player_id', 'game_id'], how="outer", indicator=True)


def create_player_participation(participation_df: DataFrame):
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
                                               ['season',  'game_id', 'game_order', 'play_id', 'possession_team'],
                                               'offense_players')
    offense_players.rename(columns={'offense_players': 'player_id', 'possession_team': 'team'}, inplace=True)
    offense_players['lineup'] = 'offense'

    logger.info("Exploding defense_players to their own dataset...")
    defense_players = explode_column_with_cols(participation_df,
                                               ['season',  'game_id', 'game_order', 'play_id', 'defense_team'],
                                               'defense_players')
    defense_players.rename(columns={'defense_players': 'player_id', 'defense_team': 'team'}, inplace=True)
    defense_players['lineup'] = 'defense'

    player_count = defense_players.shape[0] + offense_players.shape[0]

    players = pd.concat([offense_players, defense_players], axis=0).dropna().drop_duplicates()

    assert_and_alert(
        player_count - len(players) < 100,
        msg=f"count after merge: {player_count} != {len(players)}"
    )

    return players


def update_player_events(player_events_df: DataFrame, player_participation_df: DataFrame) -> DataFrame:
    known_fubars = {'00-0036906': 'CHI'}

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

    # Group the DataFrame by player and season, and fill any null team_id
    merge_df['team'] = merge_df.groupby(['player_id', 'season'])['team'].fillna(method='ffill')
    merge_df['team'] = merge_df.groupby(['player_id', 'season'])['team'].fillna(method='bfill')

    for player, team in known_fubars.items():
        merge_df.loc[(merge_df.player_id == player) & (merge_df.team.isnull()), 'team'] = team

    return merge_df


def transform_pbp_participation(participation_df, player_events):
    participation_df = conform_participation_keys(participation_df)
    players_participation_df = create_player_participation(participation_df)
    player_events = update_player_events(player_events, players_participation_df)
    return players_participation_df, player_events
