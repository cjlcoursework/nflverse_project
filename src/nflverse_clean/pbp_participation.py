import pandas as pd
from pandas import DataFrame

from utils import assert_and_alert, impute_columns, get_duplicates_by_key, conform_binary_column, explode_column

import logging
import warnings

warnings.filterwarnings('ignore')
# Configure logging
from logging_config import confgure_logging

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


# def reconcile_join_keys_dummy(pbp_df, participation_df):
#     # pbp to participation is a 1 to 1 join
#     merged_df = pd.merge(pbp_df, participation_df, left_on='play_id', right_on='play_id', how='outer', indicator=True,
#                          suffixes=('_new', '_prev'))
#     merged_df = merged_df.copy()
#     assert_and_alert(len(get_duplicates_by_key(merged_df, 'play_id')) == 0,
#                      msg="Unexpected duplicate keys found creating play_id from game_id and play_id")
#
#     missing_participations = merged_df.loc[(merged_df['_merge'] == 'left_only')] \
#         [['play_id', 'play_counter_new', 'game_id_new', 'old_game_id_new']] \
#         .rename(columns={
#         'play_counter_new': 'play_counter',
#         'game_id_new': 'game_id',
#         'old_game_id_new': 'old_game_id'
#     }).drop_duplicates()
#
#     found = 0
#     nf = 0
#     for index, row in missing_participations.iterrows():
#         good_game_id = row['game_id']
#         good_play_id = row['play_id']
#         good_old_game_id = row['old_game_id']
#         fix = participation_df.loc[(participation_df['old_game_id'] == good_old_game_id), ['play_id', 'game_id']]
#         participation_df.loc[participation_df['old_game_id'] == good_old_game_id, ['play_id', 'game_id']] = [
#             good_play_id, good_game_id]
#
#         if len(fix) == 0:
#             nf += 1
#         else:
#             found += 1
#
#     print(f"Missing participants:   fixed {found},  not fixed: {nf}")
#
#     # create play_id : (pbp_df["game_id"]+"_"+pbp_df["play_id"])
#     participation_df['play_id'] = participation_df["game_id"].astype(str) + "_" + participation_df[
#         "play_counter"].astype(str)
#     assert_and_alert(len(get_duplicates_by_key(participation_df, 'play_id')) == 0,
#                      msg="Unexpected duplicate keys found creating play_id from game_id and play_id")
#
#     merged_df = pd.merge(pbp_df, participation_df, left_on='play_id', right_on='play_id', how='outer', indicator=True,
#                          suffixes=('_new', '_prev'))
#     merged_df = merged_df.copy()
#     print(merged_df['_merge'].value_counts())


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


def explode_player_lists(participation_df):
    logger.info("Exploding offensive players to their own dataset...")
    offense_players = explode_column(participation_df, 'play_id', 'offense_players')

    logger.info("Exploding defense_players to their own dataset...")
    defense_players = explode_column(participation_df, 'play_id', 'defense_players')

    return offense_players, defense_players


def load_pbp_participation_file(file_path: str) -> DataFrame:
    df = pd.read_csv(file_path, header=0)
    return df


def test_sanity():
    df = load_pbp_participation_file("../../output/playbyplay2021_participation.csv")
    print(df.shape)
    print("bye")
