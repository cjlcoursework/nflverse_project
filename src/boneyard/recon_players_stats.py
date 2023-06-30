import pandas as pd
from sqlalchemy import create_engine

from NFLVersReader.src.database_loader import DatabaseLoader
from NFLVersReader.src.nflverse_clean.utils import assert_not_null
from configs import get_config

def get_db_loader():
    return DatabaseLoader(get_config("connection_string"))


def do_work():
    loader = get_db_loader()
    df_stats = loader.read_table("player_stats")
    df_players = loader.read_table("players")

    assert_not_null(df_players, 'gsis_id')
    assert_not_null(df_players, 'status')

    df = pd.merge([df_stats, df_players], left_on='player_id', right_on='gsis_id', how='outer', indicator=True)



