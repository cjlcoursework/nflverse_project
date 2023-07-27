import joblib
import numpy as np
from sklearn.preprocessing import MinMaxScaler, StandardScaler, RobustScaler
from typing import Union, Any, Optional

import os
import pandas as pd
from pandas import DataFrame, Series

import tensorflow as tf
from tensorflow.keras import Sequential
from tensorflow.keras import regularizers
from tensorflow.keras.callbacks import EarlyStopping
from tensorflow.keras.optimizers.legacy import Adam
from tensorflow.keras.models import load_model

from src import configs
from src.configs import get_config
from src.nfl_04_merge_game_features import load_file
from src.util.database_loader import DatabaseLoader
from src.util.utils import assert_and_alert
import warnings

warnings.filterwarnings('ignore')



identity_columns = ['team', 'season', 'week']
power_columns = ['offense_power', 'defense_power']

defense_columns = ['team', 'season', 'week', 'interception',
                                             'ps_interceptions',
                                             'qb_hit',
                                             'sack_yards',
                                             'sack',
                                             'tackle', 'defense_power']

offense_columns = ['team', 'season', 'week', 'carries',
                                             'pass_touchdowns',
                                             'passer_rating',
                                             'receiving_air_yards',
                                             'receiving_tds',
                                             'receiving_yards',
                                             'rushing_tds',
                                             'rushing_yards',
                                             'special_teams_tds', 'offense_power']


def load_pbp_actions(season, week=1):
    pbp_actions_df = load_file(get_config('data_directory'), get_config('action_week_prep'))
    pbp_actions_df = pbp_actions_df.loc[(pbp_actions_df['season'] == season) & (pbp_actions_df['week'] == week)]\
        .sort_values(by=['week'])[['season', 'week', 'game_id', 'home_team', 'away_team', 'home_final_score', 'away_final_score']]\
        .drop_duplicates()

    for index, row in pbp_actions_df.iterrows():
        game = row[['season', 'week', 'game_id', 'home_team', 'away_team', 'home_final_score', 'away_final_score']].to_dict()
        game['win_lose'] = 1 if game['home_final_score'] >= game['away_final_score'] else 0
        yield game


class NFLWinLossDeployment:

    def __init__(self, season=2022, week=1):
        self.season = season
        self.week = week
        self.db = DatabaseLoader(get_config('connection_string'))
        self.model = self.load_win_loss_model()
        self.scaler = self.get_scaler()
        self.offense, self.defense = self.get_season(season, week)

    def load_win_loss_model(self):
        model_directory = get_config('model_directory')
        model_name = get_config('experiment_2_model')
        full_path = os.path.join(model_directory, f'{model_name}.h5')
        loaded_model = tf.keras.models.load_model(full_path)
        loaded_model.summary()
        return loaded_model

    def get_scaler(self):
        model_directory = get_config('model_directory')
        model_name = get_config('experiment_2_model')
        full_path = os.path.join(model_directory, f'{model_name}_scaler.pkl')
        # Save the scaler to a file
        scaler = joblib.load(full_path)
        return scaler


    def predict(self, X_test: DataFrame):
        y_pred_probs = self.model.predict(X_test, verbose=0)
        y_pred_probs = y_pred_probs[:, 0][0]

        threshold = 0.5
        y_pred_binary = (y_pred_probs >= threshold).astype(int)

        return y_pred_binary

    def validate_schema(self, df: DataFrame):
        expected_columns = set(configs.ml_win_lose_features)
        actual_columns = set(df.columns)
        missing_columns = expected_columns.difference(actual_columns)
        excess_columns = actual_columns.difference(expected_columns)
        ok = len(missing_columns) == 0
        assert_and_alert(ok, f"Missing or extra columns {missing_columns} in dataframe")
        return ok, excess_columns

    def suffix_columns(self, df: DataFrame, suffix: str, renames: dict = None):
        columns = df.columns
        renames.update(
            {column: column + suffix for column in columns if column not in identity_columns and column not in renames.keys()} )
        return df.rename(columns=renames)

    def get_team_stats(self, team: str, suffix: str = 'home'):
        kernel = suffix[0]
        offense_stats = self.suffix_columns(
            self.offense.loc[(self.offense['team'] == team)],
            suffix=f"_{kernel}op",
            renames={
                'offense_power': f'{suffix}_team_offense_power',
                'team': f'{suffix}_team'
            }
        )

        defensive_stats = self.suffix_columns(
            self.defense.loc[(self.defense['team'] == team)],
            f"_{kernel}dp",
            renames={
                'defense_power': f'{suffix}_team_defense_power',
                'team': f'{suffix}_team'
            }
        )
        return pd.merge(offense_stats, defensive_stats, on=['season', 'week', f'{suffix}_team'])

    def scale_numeric_columns(self, df):
        numeric_df = df.select_dtypes(include=np.number)
        scaled_columns = numeric_df.columns
        features = self.scaler.fit_transform(numeric_df.to_numpy())
        scaled_df = pd.DataFrame(features, columns=scaled_columns)
        df[scaled_columns] = scaled_df[scaled_columns]
        return df.copy()


    def get_season(self, season, week):
        # get the base features
        offense = self.db.query_to_df(
            f"""select {",".join(offense_columns)} from controls.offense_week_features where season = {season} and week <= {week}""")
        defense = self.db.query_to_df(
            f"""select {",".join(defense_columns)} from controls.defense_week_features where season = {season} and week <= {week}""")

        # scale the numeric columns
        offense = self.scale_numeric_columns(offense)
        defense = self.scale_numeric_columns(defense)
        return offense, defense


    def create_match(self, home_team: str, away_team: str) -> Optional[DataFrame]:
        home_stats = self.get_team_stats(team=home_team, suffix='home')
        away_stats = self.get_team_stats(team=away_team, suffix='away')
        match_df = pd.merge(home_stats, away_stats, on=['season', 'week'])
        ok, excess_columns = self.validate_schema(match_df)
        if ok:
            return match_df
        return None


def main():
    model = NFLWinLossDeployment(season=2022, week=1)
    match_df = model.create_match('ARI', 'KC')
    X_test = match_df[configs.ml_win_lose_features]
    print("done")


def test_pbp_actions(season: int):
    model = NFLWinLossDeployment(season=2022, week=1)

    # Call the generator function with the 'season' parameter
    generator = load_pbp_actions(season=2022, week=1)

    # Iterate through the generator and print each row
    stats = []
    total = 0
    corrects = 0
    for row in generator:
        total += 1
        match_df = model.create_match(row['home_team'], row['away_team'])

        y_actual = row['win_lose']
        y_pred = model.predict(match_df[configs.ml_win_lose_features])
        stats.append(dict(home=row['home_team'], away=row['away_team'], y_actual=y_actual, y_pred=y_pred))
        if y_actual == y_pred:
            corrects += 1
        print(f"match {row['home_team']} vs {row['away_team']} results: y_actual: {y_actual} y_pred: {y_pred}")
        if total > 20:
            break

    print(f"total: {total} corrects: {corrects} accuracy: {corrects/total}")


if __name__ == '__main__':
    test_pbp_actions(2022)
    main()
