from typing import Set

import os
import sys

from src.nfl_power_scores import prepare_power_data, concat_power_score
from src import *
from src.utils import assert_and_alert
import xgboost as xgb

from src.utils_eda import plot_correlations, plot_heatmap

logger = configs.configure_logging("pbp_logger")
logger.setLevel(logging.INFO)


class SelectNFLFeatures:

    def __init__(self, side: str):
        self.side = side.lower()
        self.directory = get_config('data_directory')
        self.input_stats_df = None

        logger.info("SelectNFLFeatures")
        if side == 'offense':
            self.power_column = "offense_power"
            self.input_file_name = get_config('offense_week_prep')
            self.output_file_name = get_config('offense_week_features')
        else:
            self.power_column = "defense_power"
            self.input_file_name = get_config('defense_week_prep')
            self.output_file_name = get_config('defense_week_features')

        self.read_input()

        logger.info("prepare a features dataset")
        self.features_df = prepare_power_data(self.input_stats_df)
        self.target = self.features_df.pop('target')
        self.top_features, self.set_features = (None, None)

    def read_input(self):
        logger.info(f"load {self.input_file_name}")
        input_path = os.path.join(self.directory, f"{self.input_file_name}.parquet")
        self.input_stats_df = pd.read_parquet(input_path)

    def write_output(self):
        logger.info(f"Writing to {self.output_file_name}")
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)

        output_path = os.path.join(self.directory, f"{self.output_file_name}_ml.parquet")
        self.input_stats_df.to_parquet(output_path, engine='fastparquet', compression='snappy')

    def get_best_features(self):
        self.top_features, self.set_features = self.calc_feature_importance(self.features_df, self.target, top_n=30)

    def calculate_and_add_power_score(self):
        concat_power_score(
            df=self.input_stats_df,
            summary_data=self.top_features,
            threshold=.01,
            power_column=self.power_column)

    def show_correlations(self):
        top_correlations, set_correlations = self.correlate_to_target(self.input_stats_df, 'target', 30)
        plot_correlations(top_correlations['corr'], top_correlations['y'], 'Feature Correlations')

    def show_heatmap(self):
        plot_heatmap(self.input_stats_df, drop_columns=['season', 'week', 'count'])

    def plot_best_features(self):
        plot_correlations(
            self.top_features['corr'],
            self.top_features['y'], "Feature Importance")

    def correlate_to_target(self, df: pd.DataFrame, target_column: str, top_n: int) -> (pd.DataFrame, Set):
        # Calculate correlation matrix
        correlation_matrix = df.corr()

        # Filter and sort correlation coefficients by absolute value
        corr_df = pd.DataFrame(correlation_matrix.abs().unstack().sort_values(ascending=False))
        corr_df.reset_index(inplace=True)
        corr_df.columns = ['x', 'y', 'corr']

        not_self_correlated = (corr_df.y != target_column)
        win_correlated = (corr_df.x == target_column)

        df = corr_df.loc[win_correlated & not_self_correlated] \
            .sort_values(by='corr', ascending=False) \
            .drop(columns=['x']) \
            .head(top_n) \
            .copy()

        s = set(df['y'].values)

        return df, s

    def calc_feature_importance(self, X: pd.DataFrame, y: pd.Series, top_n=30) -> (pd.DataFrame, Set):
        # Create an XGBoost model
        model = xgb.XGBRegressor()

        X = self.features_df

        # Fit the model
        model.fit(X, y)

        # Get feature importance scores
        importance_scores = model.feature_importances_

        # Sort feature importance scores
        sorted_indices = importance_scores.argsort()[::-1]
        sorted_scores = importance_scores[sorted_indices]
        feature_names = X.columns[sorted_indices]

        # Get the top 'n' feature importance scores and names
        top_scores = sorted_scores[:top_n]
        top_features = feature_names[:top_n]
        df = pd.DataFrame({'y': top_features, 'corr': top_scores})

        set_xg_cols = set(top_features)
        return df, set(top_features)


def perform_feature_selection():
    offense = SelectNFLFeatures("offense")
    offense.get_best_features()
    offense.calculate_and_add_power_score()
    offense.write_output()

    defense = SelectNFLFeatures("defense")
    defense.get_best_features()
    defense.calculate_and_add_power_score()
    defense.write_output()


if __name__ == '__main__':
    perform_feature_selection()