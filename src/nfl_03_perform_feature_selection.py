import logging
import os
import pandas as pd

from src.configs import get_config, configure_logging
from src.nfl.nfl_power_scores import prepare_power_data, concat_power_score
from src.util.utils_eda import calc_feature_importance, correlate_to_target, plot_correlations, plot_heatmap

logger = configure_logging("pbp_logger")
logger.setLevel(logging.INFO)


class SelectNFLFeatures:
    """
    Select the best features for the NFL dataset.

    Parameters:
        side (str): Either "offense" or "defense", to indicate which side of the ball the features should be for.
    """
    def __init__(self, side: str):
        """
        Initialize the class.
        Load the data and prepare the features dataset.

        Parameters:
            side (str): Either "offense" or "defense", to indicate which side of the ball the features should be for.
        """
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

        self.input_stats_df = self.read_input()

        logger.info("prepare a features dataset")
        self.features_df = prepare_power_data(self.input_stats_df)
        self.target = self.features_df.pop('target')
        self.top_features, self.set_features = (None, None)

    def read_input(self):
        """Read the self.input_file_name data.
        Returns:
            df (pd.DataFrame): The input data.
        """
        logger.info(f"load {self.input_file_name}")
        input_path = os.path.join(self.directory, f"{self.input_file_name}.parquet")
        return pd.read_parquet(input_path)

    def write_output(self):
        """Write the output data."""
        logger.info(f"Writing to {self.output_file_name}")
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)

        output_path = os.path.join(self.directory, f"{self.output_file_name}.parquet")
        self.input_stats_df.to_parquet(output_path, engine='fastparquet', compression='snappy')

    def get_best_features(self):
        self.top_features, self.set_features = calc_feature_importance(self.features_df, self.target, top_n=30)

    def calculate_and_add_power_score(self):
        """Calculate the power score and add it to the dataset.
        """
        concat_power_score(
            df=self.input_stats_df,
            summary_data=self.top_features,
            threshold=.01,
            power_column=self.power_column)

    def show_correlations(self):
        """Show the correlations between the features and the target."""
        top_correlations, set_correlations = correlate_to_target(self.input_stats_df, 'target', 30)
        plot_correlations(top_correlations['corr'], top_correlations['y'], 'Feature Correlations')

    def show_heatmap(self):
        """Show the heatmap of the correlations between the features."""
        plot_heatmap(self.input_stats_df, drop_columns=['season', 'week', 'count'])

    def plot_best_features(self):
        """Plot the best features."""
        plot_correlations(
            self.top_features['corr'],
            self.top_features['y'], "Feature Importance")


def perform_team_week_feature_selection():
    """Perform feature selection for the team-week dataset.
    This workflow will select the best features for both the offense and defense datasets.
    """
    offense = SelectNFLFeatures("offense")
    offense.get_best_features()
    offense.calculate_and_add_power_score()
    offense.write_output()

    defense = SelectNFLFeatures("defense")
    defense.get_best_features()
    defense.calculate_and_add_power_score()
    defense.write_output()


if __name__ == '__main__':
    """
    test the feature selection process
    """
    perform_team_week_feature_selection()