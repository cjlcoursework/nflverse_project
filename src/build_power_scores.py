import os

import numpy as np
import pandas as pd
from keras import Sequential
from keras.src.callbacks import ModelCheckpoint
from keras.src.layers import Dense
from sklearn.preprocessing import MinMaxScaler

from src import *

logger = configure_logging("pbp_logger")


def summarize_indicators(df, feature_names, threshold):
    indicators = df.loc[(df.Mean > threshold) & df['Feature'].isin(feature_names)].copy()
    indicators['percentage'] = indicators.Mean / indicators.Mean.sum()
    return indicators[['Feature', 'percentage']] \
        .sort_values(by='percentage', ascending=False).pivot_table(columns=['Feature'])


def prepare_indicators(df,threshold):
    feature_column_name, metric_column_name = df.columns[:2]

    indicators = df.loc[(df[metric_column_name] > threshold)].copy()
    indicators['percentage'] = indicators[metric_column_name] / indicators[metric_column_name].sum()

    return indicators.sort_values(by='percentage', ascending=False).pivot_table(columns=[feature_column_name])


def calculate_power_score(data_df, indicators, column_name):
    weighted_columns = []
    for col in indicators.columns:
        # play_df[f"w_{col}"] = offense_indicators.iloc[0][col]
        w = f"v_{col}"
        weighted_columns.append(w)
        data_df[w] = data_df[col] * indicators.iloc[0][col]

    data_df[column_name] = data_df[weighted_columns].sum(axis=1)
    data_df.drop(columns=weighted_columns, inplace=True)


def concat_power_score(df, summary_data, threshold, power_column) -> (
        pd.DataFrame, pd.DataFrame):

    # expect the first two columns to contain a feature label and a metric

    logger.info("get percentage contribution of offensive and defensive features")
    indicator_percentages = prepare_indicators(
        df=summary_data,
        threshold=threshold)

    logger.info("calculate weighted average of offensive and defensive features")
    calculate_power_score(df, indicator_percentages, power_column)

    return df


def build_all_game_weeks(df: pd.DataFrame, team_column) -> pd.DataFrame:
    # there are some missing weeks in the power data.
    # This is probably ok for this application but let's try to backfill some of the data
    # Construct the skeleton as a list of dictionaries
    unique_teams = np.sort(df[team_column].unique())
    unique_seasons = np.sort(df['season'].unique())
    # max_week = df['week'].max()
    # max_weeks = list(range(1, max_week+1))

    skeleton = []
    for team in unique_teams:
        for season in unique_seasons:
            max_week = df.loc[df.season == season, 'week'].max()
            max_weeks = list(range(1, max_week + 1))
            for week in max_weeks:
                skeleton.append(dict(team=team, season=season, week=week))

    # Create the DataFrame from the skeleton
    all_weeks = pd.DataFrame(skeleton)
    return all_weeks


def backfill_missing_weeks(power_df, all_weeks, target_column) -> pd.DataFrame:
    df = power_df[['team', 'season', 'week', target_column]]
    df.sort_values(['team', 'season', 'week'], inplace=True)
    # Create a reference DataFrame with all possible combinations of team, season, and week

    # Merge the reference DataFrame with the original DataFrame to insert missing rows
    df = pd.merge(all_weeks, df, on=['team', 'season', 'week'], how='left')

    # Sort the DataFrame again after merging
    df.sort_values(['team', 'season', 'week'], inplace=True)

    # Use backward fill to propagate values from the next available week
    df[target_column] = df.groupby(['team', 'season'])[target_column].bfill()

    # Use forward fill to propagate values from the previous available week
    df[target_column] = df.groupby(['team', 'season'])[target_column].ffill()

    # Reset the index if needed
    df.reset_index(drop=True, inplace=True)

    return df.drop_duplicates()


def backfill_missing_stats(power_df, all_weeks, target_column) -> pd.DataFrame:
    df = power_df[['team', 'season', 'week', target_column]]
    df.sort_values(['team', 'season', 'week'], inplace=True)
    # Create a reference DataFrame with all possible combinations of team, season, and week

    # Merge the reference DataFrame with the original DataFrame to insert missing rows
    df = pd.merge(all_weeks, df, on=['team', 'season', 'week'], how='left')

    # Sort the DataFrame again after merging
    df.sort_values(['team', 'season', 'week'], inplace=True)

    # Use backward fill to propagate values from the next available week
    df[target_column] = df.groupby(['team', 'season'])[target_column].bfill()

    # Use forward fill to propagate values from the previous available week
    df[target_column] = df.groupby(['team', 'season'])[target_column].ffill()

    # Reset the index if needed
    df.reset_index(drop=True, inplace=True)

    return df.drop_duplicates()


def backfill_missing_metrics(metrics_df, all_weeks, label='metrics') -> pd.DataFrame:
    logger.info(f"back and forward fill {label} metrics by week ...")

    numeric_columns = set(metrics_df.select_dtypes(include='number').columns.tolist())
    target_columns = list(numeric_columns.difference({'team', 'season', 'week'}))

    df = metrics_df[['team', 'season', 'week'] + target_columns].copy()
    df.sort_values(['team', 'season', 'week'], inplace=True)

    # Merge the reference DataFrame with the original DataFrame to insert missing rows
    df = all_weeks.merge(df, on=['team', 'season', 'week'], how='left')

    # Sort the DataFrame again after merging
    df.sort_values(['team', 'season', 'week'], inplace=True)

    # Apply backfilling for each target column
    for column in target_columns:
        df[column] = df.groupby(['team', 'season'])[column].bfill().ffill()

    # Reset the index if needed
    df.reset_index(drop=True, inplace=True)

    return df.drop_duplicates()


def build_power_datasets(weekly_stats_df, summary_data, offense_features, defense_features) -> (
pd.DataFrame, pd.DataFrame):
    logger.info("get percentage contribution of offensive and defensive features")
    offense_indicators = summarize_indicators(
        summary_data,
        offense_features, .023)

    defense_indicators = summarize_indicators(
        summary_data,
        defense_features, .023)

    power_scores_df = weekly_stats_df.copy()

    logger.info("calculate weighted average of offensive and defensive features")
    calculate_power_score(power_scores_df, offense_indicators, 'offense_power')
    calculate_power_score(power_scores_df, defense_indicators, 'defense_power')

    # power_scores_df = power_scores_df[['season', 'week', 'team', 'offense_power', 'defense_power']].drop_duplicates()
    logger.info("build a skeleton data set of all teams, seasons, weeks")
    all_weeks = build_all_game_weeks(power_scores_df)

    # offense power scores
    logger.info("use skeleton to backfill any missing weeks")
    raw_offense_power_df = power_scores_df[['season', 'week', 'team', 'offense_power']].drop_duplicates()
    offense_power_df = backfill_missing_weeks(raw_offense_power_df, all_weeks, target_column='offense_power')

    raw_defense_power_df = power_scores_df[['season', 'week', 'team', 'defense_power']].drop_duplicates()
    defense_power_df = backfill_missing_weeks(raw_defense_power_df, all_weeks, target_column='defense_power')

    return offense_power_df, defense_power_df


def build_combined_power_datasets(weekly_stats_df, summary_data, offense_features, defense_features) -> (
pd.DataFrame, pd.DataFrame):
    logger.info("get percentage contribution of offensive and defensive features")
    offense_indicators = summarize_indicators(
        summary_data,
        offense_features, .023)

    defense_indicators = summarize_indicators(
        summary_data,
        defense_features, .023)

    power_scores_df = weekly_stats_df.copy()

    logger.info("calculate weighted average of offensive and defensive features")
    calculate_power_score(power_scores_df, offense_indicators, 'offense_power')
    calculate_power_score(power_scores_df, defense_indicators, 'defense_power')

    # power_scores_df = power_scores_df[['season', 'week', 'team', 'offense_power', 'defense_power']].drop_duplicates()
    logger.info("build a skeleton data set of all teams, seasons, weeks")
    all_weeks = build_all_game_weeks(power_scores_df, 'team')

    # offense power scores
    logger.info("use skeleton to backfill any missing weeks")
    raw_offense_power_df = power_scores_df[['season', 'week', 'team', 'offense_power']].drop_duplicates()
    offense_power_df = backfill_missing_weeks(raw_offense_power_df, all_weeks, target_column='offense_power')

    raw_defense_power_df = power_scores_df[['season', 'week', 'team', 'defense_power']].drop_duplicates()
    defense_power_df = backfill_missing_weeks(raw_defense_power_df, all_weeks, target_column='defense_power')

    return offense_power_df, defense_power_df


def create_shallow_model(X):
    from keras.src.callbacks import EarlyStopping
    from keras.src.optimizers import Adam
    from keras import regularizers

    # Set parameters
    learning_rate = .01
    activation_function = "relu"
    output_function = "sigmoid"
    loss_function = 'binary_crossentropy'
    regularization_function = regularizers.l1(0.04)
    optimizer=Adam(learning_rate=learning_rate)

    # Create a neural network model
    model = Sequential()
    model.add(Dense(32, input_dim=X.shape[1], activation=activation_function, kernel_regularizer=regularization_function))
    model.add(Dense(32, activation=activation_function, kernel_regularizer=regularization_function))
    model.add(Dense(1, activation=output_function))  # Single output neuron for binary classification

    model.compile(
        optimizer=optimizer,
        loss=loss_function,
        metrics=['accuracy']
    )

    # Define the EarlyStopping callback
    early_stopping_callback = EarlyStopping(monitor='val_loss', patience=3, restore_best_weights=True)

    callbacks = [early_stopping_callback]

    # Train the model
    return model, callbacks


def prepare_power_data(original_stats_df):
    logger.info("encode the target win/loss column")
    original_stats_df['target'] = np.where(original_stats_df['win'] == 'win', 1,
                                           np.where(original_stats_df['win'] == 'loss', 0, 2) )

    logger.info("create a features dataframe for feature selection ...")
    raw_features_df = original_stats_df.drop(columns=['season', 'week','team', 'win', 'spread','team_coach', 'opposing_coach', 'count', 'team_score', 'opposing_team', 'opposing_score' ])

    logger.info("scale all features  ...")
    scaler = MinMaxScaler()

    features = scaler.fit_transform(raw_features_df.to_numpy())
    features_df = pd.DataFrame(features, columns=raw_features_df.columns)

    return features_df


if __name__ == '__main__':
    file_name = "nfl_weekly_offense"
    data_directory = get_config('data_directory')

    input_path = os.path.join(data_directory,  f"{file_name}.parquet")
    stats_df = pd.read_parquet(input_path)
    print("Done")