import numpy as np
import os
from pandas import DataFrame
from sklearn.preprocessing import MinMaxScaler

from src import *

logger = configure_logging("pbp_logger")


def summarize_indicators(df, feature_names, threshold):
    indicators = df.loc[(df.Mean > threshold) & df['Feature'].isin(feature_names)].copy()
    indicators['percentage'] = indicators.Mean / indicators.Mean.sum()
    return indicators[['Feature', 'percentage']] \
        .sort_values(by='percentage', ascending=False).pivot_table(columns=['Feature'])


def prepare_indicators(feature_weights_df: DataFrame, threshold: float) -> DataFrame:

    """
    We receive a two column mapping of features and percentages (weights)
    we drop the summary data rows whose values are less than the threshold
    and recompute the percentage values so that they sum to one.

    Parameters:
        feature_weights_df (pd.DataFrame): a feature importance or correlation dataframe with a feature label and a 'weight''
        threshold (float): this is just a label that we can use to log which metrics_df dataset we are working on

    Returns:
       df (pd.DataFrame): a recomputed version of the feature_weights_df

    """

    # get only the first two columns
    feature_column_name, metric_column_name = feature_weights_df.columns[:2]

    # drop percentages under the threshold
    indicators = feature_weights_df.loc[(feature_weights_df[metric_column_name] > threshold)].copy()

    # recompute the averages
    indicators['percentage'] = indicators[metric_column_name] / indicators[metric_column_name].sum()

    # sort and return
    return indicators.sort_values(by='percentage', ascending=False).pivot_table(columns=[feature_column_name])


def calculate_power_score(data_df: DataFrame, feature_map_df: DataFrame, power_column: str):

    """
    Here we start with a defense or offense stats dataframe (df),
    and the recomputed output of feature selection,
    and we add a power_column that contains the weighted averages from all the feature

    again, the column names in the summary_data dataset correspond to real columns in the df dataset,
    so we are using the feature_map_df weights to calculate on the column names in data_df

    Parameters:
        data_df (pd.DataFrame): an offense or defense stats dataframe that we can use for feature selection
        feature_map_df (pd.DataFrame): our new recomputed feature_map with a percentage for each feature
        power_column (str): this is the column name we are going to put our aggregated score into
                            - it will be either defense_power or offense_power

    Returns:
       None - we add the power score inplace.

     """

    # first, get the weights for all columns in df that are also in features_map_df
    # add a v_<column> column that contains the column value time the feature_map_df weight
    weighted_columns = []
    for col in feature_map_df.columns:
        w = f"v_{col}"
        weighted_columns.append(w)

        # add a temp weight column to the df todo: a little hacky to add weight columns that we'll then delete
        data_df[w] = data_df[col] * feature_map_df.iloc[0][col]

    # next add the new power_column to contains the sum of all v_<column> weights columns
    data_df[power_column] = data_df[weighted_columns].sum(axis=1)

    # drop the temp v_<column> weight columns
    data_df.drop(columns=weighted_columns, inplace=True)


def concat_power_score(df: DataFrame, summary_data: DataFrame, threshold: float, power_column: str) -> (
        pd.DataFrame, pd.DataFrame):
    """
    Here we start with a defense or offense stats dataframe (df),
    and the output of feature selection (summary_data) like the importance output from xgboost

    The column names in the summary_data dataset correspond to real columns in the df dataset

    we drop the summary data rows whose values are less than the threshold and recompute the values so they sum to one.
    we'll then use that feature mapping to find the same feature columns in df and score them according to their value
    weighted by the summary_data percentage that we just calculated.

    Parameters:
        df (pd.DataFrame): an offense or defense stats dataframe that we can use for feature selection
        summary_data (pd.DataFrame): a feature importance or correlation dataframe with a feature label and a 'weight''
        threshold (float): this is just a label that we can use to log which metrics_df dataset we are working on
        power_column (str): this is just a label that we can use to log which metrics_df dataset we are working on

    Returns:
       df (pd.DataFrame): we return the new backfilled replacement for mertics_df

    Example:
        Lets say that we have a summary data column that looks like this:
            sacks      50%
            qb_hits    40%
            tackles    10%
         and our threshold is 11%, so we drop the 'tackles' row and re-compute the percentage
         to look something like this:
            sacks      55%
            qb_hits    45%

        We then look up a row in the df dataset - it has the following data
             sacks      25
             qb_hits    39

        we'll multiply the actual values from each df row with their weights from summary_data:

            new_df[power_column] -> sacks * 55% + qb_hits * 45%
            =  25 * 55% + 39 * 45%
            =  13.75 + 17.55
            =  31.3
     """

    logger.info("get percentage contribution of offensive and defensive features")

    # drop weights under the threshold and recompute the probabilities
    feature_map_df = prepare_indicators(
        feature_weights_df=summary_data,
        threshold=threshold)

    logger.info("calculate weighted average of offensive and defensive features")
    calculate_power_score(df, feature_map_df, power_column)

    return df


def backfill_missing_metrics(metrics_df: DataFrame, all_weeks: DataFrame, label: str = 'metrics') -> DataFrame:
    """
     This function is applied to any stats dataframes, such as pbp_events,  defense_stats, ngs_air_power
     We will join all of these together on season, week and team
     If there are missing weeks, the stats from the next closest week are valid, and we want to backfill from those rows
     We'll create rows with null values in metrics_df by merging with the all_weeks dataset
     We can then use backfill and foward fill to fill those rows with missing vaues


    Parameters:
        metrics_df (pd.DataFrame): an offense or defense stats dataframe that we can use for feature selection
        all_weeks (pd.DataFrame): a skeleton dataset containing all possible season, week, team
        label (str): this is just a label that we can use to log which metrics_df dataset we are working on

    Returns:
       df (pd.DataFrame): we return the new backfilled replacement for mertics_df

     """

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


def prepare_power_data(original_stats_df: DataFrame) -> DataFrame:
    """
    Helper function to create a dataset that is ready for feature selection
    e.g. for xgboost

        - add a categorical win or loss 'y' category to a stats dataframe
        - drop columns that are not important for this feaure selection
        - scale remaining columns

    Parameters:
        original_stats_df (pd.DataFrame): an offense or defense stats dataframe that we can use for feature selection

    Returns:
         features_df (pd.DataFrame): the features  we'll use for feature selection

    """
    logger.info("encode the target win/loss column")
    original_stats_df['target'] = np.where(original_stats_df['win'] == 'win', 1,
                                           np.where(original_stats_df['win'] == 'loss', 0, 2))

    logger.info("create a features dataframe for feature selection ...")
    raw_features_df = original_stats_df.drop(
        columns=['season', 'week', 'team', 'win', 'spread', 'team_coach', 'opposing_coach', 'count', 'team_score',
                 'opposing_team', 'opposing_score'])

    logger.info("scale all features  ...")
    scaler = MinMaxScaler()

    features = scaler.fit_transform(raw_features_df.to_numpy())
    features_df = pd.DataFrame(features, columns=raw_features_df.columns)

    return features_df
